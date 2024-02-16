import csv
import logging
from typing import List, Tuple, Union, Dict
import re
import time
import json

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException
from keboola.component.base import sync_action
from keboola.component.sync_actions import ValidationResult, MessageType
from keboola.component.dao import FileDefinition
from jinja2 import Template

from configuration import Configuration
from client import SMTPClient


KEY_CONNECTION_CONFIG = 'connection_config'
KEY_SENDER_EMAIL_ADDRESS = 'sender_email_address'
KEY_SENDER_PASSWORD = 'pswd_sender_password'
KEY_SERVER_HOST = 'server_host'
KEY_SERVER_PORT = 'server_port'
KEY_PROXY_SERVER_HOST = 'proxy_server_host'
KEY_PROXY_SERVER_PORT = 'proxy_server_port'
KEY_CONNECTION_PROTOCOL = 'connection_protocol'
KEY_PROXY_SERVER_USERNAME = 'proxy_server_username'
KEY_PROXY_SERVER_PASSWORD = 'pswd_proxy_server_password'

KEY_RECIPIENT_EMAIL_ADDRESS_COLUMN = 'recipient_email_address_column'

KEY_SUBJECT_CONFIG = 'subject_config'
KEY_SUBJECT_SOURCE = 'subject_source'
KEY_SUBJECT_COLUMN = 'subject_column'
KEY_SUBJECT_TEMPLATE = 'subject_template'

KEY_MESSAGE_BODY_CONFIG = 'message_body_config'
KEY_MESSAGE_BODY_SOURCE = 'message_body_source'
KEY_USE_HTML_TEMPLATE = 'use_html_template'
KEY_MESSAGE_BODY_COLUMN = 'message_body_column'
KEY_PLAINTEXT_TEMPLATE_COLUMN = 'plaintext_template_column'
KEY_HTML_TEMPLATE_COLUMN = 'html_template_column'
KEY_PLAINTEXT_TEMPLATE_FILENAME = 'plaintext_template_filename'
KEY_HTML_TEMPLATE_FILENAME = 'html_template_filename'
KEY_PLAINTEXT_TEMPLATE_DEFINITION = 'plaintext_template_definition'
KEY_HTML_TEMPLATE_DEFINITION = 'html_template_definition'

KEY_ATTACHMENTS_CONFIG = 'attachments_config'
KEY_ATTACHMENTS_SOURCE = 'attachments_source'
KEY_ATTACHMENTS_COLUMN = 'attachments_column'

KEY_DRY_RUN = 'dry_run'

SLEEP_INTERVAL = 0.1

RESULT_TABLE_COLUMNS = ('status', 'recipient_email_address', 'sender_email_address', 'subject',
                        'plaintext_message_body', 'html_message_body', 'attachment_filenames', 'error_message')

VALID_CONNECTION_CONFIG_MESSAGE = 'OK - Connection configuration is valid'
VALID_SUBJECT_MESSAGE = 'OK - All subject placeholders are present in the input table'
VALID_TEMPLATE_MESSAGE = 'OK - All template placeholders are present in the input table'
VALID_ATTACHMENTS_MESSAGE = 'OK - All attachments are present'

# list of mandatory parameters => if some is missing,
# TODO: fix mandatory params check
# REQUIRED_PARAMETERS = {KEY_SENDER_EMAIL_ADDRESS, KEY_SENDER_PASSWORD, KEY_SERVER_HOST, KEY_SERVER_PORT}
REQUIRED_PARAMETERS = set()

# port 465 for SMTP_SSL
# port 587 for SMTP with TLS


class Component(ComponentBase):
    """Component for sending emails"""
    def __init__(self):
        super().__init__()
        self.cfg = Configuration
        self._client: SMTPClient = None
        self._results_writer = None
        self.plaintext_template_path = None
        self.html_template_path = None

    def run(self):
        self._init_configuration()
        self.init_client()
        in_tables = self.get_input_tables_definitions()
        in_files = self.get_input_files_definitions()
        in_table_path = in_tables[0].full_path
        self.plaintext_template_path, self.html_template_path = self._extract_template_files_full_paths(in_files)

        attachments_paths_by_filename = {
            file.name.replace(f'{file.id}_', ''): file.full_path
            for file in in_files
            if file.full_path not in [self.plaintext_template_path, self.html_template_path]}

        # TODO: return write_always=True once we have queue_v2
        results_table = self.create_out_table_definition('results.csv')
        with open(results_table.full_path, 'w', newline='') as output_file:
            self._results_writer = csv.DictWriter(output_file, fieldnames=RESULT_TABLE_COLUMNS)
            self._results_writer.writeheader()
            self.send_emails(in_table_path, attachments_paths_by_filename=attachments_paths_by_filename)
        self.write_manifest(results_table)

    def _init_configuration(self) -> None:
        self.validate_configuration_parameters(Configuration.get_dataclass_required_parameters())
        self.cfg: Configuration = Configuration.load_from_dict(self.configuration.parameters)

    def init_client(self):
        connection_config = self.cfg[KEY_CONNECTION_CONFIG]
        use_ssl = connection_config[KEY_CONNECTION_PROTOCOL] == 'SSL'
        self._client = SMTPClient(
            sender_email_address=connection_config[KEY_SENDER_EMAIL_ADDRESS],
            password=connection_config[KEY_SENDER_PASSWORD],
            server_host=connection_config[KEY_SERVER_HOST],
            server_port=connection_config[KEY_SERVER_PORT],
            proxy_server_host=connection_config[KEY_PROXY_SERVER_HOST],
            proxy_server_port=connection_config[KEY_PROXY_SERVER_PORT],
            proxy_server_username=connection_config[KEY_PROXY_SERVER_USERNAME],
            proxy_server_password=connection_config[KEY_PROXY_SERVER_PASSWORD],
            use_ssl=use_ssl
        )
        self._client.init_smtp_server()

    def send_emails(self, in_table_path: str, attachments_paths_by_filename: Dict[str, str]) -> None:
        dry_run = self.cfg[KEY_DRY_RUN]
        subject_config = self.cfg[KEY_SUBJECT_CONFIG]
        message_body_config = self.cfg[KEY_MESSAGE_BODY_CONFIG]
        attachments_config = self.cfg[KEY_ATTACHMENTS_CONFIG]
        use_html_template = message_body_config[KEY_USE_HTML_TEMPLATE]

        with open(in_table_path) as in_table:
            reader = csv.DictReader(in_table)
            columns = set(reader.fieldnames)

            subject_column = None
            if subject_config.get(KEY_SUBJECT_SOURCE) == 'from_table':
                subject_column = subject_config.get(KEY_SUBJECT_COLUMN)
            else:
                subject_template_text = subject_config[KEY_SUBJECT_TEMPLATE]
                self._validate_template_text(subject_template_text, columns)

            if message_body_config[KEY_MESSAGE_BODY_SOURCE] == 'from_table':
                plaintext_template_column = message_body_config[KEY_PLAINTEXT_TEMPLATE_COLUMN]
                html_template_column = message_body_config.get(KEY_HTML_TEMPLATE_COLUMN)
            else:
                plaintext_template_column = None
                html_template_column = None
                plaintext_template_text = self._read_template_text()
                self._validate_template_text(plaintext_template_text, columns)
                if use_html_template:
                    html_template_text = self._read_template_text(plaintext=False)
                    self._validate_template_text(html_template_text, columns)

            all_attachments = attachments_config[KEY_ATTACHMENTS_SOURCE] == 'all_input_files'
            if not all_attachments:
                attachments_column = attachments_config.get(KEY_ATTACHMENTS_COLUMN)

            for row in reader:
                if subject_column is not None:
                    subject_template_text = row[subject_column]
                    self._validate_template_text(subject_template_text, columns)

                try:
                    rendered_subject = Template(subject_template_text).render(row)
                except Exception:
                    rendered_subject = subject_template_text

                if plaintext_template_column is not None:
                    plaintext_template_text = row[plaintext_template_column]
                    self._validate_template_text(plaintext_template_text, columns)

                    if html_template_column is not None:
                        html_template_text = row[html_template_column]
                        self._validate_template_text(html_template_text, columns)

                rendered_plaintext_message = Template(plaintext_template_text).render(row)

                rendered_html_message = None
                if use_html_template:
                    rendered_html_message = Template(html_template_text).render(row)

                custom_attachments_paths_by_filename = attachments_paths_by_filename
                if not all_attachments:
                    custom_attachments_paths_by_filename = {
                        attachment_filename: attachments_paths_by_filename[attachment_filename]
                        for attachment_filename in json.loads(row[attachments_column])
                    }

                email_ = self._client.build_email(
                    recipient_email_address=row[self.cfg[KEY_RECIPIENT_EMAIL_ADDRESS_COLUMN]],
                    subject=rendered_subject,
                    attachments_paths_by_filename=custom_attachments_paths_by_filename,
                    rendered_plaintext_message=rendered_plaintext_message,
                    rendered_html_message=rendered_html_message
                )

                logging.info(
                    f"Sending email with subject: `{email_['Subject']}` from `{email_['From']}` to `{email_['To']}`")
                status = 'OK'
                error_message = ''
                if not dry_run:
                    try:
                        self._client.send_email(email_)
                    except Exception as e:
                        error_message = str(e)
                        status = 'ERROR'

                rendered_html_message_writable = ''
                if rendered_html_message is not None:
                    rendered_html_message_writable = rendered_html_message.replace('\n', '<newline>')

                self._results_writer.writerow(dict(
                    status=status,
                    recipient_email_address=email_['To'],
                    sender_email_address=email_['From'],
                    subject=email_['Subject'],
                    plaintext_message_body=rendered_plaintext_message.replace('\n', '<newline>'),
                    html_message_body=rendered_html_message_writable,
                    attachment_filenames=json.dumps(list(attachments_paths_by_filename)),
                    error_message=error_message
                ))
                time.sleep(SLEEP_INTERVAL)

    def _extract_template_files_full_paths(
            self, in_files: List[FileDefinition]) -> Tuple[Union[str, None], Union[str, None]]:
        """Extracts full paths for template files if they are provided"""
        msg_body_config = self.cfg[KEY_MESSAGE_BODY_CONFIG]
        plaintext_template_path = None
        html_template_path = None
        if msg_body_config[KEY_MESSAGE_BODY_SOURCE] == 'from_template_file':
            plaintext_template_filename = msg_body_config[KEY_PLAINTEXT_TEMPLATE_FILENAME]
            plaintext_template_path = next(file.full_path for file in in_files
                                           if file.name.endswith(plaintext_template_filename))
            if msg_body_config[KEY_USE_HTML_TEMPLATE]:
                html_template_filename = msg_body_config[KEY_HTML_TEMPLATE_FILENAME]
                html_template_path = next(file.full_path for file in in_files
                                          if file.name.endswith(html_template_filename))
        return plaintext_template_path, html_template_path

    @staticmethod
    def _read_template_file(template_path: str) -> str:
        with open(template_path) as file:
            return file.read()

    @staticmethod
    def _parse_template_placeholders(template_text: str) -> set:
        placeholders = re.findall(r'\{\{.*?\}\}', template_text)
        placeholders = set([placeholder.strip('{}') for placeholder in placeholders])
        return placeholders

    def _validate_template_text(self, template_text: str, columns: set) -> None:
        template_placeholders = self._parse_template_placeholders(template_text)
        missing_columns = set(template_placeholders) - set(columns)
        if missing_columns:
            raise UserException(f"ERROR - missing columns: {missing_columns}")

    def _get_attachments_filenames_from_table(self, in_table_path):
        attachments_filenames = set()
        with open(in_table_path) as in_table:
            reader = csv.DictReader(in_table)
            attachments_column = self.cfg[KEY_ATTACHMENTS_CONFIG].get(KEY_ATTACHMENTS_COLUMN)
            for row in reader:
                for attachment_filename in json.loads(row[attachments_column]):
                    attachments_filenames.add(attachment_filename)
        return attachments_filenames

    def _validate_templates_from_table(self, reader: csv.DictReader, plaintext: str) -> ValidationResult:
        key_template_column = KEY_PLAINTEXT_TEMPLATE_COLUMN if plaintext else KEY_HTML_TEMPLATE_COLUMN
        template_column = self.cfg[KEY_MESSAGE_BODY_CONFIG][key_template_column]
        unique_placeholders = set()
        for row in reader:
            row_placeholders = self._parse_template_placeholders(template_text=row[template_column])
            unique_placeholders = unique_placeholders.union(row_placeholders)
        missing_columns = set(unique_placeholders) - set(reader.fieldnames)
        message = VALID_TEMPLATE_MESSAGE
        if missing_columns:
            message = 'ERROR - missing columns:' + ', '.join(missing_columns)
        return ValidationResult(message, MessageType.SUCCESS)

    def _read_template_text(self, plaintext: bool = True) -> str:
        """Reads in template either from file, or from config"""
        message_body_config = self.cfg[KEY_MESSAGE_BODY_CONFIG]
        message_body_source = message_body_config[KEY_MESSAGE_BODY_SOURCE]

        if message_body_source == 'from_template_file':
            template_path = self.plaintext_template_path if plaintext else self.html_template_path
            template_text = self._read_template_file(template_path)
        elif message_body_source == 'from_template_definition':
            key_template_text = KEY_PLAINTEXT_TEMPLATE_DEFINITION if plaintext else KEY_HTML_TEMPLATE_DEFINITION
            template_text = message_body_config[key_template_text]
        else:
            raise UserException('Invalid message body source')
        return template_text

    def _validate_template(self, plaintext=True) -> ValidationResult:
        self._init_configuration()
        in_tables = self.get_input_tables_definitions()
        in_table_path = in_tables[0].full_path
        with open(in_table_path) as in_table:
            reader = csv.DictReader(in_table)
            columns = set(reader.fieldnames)

            if self.cfg[KEY_MESSAGE_BODY_CONFIG].get(KEY_MESSAGE_BODY_SOURCE) == 'from_table':
                return self._validate_templates_from_table(reader, KEY_PLAINTEXT_TEMPLATE_COLUMN)

        template_text = self._read_template_text(plaintext)
        try:
            self._validate_template_text(template_text, columns)
            print(VALID_TEMPLATE_MESSAGE)
            return ValidationResult(VALID_TEMPLATE_MESSAGE, MessageType.SUCCESS)
        except UserException as e:
            print(e)
            return ValidationResult(e, MessageType.SUCCESS)

    def __exit__(self):
        self._client._smtp_server.close()

    @sync_action('test_smtp_server_connection')
    def test_smtp_server_connection(self) -> None:
        self._init_configuration()
        try:
            self.init_client()
            return ValidationResult('OK - Connection established!', MessageType.SUCCESS)
        except Exception as e:
            return ValidationResult(f"ERROR - Could not establish connection! - {e}", MessageType.SUCCESS)

    @sync_action('validate_plaintext_template')
    def validate_plaintext_template(self) -> ValidationResult:
        return self._validate_template(plaintext=True)

    @sync_action('validate_html_template')
    def validate_html_template(self) -> ValidationResult:
        return self._validate_template(plaintext=False)

    @sync_action('validate_subject')
    def validate_subject(self) -> ValidationResult:
        self._init_configuration()
        subject_config = self.cfg[KEY_SUBJECT_CONFIG]
        message = VALID_SUBJECT_MESSAGE
        subject_column = None
        if subject_config.get(KEY_SUBJECT_SOURCE) == 'from_table':
            subject_column = subject_config.get(KEY_SUBJECT_COLUMN)

        in_tables = self.get_input_tables_definitions()
        in_table_path = in_tables[0].full_path
        with open(in_table_path) as in_table:
            reader = csv.DictReader(in_table)
            columns = set(reader.fieldnames)
            if subject_column is not None:
                unique_placeholders = set()
                for row in reader:
                    subject_template_text = row[subject_column]
                    row_placeholders = self._parse_template_placeholders(subject_template_text)
                    unique_placeholders = unique_placeholders.union(row_placeholders)
                    missing_columns = set(unique_placeholders) - set(columns)
                    if missing_columns:
                        message = 'ERROR - missing placeholders:' + ', '.join(missing_columns)
            else:
                subject_template_text = subject_config[KEY_SUBJECT_TEMPLATE]
                try:
                    self._validate_template_text(subject_template_text, columns)
                except Exception as e:
                    message = str(e)
        print(message)
        return ValidationResult(message, MessageType.SUCCESS)

    @sync_action('validate_attachments')
    def validate_attachments(self) -> ValidationResult:
        self._init_configuration()
        message = VALID_ATTACHMENTS_MESSAGE
        if self.cfg[KEY_ATTACHMENTS_CONFIG][KEY_ATTACHMENTS_SOURCE] == 'all_input_files':
            print(message)
            return ValidationResult(message, MessageType.SUCCESS)

        in_tables = self.get_input_tables_definitions()
        in_table_path = in_tables[0].full_path
        in_files = self.get_input_files_definitions()
        input_filenames = {file.name for file in in_files}
        expected_input_filenames = self._get_attachments_filenames_from_table(in_table_path)
        missing_attachments = expected_input_filenames - set(input_filenames)
        if missing_attachments:
            message = 'ERROR - Missing attachments: ' + ', '.join(missing_attachments)
        print(message)
        return ValidationResult(message, MessageType.SUCCESS)

    @sync_action("validate_config")
    def validate_config(self):
        self._init_configuration()
        messages = []
        try:
            self.init_client()
            messages.append(VALID_CONNECTION_CONFIG_MESSAGE)
        except Exception as e:
            messages.append(f"ERROR - Could not establish connection! - {e}")

        for template_validation_method in (self.validate_plaintext_template, self.validate_html_template,
                                           self.validate_subject):
            template_validation_result = template_validation_method()
            template_validation_result_message = template_validation_result.message
            messages.append(template_validation_result_message)

        attachments_validation_result_message = VALID_ATTACHMENTS_MESSAGE
        if self.cfg[KEY_ATTACHMENTS_CONFIG][KEY_ATTACHMENTS_SOURCE] == 'from_table':
            attachments_validation_result = self.validate_attachments()
            attachments_validation_result_message = attachments_validation_result.message
        messages.append(attachments_validation_result_message)

        if any(message.startswith('ERROR') for message in messages):
            message_base = 'ERROR - Config Invalid!\n'
        else:
            message_base = 'OK - Config Valid!\n'
        message = message_base + '\n'.join(messages)
        print(message)
        return ValidationResult(message, MessageType.SUCCESS)


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
