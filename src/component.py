import csv
import logging
from typing import List, Union
import re
import os
import time
import json

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException
from keboola.component.base import sync_action
from keboola.component.sync_actions import ValidationResult, MessageType
from jinja2 import Template

from configuration import Configuration
from client import SMTPClient


KEY_CONNECTION_CONFIG = 'connection_config'
KEY_SENDER_EMAIL_ADDRESS = 'sender_email_address'
KEY_SENDER_PASSWORD = 'sender_password'
KEY_SERVER_HOST = 'server_host'
KEY_SERVER_PORT = 'server_port'
KEY_PROXY_SERVER_HOST = 'proxy_server_host'
KEY_PROXY_SERVER_PORT = 'proxy_server_port'
KEY_CONNECTION_PROTOCOL = 'connection_protocol'
KEY_PROXY_SERVER_USERNAME = 'proxy_server_username'
KEY_PROXY_SERVER_PASSWORD = 'proxy_server_password'

KEY_RECIPIENT_EMAIL_ADDRESS_COLUMN = 'recipient_email_address_column'

KEY_SUBJECT_CONFIG = 'subject_config'
KEY_SUBJECT_SOURCE = 'subject_source'
KEY_SUBJECT_COLUMN = 'subject_column'
KEY_SUBJECT_TEMPLATE = 'subject_template'

KEY_MESSAGE_BODY_CONFIG = 'message_body_config'
KEY_MESSAGE_BODY_COLUMN = 'message_body_column'

KEY_MESSAGE_BODY_SOURCE = 'message_body_source'
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
                        'plaintext_message_body', 'html_message_body', 'error_message')

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

    def run(self):
        """
        Main execution code
        """
        self.__init_configuration()
        self.init_client()
        in_tables = self.get_input_tables_definitions()
        in_files = self.get_input_files_definitions()
        in_table_path = in_tables[0].full_path
        in_files_paths_by_filename = {file.name: file.full_path for file in in_files}

        html_template_name = self.cfg[KEY_MESSAGE_BODY_CONFIG].get(KEY_HTML_TEMPLATE_FILENAME)
        html_template_path = in_files_paths_by_filename.pop(html_template_name, None)

        results_table = self.create_out_table_definition('results.csv', write_always=True)
        with open(results_table.full_path, 'w', newline='') as output_file:
            self._results_writer = csv.DictWriter(output_file, fieldnames=RESULT_TABLE_COLUMNS)
            self._results_writer.writeheader()
            self.send_emails(
                in_table_path,
                attachments_paths=in_files_paths_by_filename.values(),
                html_template_path=html_template_path)

        self.write_manifest(results_table)

    def __init_configuration(self):
        try:
            self._validate_parameters(self.configuration.parameters, REQUIRED_PARAMETERS, 'Row')
        except UserException as e:
            raise UserException(f"{e} The configuration is invalid. Please check that you added a configuration row.")
        self.cfg: Configuration = Configuration.fromDict(parameters=self.configuration.parameters)

    def init_client(self):
        # TODO: handle connection through proxy
        connection_config = self.cfg[KEY_CONNECTION_CONFIG]
        use_ssl = self.cfg[KEY_CONNECTION_CONFIG][KEY_CONNECTION_PROTOCOL] == 'SSL'
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

    def send_emails(self, in_table_path: str, attachments_paths: List[str], html_template_path: Union[str, None] = None) -> None:
        dry_run = self.cfg.get(KEY_DRY_RUN, False)
        if self.cfg[KEY_MESSAGE_BODY_CONFIG][KEY_MESSAGE_BODY_SOURCE] == 'from_table':
            plaintext_template_column = self.cfg[KEY_MESSAGE_BODY_CONFIG][KEY_PLAINTEXT_TEMPLATE_COLUMN]
            html_template_column = self.cfg[KEY_MESSAGE_BODY_CONFIG][KEY_HTML_TEMPLATE_COLUMN]
        else:
            plaintext_template_column = None
            html_template_column = None

        with open(in_table_path) as in_table:
            reader = csv.DictReader(in_table, quotechar='\'')
            subject_column = None
            if self.cfg[KEY_SUBJECT_CONFIG].get(KEY_SUBJECT_SOURCE) == 'from_table':
                subject_column = self.cfg[KEY_SUBJECT_CONFIG].get(KEY_SUBJECT_COLUMN)

            columns = set(reader.fieldnames)
            if self.cfg[KEY_MESSAGE_BODY_CONFIG][KEY_MESSAGE_BODY_SOURCE] != 'from_table':
                plaintext_template_text = self._read_template_text()
                self._validate_template_text(plaintext_template_text, columns)

                if html_template_path is not None:
                    html_template_text = self._read_template_text(plaintext=False)
                    self._validate_template_text(html_template_text, columns)

            all_attachments = self.cfg[KEY_ATTACHMENTS_CONFIG][KEY_ATTACHMENTS_SOURCE] == 'all_input_files'
            if not all_attachments:
                attachments_column = self.cfg[KEY_ATTACHMENTS_CONFIG].get(KEY_ATTACHMENTS_COLUMN)

            for row in reader:
                if subject_column is not None:
                    subject_template_text = row[subject_column]
                else:
                    subject_template_text = self.cfg[KEY_SUBJECT_CONFIG].get(KEY_SUBJECT_TEMPLATE)
                self._validate_template_text(subject_template_text, columns)

                try:
                    rendered_subject = Template(subject_template_text).render(row)
                except Exception:
                    rendered_subject = subject_template_text

                if plaintext_template_column is not None:
                    plaintext_template_text = row[plaintext_template_column]
                    html_template_text = row[html_template_column]

                rendered_plaintext_message = Template(plaintext_template_text).render(row)
                rendered_html_message = Template(html_template_text).render(row)

                custom_attachments_paths = attachments_paths
                if not all_attachments:
                    custom_attachments_paths = [
                        os.path.join(self.files_in_path, attachment_filename)
                        for attachment_filename in json.loads(row[attachments_column])
                    ]

                email_ = self._client.build_email(
                    recipient_email_address=row[self.cfg[KEY_RECIPIENT_EMAIL_ADDRESS_COLUMN]],
                    subject=rendered_subject,
                    attachments_paths=custom_attachments_paths,
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
                    except Exception as error_message:
                        status = 'ERROR'

                self._results_writer.writerow(dict(
                    status=status,
                    recipient_email_address=email_['To'],
                    sender_email_address=email_['From'],
                    subject=email_['Subject'],
                    plaintext_message_body=rendered_plaintext_message.replace('\n', '<newline>'),
                    html_message_body=rendered_html_message.replace('\n', '<newline>'),
                    error_message=error_message
                ))
                time.sleep(SLEEP_INTERVAL)

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

    def _get_attachments_filenames_from_input_table(self, in_table_path):
        attachments_filenames = set()
        with open(in_table_path) as in_table:
            reader = csv.DictReader(in_table, quotechar='\'')
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
        if self.cfg is None:
            self.__init_configuration()
        message_body_config = self.cfg[KEY_MESSAGE_BODY_CONFIG]
        message_body_source = message_body_config[KEY_MESSAGE_BODY_SOURCE]

        if message_body_source == 'from_template_file':
            key_template_filename = KEY_PLAINTEXT_TEMPLATE_FILENAME if plaintext else KEY_HTML_TEMPLATE_FILENAME
            template_filename = message_body_config[key_template_filename]
            template_path = os.path.join(self.files_in_path, template_filename)
            template_text = self._read_template_file(template_path)
        elif message_body_source == 'from_template_definition':
            key_template_text = KEY_PLAINTEXT_TEMPLATE_DEFINITION if plaintext else KEY_HTML_TEMPLATE_DEFINITION
            template_text = message_body_config[key_template_text]
        else:
            raise UserException('Invalid message body source')
        return template_text

    def _validate_template(self, plaintext=True) -> ValidationResult:
        self.__init_configuration()
        in_tables = self.get_input_tables_definitions()
        in_table_path = in_tables[0].full_path
        with open(in_table_path) as in_table:
            reader = csv.DictReader(in_table, quotechar='\'')
            columns = set(reader.fieldnames)

            if self.cfg[KEY_MESSAGE_BODY_CONFIG].get(KEY_MESSAGE_BODY_SOURCE) == 'from_table':
                return self._validate_templates_from_table(reader, KEY_PLAINTEXT_TEMPLATE_COLUMN)

        template_text = self._read_template_text(plaintext)
        try:
            self._validate_template_text(template_text, columns)
            return ValidationResult(VALID_TEMPLATE_MESSAGE, MessageType.SUCCESS)
        except UserException as e:
            return ValidationResult(e, MessageType.SUCCESS)

    @sync_action('test_smtp_server_connection')
    def test_smtp_server_connection(self) -> None:
        self.__init_configuration()
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
        message = VALID_SUBJECT_MESSAGE
        subject_column = None
        if self.cfg[KEY_SUBJECT_CONFIG].get(KEY_SUBJECT_SOURCE) == 'from_table':
            subject_column = self.cfg[KEY_SUBJECT_CONFIG].get(KEY_SUBJECT_COLUMN)

        in_tables = self.get_input_tables_definitions()
        in_table_path = in_tables[0].full_path
        with open(in_table_path) as in_table:
            reader = csv.DictReader(in_table, quotechar='\'')
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
                subject_template_text = self.cfg[KEY_SUBJECT_CONFIG].get(KEY_SUBJECT_TEMPLATE)
                try:
                    self._validate_template_text(subject_template_text, columns)
                except Exception as e:
                    message = str(e)
        return ValidationResult(message, MessageType.SUCCESS)

    @sync_action('validate_attachments')
    def validate_attachments(self) -> ValidationResult:
        self.__init_configuration()
        if self.cfg[KEY_ATTACHMENTS_CONFIG][KEY_ATTACHMENTS_SOURCE] == 'all_input_files':
            return ValidationResult(VALID_ATTACHMENTS_MESSAGE, MessageType.SUCCESS)

        in_tables = self.get_input_tables_definitions()
        in_table_path = in_tables[0].full_path
        in_files = self.get_input_files_definitions()
        input_filenames = {file.name for file in in_files}
        expected_input_filenames = self._get_attachments_filenames_from_input_table(in_table_path)
        missing_attachments = expected_input_filenames - set(input_filenames)
        message = VALID_ATTACHMENTS_MESSAGE
        if missing_attachments:
            message = 'ERROR - Missing attachments: ' + ', '.join(missing_attachments)
        return ValidationResult(message, MessageType.SUCCESS)

    @sync_action("validate_config")
    def validate_config(self):
        self.__init_configuration()
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
