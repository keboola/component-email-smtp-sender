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
from kbcstorage.client import Client as StorageClient
from jinja2 import Template

from configuration import Configuration
from client import SMTPClient


KEY_PLAINTEXT_TEMPLATE_COLUMN = 'plaintext_template_column'
KEY_HTML_TEMPLATE_COLUMN = 'html_template_column'
KEY_PLAINTEXT_TEMPLATE_DEFINITION = 'plaintext_template_definition'
KEY_HTML_TEMPLATE_DEFINITION = 'html_template_definition'

SLEEP_INTERVAL = 0.1

RESULT_TABLE_COLUMNS = ('status', 'recipient_email_address', 'sender_email_address', 'subject',
                        'plaintext_message_body', 'html_message_body', 'attachment_filenames', 'error_message')

VALID_CONNECTION_CONFIG_MESSAGE = '✅ - Connection configuration is valid'
VALID_SUBJECT_MESSAGE = '✅ - All subject placeholders are present in the input table'
VALID_TEMPLATE_MESSAGE = '✅ - All template placeholders are present in the input table'
VALID_ATTACHMENTS_MESSAGE = '✅ - All attachments are present'

general_error_row = {
    'status': 'ERROR',
    'recipient_email_address': '',
    'sender_email_address': '',
    'subject': '',
    'plaintext_message_body': '',
    'html_message_body': '',
    'attachment_filenames': ''}


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
        in_files_by_name = self.get_input_file_definitions_grouped_by_name()
        in_table_path = in_tables[0].full_path
        self.plaintext_template_path, self.html_template_path = \
            self._extract_template_files_full_paths(in_files_by_name)

        attachments_paths_by_filename = {
            name: files[0].full_path
            for name, files in in_files_by_name.items()
            if files[0].full_path not in [self.plaintext_template_path, self.html_template_path]}

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
        connection_config = self.cfg.connection_config
        proxy_server_config = connection_config.proxy_server_config
        self._client = SMTPClient(
            sender_email_address=connection_config.sender_email_address,
            password=connection_config.pswd_sender_password,
            server_host=connection_config.server_host,
            server_port=connection_config.server_port,
            connection_protocol=connection_config.connection_protocol,
            proxy_server_host=proxy_server_config.proxy_server_host,
            proxy_server_port=proxy_server_config.proxy_server_port,
            proxy_server_username=proxy_server_config.proxy_server_username,
            proxy_server_password=proxy_server_config.pswd_proxy_server_password)
        self._client.init_smtp_server()

    def send_emails(self, in_table_path: str, attachments_paths_by_filename: Dict[str, str]) -> None:
        continue_on_error = self.cfg.continue_on_error
        dry_run = self.cfg.dry_run
        subject_config = self.cfg.subject_config
        message_body_config = self.cfg.message_body_config
        attachments_config = self.cfg.attachments_config
        use_html_template = message_body_config.use_html_template

        with open(in_table_path) as in_table:
            reader = csv.DictReader(in_table)
            columns = set(reader.fieldnames)

            subject_column = None
            if subject_config.subject_source == 'from_table':
                subject_column = subject_config.subject_column
            else:
                subject_template_text = subject_config.subject_template
                self._validate_template_text(subject_template_text, columns)

            if message_body_config.message_body_source == 'from_table':
                plaintext_template_column = message_body_config.plaintext_template_column
                html_template_column = message_body_config.html_template_column
            else:
                plaintext_template_column = None
                html_template_column = None
                plaintext_template_text = self._read_template_text()
                self._validate_template_text(plaintext_template_text, columns)
                if use_html_template:
                    html_template_text = self._read_template_text(plaintext=False)
                    self._validate_template_text(html_template_text, columns)

            all_attachments = attachments_config.attachments_source == 'all_input_files'
            if not all_attachments:
                attachments_column = attachments_config.attachments_column

            for row in reader:
                recipient_email_address = row[self.cfg.recipient_email_address_column]
                try:
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
                        recipient_email_address=recipient_email_address,
                        subject=rendered_subject,
                        attachments_paths_by_filename=custom_attachments_paths_by_filename,
                        rendered_plaintext_message=rendered_plaintext_message,
                        rendered_html_message=rendered_html_message)

                    logging.info(
                        f"Sending email with subject: `{email_['Subject']}`"
                        f" from `{email_['From']}` to `{email_['To']}`")
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
                        rendered_html_message_writable = rendered_html_message

                    self._results_writer.writerow(dict(
                        status=status,
                        recipient_email_address=email_['To'],
                        sender_email_address=email_['From'],
                        subject=email_['Subject'],
                        plaintext_message_body=rendered_plaintext_message,
                        html_message_body=rendered_html_message_writable,
                        attachment_filenames=json.dumps(list(attachments_paths_by_filename)),
                        error_message=error_message))
                    time.sleep(SLEEP_INTERVAL)
                except Exception as e:
                    if not continue_on_error:
                        raise UserException(
                            'Error occurred, when trying to send an email. Please validate your configuration.')
                    self._results_writer.writerow({
                        **general_error_row,
                        'sender_email_address': self._client.sender_email_address,
                        'recipient_email_address': recipient_email_address,
                        'error_message': str(e)})

    def _extract_template_files_full_paths(
            self, in_files_by_name: Dict[str, List[FileDefinition]]) -> Tuple[Union[str, None], Union[str, None]]:
        """Extracts full paths for template files if they are provided"""
        msg_body_config = self.cfg.message_body_config
        plaintext_template_path = None
        html_template_path = None
        if msg_body_config.message_body_source == 'from_template_file':
            plaintext_template_filename = msg_body_config.plaintext_template_filename
            plaintext_template_path = next(files[0].full_path for name, files in in_files_by_name.items()
                                           if files[0].name.endswith(plaintext_template_filename))
            if msg_body_config.use_html_template:
                html_template_filename = msg_body_config.html_template_filename
                html_template_path = next(files[0].full_path for name, files in in_files_by_name.items()
                                          if files[0].name.endswith(html_template_filename))
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

    def _validate_template_text(self, template_text: str, columns: set, continue_on_error: bool = False) -> None:
        template_placeholders = self._parse_template_placeholders(template_text)
        missing_columns = set(template_placeholders) - set(columns)
        if missing_columns:
            if not continue_on_error:
                raise UserException("❌ - missing columns: " + ', '.join(missing_columns))

    def _get_attachments_filenames_from_table(self, in_table_path):
        attachments_filenames = set()
        with open(in_table_path) as in_table:
            reader = csv.DictReader(in_table)
            attachments_column = self.cfg.attachments_config.attachments_column
            for row in reader:
                for attachment_filename in json.loads(row[attachments_column]):
                    attachments_filenames.add(attachment_filename)
        return attachments_filenames

    def _validate_templates_from_table(self, reader: csv.DictReader, plaintext: bool) -> ValidationResult:
        key_template_column = KEY_PLAINTEXT_TEMPLATE_COLUMN if plaintext else KEY_HTML_TEMPLATE_COLUMN
        template_column = self.cfg.message_body_config[key_template_column]
        unique_placeholders = set()
        for row in reader:
            row_placeholders = self._parse_template_placeholders(template_text=row[template_column])
            unique_placeholders = unique_placeholders.union(row_placeholders)
        missing_columns = set(unique_placeholders) - set(reader.fieldnames)
        message = VALID_TEMPLATE_MESSAGE
        if missing_columns:
            message = '❌ - missing columns: ' + ', '.join(missing_columns)
        return ValidationResult(message, MessageType.SUCCESS)

    def _read_template_text(self, plaintext: bool = True) -> str:
        """Reads in template either from file, or from config"""
        message_body_config = self.cfg.message_body_config
        message_body_source = message_body_config.message_body_source

        if message_body_source == 'from_template_file':
            template_path = self.plaintext_template_path if plaintext else self.html_template_path
            template_text = self._read_template_file(template_path)
        elif message_body_source == 'from_template_definition':
            key_template_text = KEY_PLAINTEXT_TEMPLATE_DEFINITION if plaintext else KEY_HTML_TEMPLATE_DEFINITION
            template_text = message_body_config[key_template_text]
        else:
            raise UserException('Invalid message body source')
        return template_text

    def _list_input_filenames_in_sync_action(self):
        #  TODO: validate this
        self._init_configuration()
        filenames = [file.destination for file in self.configuration.files_input_mapping]
        return filenames

    def _init_storage_client(self):
        storage_token = self.environment_variables.token
        storage_client = StorageClient('https://connection.keboola.com', storage_token)
        return storage_client

    def _download_table_from_storage_api(self) -> str:
        storage_client = self._init_storage_client()
        table_id = self.configuration.tables_input_mapping[0].source
        table = storage_client.tables.export_to_file(table_id=table_id, path_name='data')
        # TODO: validate that returned value actually contains full_table attribute
        return table.full_path

    def _download_file_from_storage_api(self) -> str:
        storage_client = self._init_storage_client()
        file_id = self.configuration.files_input_mapping[0].source
        file = storage_client.files.export_to_file(file_id=file_id, path_name='data')
        # TODO: validate that returned object actually contains full_path attribute
        return file.full_path

    def _validate_template(self, plaintext=True) -> ValidationResult:
        self._init_configuration()
        in_table_path = self._download_table_from_storage_api()
        with open(in_table_path) as in_table:
            reader = csv.DictReader(in_table, quotechar='\'')
            columns = set(reader.fieldnames)

            if self.cfg.message_body_config.message_body_source == 'from_table':
                return self._validate_templates_from_table(reader, KEY_PLAINTEXT_TEMPLATE_COLUMN)

        template_text = self._read_template_text(plaintext)
        try:
            self._validate_template_text(template_text, columns)
            print(VALID_TEMPLATE_MESSAGE)
            return ValidationResult(VALID_TEMPLATE_MESSAGE, MessageType.SUCCESS)
        except UserException as e:
            print(e)
            return ValidationResult(e, MessageType.DANGER)

    def __exit__(self):
        self._client._smtp_server.close()

    @sync_action('testConnection')
    def test_smtp_server_connection(self) -> None:
        self._init_configuration()
        try:
            self.init_client()
            return ValidationResult('✅ Connection successful!', MessageType.SUCCESS)
        except Exception:
            return ValidationResult('❌ Connection failed', MessageType.DANGER)

    @sync_action('validate_plaintext_template')
    def validate_plaintext_template(self) -> ValidationResult:
        return self._validate_template(plaintext=True)

    @sync_action('validate_html_template')
    def validate_html_template(self) -> ValidationResult:
        return self._validate_template(plaintext=False)

    @sync_action('validate_subject')
    def validate_subject(self) -> ValidationResult:
        self._init_configuration()
        subject_config = self.cfg.subject_config
        message = VALID_SUBJECT_MESSAGE
        subject_column = None
        if subject_config.subject_source == 'from_table':
            subject_column = subject_config.subject_column

        in_table_path = self._download_table_from_storage_api()
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
                        message = '❌ - missing columns: ' + ', '.join(missing_columns)
            else:
                subject_template_text = subject_config.subject_template
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
        if self.cfg.attachments_config.attachments_source == 'all_input_files':
            print(message)
            return ValidationResult(message, MessageType.SUCCESS)

        input_filenames = self._list_input_filenames_in_sync_action()
        in_table_path = self._download_table_from_storage_api()
        expected_input_filenames = self._get_attachments_filenames_from_table(in_table_path)
        missing_attachments = expected_input_filenames - set(input_filenames)
        if missing_attachments:
            message = '❌ - Missing attachments: ' + ', '.join(missing_attachments)
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
            messages.append(f"❌ - Could not establish connection! - {e}")

        for template_validation_method in (self.validate_plaintext_template, self.validate_html_template,
                                           self.validate_subject):
            template_validation_result = template_validation_method()
            template_validation_result_message = template_validation_result.message
            messages.append(template_validation_result_message)

        attachments_validation_result_message = VALID_ATTACHMENTS_MESSAGE
        if self.cfg.attachments_config.attachments_source == 'from_table':
            attachments_validation_result = self.validate_attachments()
            attachments_validation_result_message = attachments_validation_result.message
        messages.append(attachments_validation_result_message)

        if any(message.startswith('❌') for message in messages):
            message_base = '❌ - Config Invalid!\n'
        else:
            message_base = '✅ - Config Valid!\n'
        message = message_base + '\n'.join(messages)
        print(message)
        if message.startswith('✅'):
            return ValidationResult(message, MessageType.SUCCESS)
        else:
            return ValidationResult(message, MessageType.DANGER)


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
