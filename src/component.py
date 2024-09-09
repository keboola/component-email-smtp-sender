import csv
import logging
from typing import List, Tuple, Union, Dict, Set
import re
import time
import json
import os
from io import StringIO
from pathlib import Path

from keboola.component.base import ComponentBase, sync_action
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import ValidationResult, MessageType, SelectElement
from keboola.component.dao import FileDefinition
from kbcstorage.client import Client as StorageClient
from kbcstorage.tables import Tables as StorageTables
from jinja2 import Template

from configuration import Configuration, ConnectionConfig, AdvancedEmailOptions
from client import SMTPClient
from stack_overrides import StackOverridesParameters


KEY_PLAINTEXT_TEMPLATE_COLUMN = 'plaintext_template_column'
KEY_HTML_TEMPLATE_COLUMN = 'html_template_column'
KEY_PLAINTEXT_TEMPLATE_FILENAME = 'plaintext_template_filename'
KEY_HTML_TEMPLATE_FILENAME = 'html_template_filename'
KEY_PLAINTEXT_TEMPLATE_DEFINITION = 'plaintext_template_definition'
KEY_HTML_TEMPLATE_DEFINITION = 'html_template_definition'

# STACK OVERRIDES
KEY_ALLOWED_HOSTS = 'allowed_hosts'
KEY_ADDRESS_WHITELIST = 'address_whitelist'
KEY_DISABLE_ATTACHMENTS = 'disable_attachments'

SLEEP_INTERVAL = 0.1

RESULT_TABLE_COLUMNS = ('status', 'recipient_email_address', 'sender_email_address', 'subject',
                        'plaintext_message_body', 'html_message_body', 'attachment_filenames', 'error_message')

VALID_CONNECTION_CONFIG_MESSAGE = '✅ - Connection configuration is valid'
VALID_SUBJECT_MESSAGE = '✅ - All subject placeholders are present in the input table'
VALID_PLAINTEXT_TEMPLATE_MESSAGE = '✅ - All plaintext template placeholders are present in the input table'
VALID_HTML_TEMPLATE_MESSAGE = '✅ - All HTML template placeholders are present in the input table'
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
        email_data_table_name = self.cfg.advanced_options.email_data_table_name
        email_data_table_path = self.load_email_data_table_path(in_tables, email_data_table_name)
        self.plaintext_template_path, self.html_template_path = \
            self._extract_template_files_full_paths(in_files_by_name)
        attachments_paths_by_filename = \
            self.load_attachment_paths_by_filename(in_tables, email_data_table_name, in_files_by_name)

        results_table = self.create_out_table_definition('results.csv', write_always=True)
        with open(results_table.full_path, 'w', newline='') as output_file:
            self._results_writer = csv.DictWriter(output_file, fieldnames=RESULT_TABLE_COLUMNS)
            self._results_writer.writeheader()
            self._results_writer.errors = False
            self.send_emails(email_data_table_path=email_data_table_path,
                             attachments_paths_by_filename=attachments_paths_by_filename)
        self.write_manifest(results_table)

        if self._results_writer.errors:
            raise UserException("Some emails couldn't be sent - check results.csv for more details.")

    def _init_configuration(self) -> None:
        self.validate_configuration_parameters(Configuration.get_dataclass_required_parameters())
        self.cfg: Configuration = Configuration.load_from_dict(self.configuration.parameters)

    def _load_stack_overrides(self) -> StackOverridesParameters:
        image_parameters = self.configuration.image_parameters or {}

        allowed_hosts = image_parameters.get(KEY_ALLOWED_HOSTS, [])
        address_whitelist = image_parameters.get(KEY_ADDRESS_WHITELIST, [])
        disable_attachments = image_parameters.get(KEY_DISABLE_ATTACHMENTS, False)

        return StackOverridesParameters(
            allowed_hosts=allowed_hosts,
            address_whitelist=address_whitelist,
            disable_attachments=disable_attachments
        )

    def init_client(self, connection_config: Union[ConnectionConfig, None] = None) -> None:
        if connection_config is None:
            connection_config = self.cfg.connection_config

        proxy_server_config = connection_config.creds_config.proxy_server_config
        oauth_config = connection_config.oauth_config
        creds_config = connection_config.creds_config

        overrides: StackOverridesParameters = self._load_stack_overrides()
        self.validate_allowed_hosts(overrides, creds_config)

        self._client = SMTPClient(
            use_oauth=connection_config.use_oauth,
            sender_email_address=creds_config.sender_email_address or oauth_config.sender_email_address,
            password=creds_config.pswd_sender_password,
            server_host=creds_config.server_host,
            server_port=creds_config.server_port,
            connection_protocol=creds_config.connection_protocol,
            proxy_server_host=proxy_server_config.proxy_server_host,
            proxy_server_port=proxy_server_config.proxy_server_port,
            proxy_server_username=proxy_server_config.proxy_server_username,
            proxy_server_password=proxy_server_config.pswd_proxy_server_password,
            tenant_id=oauth_config.tenant_id,
            client_id=oauth_config.client_id,
            client_secret=oauth_config.pswd_client_secret,
            address_whitelist=overrides.address_whitelist,
            disable_attachments=overrides.disable_attachments
        )

        self._client.init_smtp_server()

    @staticmethod
    def validate_allowed_hosts(overrides: StackOverridesParameters, creds_config) -> None:
        if overrides.allowed_hosts:
            match = False
            for item in overrides.allowed_hosts:
                if item.get('host') == creds_config.server_host and item.get('port') == creds_config.server_port:
                    match = True

            if not match:
                raise UserException(f"Host {creds_config.server_host}:{creds_config.server_port} is not allowed")

    @staticmethod
    def load_email_data_table_path(in_tables, email_data_table_name):
        try:
            table_path = next(in_table.full_path for in_table in in_tables
                              if in_table.name == email_data_table_name)
        except StopIteration:
            table_path = None
        return table_path

    @staticmethod
    def _load_attachment_tables(in_tables, table_to_exclude):
        tables = {
            in_table.name: in_table.full_path
            for in_table in in_tables
            if in_table.name != table_to_exclude}
        return tables

    def _load_attachment_files(self, in_files_by_name):
        attachment_files = {}
        for name, files in in_files_by_name.items():
            file = files[0]
            original_path = file.full_path
            if original_path not in [self.plaintext_template_path, self.html_template_path]:
                directory = os.path.split(original_path)[0]
                new_path = os.path.join(directory, file.name)
                Path.rename(original_path, new_path)
                attachment_files[file.name] = new_path
        return attachment_files

    def load_attachment_paths_by_filename(self, in_tables, email_data_table_name, in_files_by_name):
        if self.cfg.configuration_type == 'basic' and not self.cfg.basic_options.include_attachments:
            return {}
        table_attachments_paths_by_filename = self._load_attachment_tables(in_tables, email_data_table_name)
        file_attachments_paths_by_filename = self._load_attachment_files(in_files_by_name)
        return {**table_attachments_paths_by_filename, **file_attachments_paths_by_filename}

    def send_emails(self, attachments_paths_by_filename: Dict[str, str],
                    email_data_table_path: Union[str, None] = None) -> None:
        continue_on_error = self.cfg.continue_on_error
        dry_run = self.cfg.dry_run
        use_advanced_options = self.cfg.configuration_type == 'advanced'
        basic_options = self.cfg.basic_options
        advanced_options = self.cfg.advanced_options
        subject_config = advanced_options.subject_config
        message_body_config = advanced_options.message_body_config
        attachments_config = advanced_options.attachments_config
        use_html_template = message_body_config.use_html_template
        subject_column = None
        plaintext_template_column = None
        html_template_column = None
        all_attachments = attachments_config.attachments_source == 'all_input_files'
        attachments_column = attachments_config.attachments_column

        if email_data_table_path:
            in_table = open(email_data_table_path)
            reader = csv.DictReader(in_table)
            columns = set(reader.fieldnames)

            if subject_config.subject_source == 'from_table':
                subject_column = subject_config.subject_column
            else:
                subject_template_text = subject_config.subject_template_definition
                self._validate_template_text(subject_template_text, columns)

            if message_body_config.message_body_source == 'from_table':
                plaintext_template_column = message_body_config.plaintext_template_column
                if use_html_template:
                    html_template_column = message_body_config.html_template_column
            else:
                plaintext_template_text = self._read_template_text()
                self._validate_template_text(plaintext_template_text, columns)
                if use_html_template:
                    html_template_text = self._read_template_text(plaintext=False)
                    self._validate_template_text(html_template_text, columns)
        else:
            try:
                reader = iter(basic_options.recipient_email_addresses.split(','))
            except AttributeError:
                raise UserException("No input table found with specified name or no recipient email addresses provided")

        for row in reader:
            try:
                recipient_email_address = row
                if isinstance(reader, csv.DictReader):
                    recipient_email_address = row[advanced_options.recipient_email_address_column]

                if not use_advanced_options:
                    rendered_subject = basic_options.subject
                    rendered_plaintext_message = basic_options.message_body
                    rendered_html_message = None
                    custom_attachments_paths_by_filename = attachments_paths_by_filename
                else:
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
                    if not self._client.disable_attachments:
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

                status = 'OK'
                error_message = ''
                if not dry_run:

                    try:
                        logging.info(
                            f"Sending email with subject: `{email_['Subject']}`"
                            f" from `{email_['From']}` to `{email_['To']}`")

                        if self._client.disable_attachments:
                            attachment_paths = None
                        else:
                            attachment_paths = custom_attachments_paths_by_filename.values()

                        self._client.send_email(email_, message_body=rendered_plaintext_message,
                                                html_message_body=rendered_html_message,
                                                attachments_paths=attachment_paths)

                    except Exception as e:
                        error_message = str(e)
                        status = 'ERROR'
                        self._results_writer.errors = True

                rendered_html_message_writable = ''
                if rendered_html_message:
                    rendered_html_message_writable = rendered_html_message

                attachments_to_log = json.dumps(list(custom_attachments_paths_by_filename)) \
                    if custom_attachments_paths_by_filename else []

                self._results_writer.writerow(dict(
                    status=status,
                    recipient_email_address=email_['To'],
                    sender_email_address=email_['From'],
                    subject=email_['Subject'],
                    plaintext_message_body=rendered_plaintext_message,
                    html_message_body=rendered_html_message_writable,
                    attachment_filenames=attachments_to_log,
                    error_message=error_message))
                if error_message and not continue_on_error:
                    break
                time.sleep(SLEEP_INTERVAL)

            except Exception as e:
                self._results_writer.writerow({
                    **general_error_row,
                    'sender_email_address': self._client.sender_email_address,
                    'recipient_email_address': recipient_email_address,
                    'error_message': str(e)})
                self._results_writer.errors = True
                if not continue_on_error:
                    break

        try:
            in_table.close()
        except NameError:
            pass

    def _extract_template_files_full_paths(
            self, in_files_by_name: Dict[str, List[FileDefinition]]) -> Tuple[Union[str, None], Union[str, None]]:
        """Extracts full paths for template files if they are provided"""
        msg_body_config = self.cfg.advanced_options.message_body_config
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
    def _parse_template_placeholders(template_text: str) -> Set[str]:
        placeholders = re.findall(r'\{\{.*?\}\}', template_text)
        placeholders = set([placeholder.strip('{}') for placeholder in placeholders])
        return placeholders

    def _validate_template_text(self, template_text: str, columns: set, continue_on_error: bool = False) -> None:
        template_placeholders = self._parse_template_placeholders(template_text)
        missing_columns = set(template_placeholders) - set(columns)
        if missing_columns:
            if not continue_on_error:
                raise UserException("❌ - Missing columns: " + ', '.join(missing_columns))

    def _get_attachments_filenames_from_table(self, in_table_path: str) -> Set[str]:
        attachments_filenames = set()
        with open(in_table_path) as in_table:
            reader = csv.DictReader(in_table)
            attachments_column = self.cfg.advanced_options.attachments_config.attachments_column
            for row in reader:
                for attachment_filename in json.loads(row[attachments_column]):
                    attachments_filenames.add(attachment_filename)
        return attachments_filenames

    def _get_missing_columns_from_table(self, reader: csv.DictReader, column: str) -> Set[str]:
        unique_placeholders = set()
        for row in reader:
            row_placeholders = self._parse_template_placeholders(template_text=row[column])
            unique_placeholders = unique_placeholders.union(row_placeholders)
        missing_columns = set(unique_placeholders) - set(reader.fieldnames)
        return missing_columns

    def _validate_templates_from_table(self, reader: csv.DictReader, plaintext: bool) -> ValidationResult:
        message = VALID_PLAINTEXT_TEMPLATE_MESSAGE if plaintext else VALID_HTML_TEMPLATE_MESSAGE
        key_template_column = KEY_PLAINTEXT_TEMPLATE_COLUMN if plaintext else KEY_HTML_TEMPLATE_COLUMN
        message_type = MessageType.SUCCESS
        template_column = self.cfg.advanced_options.message_body_config[key_template_column]
        missing_columns = self._get_missing_columns_from_table(reader, template_column)
        if missing_columns:
            message = '❌ - Missing columns: ' + ', '.join(missing_columns)
            message_type = MessageType.DANGER
        return ValidationResult(message, message_type)

    def _read_template_text(self, plaintext: bool = True) -> str:
        """Reads in template either from file, or from config"""
        message_body_config = self.cfg.advanced_options.message_body_config
        message_body_source = message_body_config.message_body_source

        if message_body_source == 'from_template_file':
            key_template_filename = KEY_PLAINTEXT_TEMPLATE_FILENAME if plaintext else KEY_HTML_TEMPLATE_FILENAME
            template_filename = message_body_config[key_template_filename]
            files = self._list_files_in_sync_actions()
            if not files:
                raise UserException('No files found in the storage. Please use tags to select your files instead of '
                                    'query.')
            template_file_id = next(file['id'] for file in files if file['name'] == template_filename)
            template_path = self._download_file_from_storage_api(template_file_id)
            template_text = self._read_template_file(template_path)
        elif message_body_source == 'from_template_definition':
            key_template_text = KEY_PLAINTEXT_TEMPLATE_DEFINITION if plaintext else KEY_HTML_TEMPLATE_DEFINITION
            template_text = message_body_config[key_template_text]
        else:
            raise UserException('Invalid message body source')
        return template_text

    def _init_storage_client(self) -> StorageClient:
        storage_token = self.environment_variables.token
        storage_client = StorageClient(self.environment_variables.url, storage_token)
        return storage_client

    def _download_table_from_storage_api(self, table_name) -> str:
        storage_client = self._init_storage_client()
        table_id = next(table.source for table in self.configuration.tables_input_mapping
                        if table.destination == table_name)
        table_path = storage_client.tables.export_to_file(table_id=table_id, path_name=self.files_in_path)
        return table_path

    def _download_file_from_storage_api(self, file_id) -> str:
        storage_client = self._init_storage_client()
        file_path = storage_client.files.download(file_id=file_id, local_path=self.files_in_path)
        return file_path

    def _list_files_in_sync_actions(self) -> List[Dict]:
        storage_client = self._init_storage_client()
        all_input_files = []
        try:
            for file_input in self.configuration.config_data['storage']['input']['files']:
                tags = [tag['name'] for tag in file_input['source']['tags']]
                input_files = storage_client.files.list(tags=tags)
                all_input_files.extend(input_files)
            return all_input_files
        except KeyError:
            return []

    def _validate_template(self, plaintext: bool = True) -> ValidationResult:
        self._init_configuration()
        valid_message = VALID_PLAINTEXT_TEMPLATE_MESSAGE if plaintext else VALID_HTML_TEMPLATE_MESSAGE
        if self.cfg.advanced_options.message_body_config.message_body_source == 'from_table':
            table_name = self.cfg.advanced_options.email_data_table_name
            in_table_path = self._download_table_from_storage_api(table_name)
            with open(in_table_path) as in_table:
                reader = csv.DictReader(in_table)
                return self._validate_templates_from_table(reader, plaintext)
        else:
            template_text = self._read_template_text(plaintext)
            try:
                columns = self.load_input_table_columns_()
                self._validate_template_text(template_text, columns)
                return ValidationResult(valid_message, MessageType.SUCCESS)
            except UserException as e:
                return ValidationResult(str(e), MessageType.DANGER)

    def __exit__(self):
        self._client.smtp_server.close()

    def test_smtp_server_connection_(self) -> ValidationResult:
        connection_config = ConnectionConfig.load_from_dict(self.configuration.parameters['connection_config'])
        try:
            self.init_client(connection_config=connection_config)
            return ValidationResult('✅ - Connection established successfully', MessageType.SUCCESS)
        except Exception as e:
            return ValidationResult(f"❌ - Connection couldn't be established. Error: {e}", MessageType.DANGER)

    @sync_action('testConnection')
    def test_smtp_server_connection(self) -> ValidationResult:
        return self.test_smtp_server_connection_()

    @sync_action("load_input_table_selection")
    def load_input_table_selection(self) -> List[SelectElement]:
        self._init_configuration()
        return [SelectElement(table.destination) for table in self.configuration.tables_input_mapping]

    def load_input_table_columns_(self) -> Union[List[str], ValidationResult]:
        advanced_options = AdvancedEmailOptions.load_from_dict(self.configuration.parameters['advanced_options'])
        table_name = advanced_options.email_data_table_name
        if table_name is None:
            message = "You must specify `Email Data Table Name` before loading columns"
            return ValidationResult(message, MessageType.DANGER)
        try:
            table_id = next(table.source for table in self.configuration.tables_input_mapping
                            if table.destination == table_name)
            storage_url = f'https://{self.environment_variables.stack_id}' if self.environment_variables.stack_id \
                          else "https://connection.keboola.com"
            tables = StorageTables(storage_url, self.environment_variables.token)
            preview = tables.preview(table_id)
            reader = csv.DictReader(StringIO(preview))
            return reader.fieldnames
        except Exception:
            return ValidationResult("Couldn't fetch columns", MessageType.DANGER)

    @sync_action('load_input_table_columns')
    def load_input_table_columns(self) -> List[SelectElement]:
        columns = self.load_input_table_columns_()
        if isinstance(columns, ValidationResult):
            return columns
        return [SelectElement(column) for column in columns]

    def validate_subject_(self) -> ValidationResult:
        self._init_configuration()
        subject_config = self.cfg.advanced_options.subject_config
        message = VALID_SUBJECT_MESSAGE
        if subject_config.subject_source == 'from_table':
            subject_column = subject_config.subject_column
            table_name = self.cfg.advanced_options.email_data_table_name
            in_table_path = self._download_table_from_storage_api(table_name)
            with open(in_table_path) as in_table:
                reader = csv.DictReader(in_table)
                missing_columns = self._get_missing_columns_from_table(reader, subject_column)
                if missing_columns:
                    message = '❌ - Missing columns: ' + ', '.join(missing_columns)
        else:
            subject_template_text = subject_config.subject_template_definition
            columns = self.load_input_table_columns_()
            try:
                self._validate_template_text(subject_template_text, columns)
            except Exception as e:
                message = str(e)
        message_type = MessageType.SUCCESS if message == VALID_SUBJECT_MESSAGE else MessageType.DANGER
        return ValidationResult(message, message_type)

    @sync_action('validate_subject')
    def validate_subject(self) -> ValidationResult:
        return self.validate_subject_()

    def validate_plaintext_template_(self) -> ValidationResult:
        return self._validate_template(plaintext=True)

    @sync_action('validate_plaintext_template')
    def validate_plaintext_template(self) -> ValidationResult:
        return self.validate_plaintext_template_()

    def validate_html_template_(self) -> ValidationResult:
        return self._validate_template(plaintext=False)

    @sync_action('validate_html_template')
    def validate_html_template(self) -> ValidationResult:
        return self.validate_html_template_()

    def validate_attachments_(self) -> ValidationResult:
        self._init_configuration()
        message = VALID_ATTACHMENTS_MESSAGE
        try:
            if self.cfg.advanced_options.attachments_config.attachments_source != 'all_input_files':
                table_name = self.cfg.advanced_options.email_data_table_name
                in_table_path = self._download_table_from_storage_api(table_name)
                expected_input_filenames = self._get_attachments_filenames_from_table(in_table_path)
                input_filenames = set([file['name'] for file in self._list_files_in_sync_actions()])
                input_tables = set([table.destination for table in self.configuration.tables_input_mapping])
                missing_attachments = expected_input_filenames - input_filenames - input_tables
                if missing_attachments:
                    message = '❌ - Missing attachments: ' + ', '.join(missing_attachments)
        except Exception as e:
            message = f"❌ - Couldn't validate attachments. Error: {e}"
        message_type = MessageType.SUCCESS if message == VALID_ATTACHMENTS_MESSAGE else MessageType.DANGER
        return ValidationResult(message, message_type)

    @sync_action('validate_attachments')
    def validate_attachments(self) -> ValidationResult:
        return self.validate_attachments_()

    @sync_action("validate_config")
    def validate_config(self) -> ValidationResult:
        self._init_configuration()
        # TODO: once sys.stdout is None handling is released, remove helper methods and use other sync actions directly
        validation_methods = [
            self.test_smtp_server_connection_,
            self.validate_subject_,
            self.validate_plaintext_template_,
            self.validate_attachments_]
        if self.cfg.advanced_options.message_body_config.use_html_template:
            validation_methods.insert(3, self.validate_html_template_)

        messages = [validation_method().message for validation_method in validation_methods]

        if any(message.startswith('❌') for message in messages):
            message_type = MessageType.DANGER
        else:
            message_type = MessageType.SUCCESS

        message = '\n\n'.join(messages)
        return ValidationResult(message, message_type)


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
