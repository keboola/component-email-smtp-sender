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

# configuration variables
KEY_SENDER_EMAIL_ADDRESS = 'sender_email_address'
KEY_SENDER_PASSWORD = 'sender_password'
KEY_SERVER_HOST = 'server_host'
KEY_SERVER_PORT = 'server_port'
KEY_CONNECTION_PROTOCOL = 'connection_protocol'

KEY_RECIPIENT_EMAIL_ADDRESS_COLUMN = 'recipient_email_address_column'
KEY_MESSAGE_BODY_COLUMN = 'message_body_column'
KEY_SUBJECT_COLUMN = 'subject_column'
KEY_ATTACHMENTS_COLUMN = 'attachments_column'

KEY_SUBJECT_TEMPLATE = 'subject_template'
KEY_HTML_TEMPLATE_FILENAME = 'html_template_filename'
KEY_PLAINTEXT_TEMPLATE_FILENAME = 'plaintext_template_filename'
KEY_ATTACHMENTS_SOURCE = 'attachments_source'

KEY_DRY_RUN = 'dry_run'

KEY_SUBJECT = 'subject'
KEY_SUBJECT_SOURCE = 'subject_source'
KEY_RECIPIENT_EMAIL_ADDRESS = 'recipient_email_address'
SLEEP_INTERVAL = 0.1

RESULT_TABLE_COLUMNS = ('status', 'recipient_email_address', 'sender_email_address', 'subject',
                        'plaintext_message_body', 'html_message_body', 'error_message')

VALID_TEMPLATE_MESSAGE = 'All placeholders are present in the input table'

# list of mandatory parameters => if some is missing,
REQUIRED_PARAMETERS = {KEY_SENDER_EMAIL_ADDRESS, KEY_SENDER_PASSWORD, KEY_SERVER_HOST, KEY_SERVER_PORT}

# port 465 for SMTP_SSL
# port 587 for SMTP with TLS


class Component(ComponentBase):
    """Component for sending emails"""

    def __init__(self):
        super().__init__()
        self.cfg = Configuration
        self._client: SMTPClient = None
        self.results_writer = None

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
        plaintext_template_filename = self.configuration.parameters.message_body_config[KEY_PLAINTEXT_TEMPLATE_FILENAME]
        try:
            plaintext_template_path = in_files_paths_by_filename.pop(plaintext_template_filename)
        except KeyError:
            raise UserException(F'{plaintext_template_filename} not in input files')

        html_template_name = self.configuration.parameters.message_body_config.get(KEY_HTML_TEMPLATE_FILENAME)
        html_template_path = in_files_paths_by_filename.pop(html_template_name, None)

        results_table = self.create_out_table_definition('results.csv', write_always=True)
        with open(results_table.full_path, 'w', newline='') as output_file:
            self.results_writer = csv.DictWriter(output_file, fieldnames=RESULT_TABLE_COLUMNS)
            self.results_writer.writeheader()
            self.send_emails(
                in_table_path,
                attachments_paths=in_files_paths_by_filename.values(),
                plaintext_template_path=plaintext_template_path,
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
        use_ssl = self.configuration.parameters.connection_config[KEY_CONNECTION_PROTOCOL] == 'SSL'
        self._client = SMTPClient(
            sender_email_address=self.configuration.parameters.connection_config.get(KEY_SENDER_EMAIL_ADDRESS),
            password=self.configuration.parameters.connection_config.get(KEY_SENDER_PASSWORD),
            server_host=self.configuration.parameters.connection_config.get(KEY_SERVER_HOST),
            server_port=self.configuration.parameters.connection_config.get(KEY_SERVER_PORT),
            use_ssl=use_ssl
        )
        self._client.init_smtp_server()

    def send_emails(self, in_table_path: str, attachments_paths: List[str], plaintext_template_path: str,
                    html_template_path: Union[str, None] = None) -> None:

        with open(in_table_path) as in_table:
            reader = csv.DictReader(in_table)
            # TODO: check validation logic
            subject_column = None
            if self.configuration.parameters.subject_config.get(KEY_SUBJECT_SOURCE) == 'from_table':
                subject_column = self.configuration.parameters.subject_config.get(KEY_SUBJECT_COLUMN)

            validation_columns = set(reader.fieldnames) - {KEY_RECIPIENT_EMAIL_ADDRESS, subject_column}
            plaintext_template_text = self._read_template_text(plaintext_template_path)
            self._validate_template_text(plaintext_template_text, validation_columns)
            recipient_email_address_column = self.configuration.parameters.get(KEY_RECIPIENT_EMAIL_ADDRESS_COLUMN)
            all_attachments = self.configuration.parameters.attachments_config.attachments_source == 'all_input_files'

            if html_template_path is not None:
                html_template_text = self._read_template_text(html_template_path)
                self._validate_template_text(html_template_text, validation_columns)

            if not all_attachments:
                attachments_column = self.configuration.parameters.attachments_config.get(KEY_ATTACHMENTS_COLUMN)

            for row in reader:
                if subject_column is not None:
                    subject_template_text = row[subject_column]
                else:
                    subject_template_text = self.configuration.subject_config.get(KEY_SUBJECT_TEMPLATE)

                rendered_subject = Template(subject_template_text).render(row)

                rendered_plaintext_message = Template(plaintext_template_text).render(row)
                rendered_html_message = None
                if html_template_path is not None:
                    rendered_html_message = Template(html_template_text).render(row)

                custom_attachments_paths = attachments_paths
                if not all_attachments:
                    custom_attachments_paths = [
                        os.path.join(self.files_in_path, attachment_filename)
                        for attachment_filename in json.loads(row[attachments_column])
                    ]

                email_ = self._client.build_email(
                    recipient_email_address=row[recipient_email_address_column],
                    subject=rendered_subject,
                    attachments_paths=custom_attachments_paths,
                    rendered_plaintext_message=rendered_plaintext_message,
                    rendered_html_message=rendered_html_message
                )

                logging.info(f"Sending email with subject: `{email_['Subject']}` to `{email_['To']}`")
                status = 'OK'
                error_message = ''
                if not self.configuration.parameters.get(KEY_DRY_RUN, False):
                    try:
                        self._client.send_email(email_)
                    except Exception as error_message:
                        status = 'ERROR'

                self.results_writer.writerow(dict(
                    status=status,
                    recipient_email_address=email_['To'],
                    sender_email_address=email_['From'],
                    subject=email_['Subject'],
                    plaintext_message_body=rendered_plaintext_message,
                    html_message_body=rendered_html_message,
                    error_message=error_message
                ))
                time.sleep(SLEEP_INTERVAL)

    @staticmethod
    def _read_template_text(template_path: str) -> str:
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
            raise UserException(f"missing columns: {missing_columns}")

    def _get_attachments_filenames_from_input_table(self, in_table_path):
        attachments_filenames = set()
        with open(in_table_path) as in_table:
            reader = csv.DictReader(in_table)
            attachments_column = self.configuration.parameters.attachments_config.get(KEY_ATTACHMENTS_COLUMN)
            for row in reader:
                for attachment_filename in json.loads(row[attachments_column]):
                    attachments_filenames.add(attachment_filename)
        return attachments_filenames

    @sync_action('test_smtp_server_connection')
    def test_smtp_server_connection(self) -> None:
        self.__init_configuration()
        try:
            self.init_client()
            return ValidationResult('OK - Connection established!', MessageType.SUCCESS)
        except Exception as e:
            return ValidationResult(f"ERROR - Could not establish connection! - {e}", MessageType.SUCCESS)

    @sync_action('validate_template')
    def validate_template(self) -> None:
        self.__init_configuration()
        in_tables = self.get_input_tables_definitions()
        in_table_path = in_tables[0].full_path
        with open(in_table_path) as in_table:
            reader = csv.DictReader(in_table)
            columns = set(reader.fieldnames)
        template_text = self.configuration.parameters.template_text
        try:
            self._validate_template_text(template_text, columns)
            return ValidationResult(VALID_TEMPLATE_MESSAGE, MessageType.SUCCESS)
        except UserException as e:
            return ValidationResult(e, MessageType.SUCCESS)

    @sync_action("validate_config")
    def validate_config(self):
        self.__init_configuration()
        messages = []
        try:
            self.init_client()
        except Exception as e:
            messages.append(f"ERROR - Could not establish connection! - {e}")

        validation_result = self.validate_template().message
        if validation_result != VALID_TEMPLATE_MESSAGE:
            messages.append(validation_result)

        if self.configuration.parameters.attaments_config[KEY_ATTACHMENTS_SOURCE] == 'from_table':
            in_files = self.get_input_files_definitions()
            input_filenames = {file.name for file in in_files}
            in_tables = self.get_input_tables_definitions()
            in_table_path = in_tables[0].full_path
            expected_input_filenames = self._get_attachments_filenames_from_input_table(in_table_path)
            missing_input_filenames = expected_input_filenames - set(input_filenames)
            if missing_input_filenames:
                messages.append(f'Missing attachment files: {", ".join(missing_input_filenames)}')

        if messages:
            error_message = 'Config Invalid!\n' + '\n'.join(messages)
            return ValidationResult(error_message, MessageType.SUCCESS)

        return ValidationResult('Config Valid!', MessageType.SUCCESS)


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
