import csv
import logging
from typing import List, Union
import re
import os
import time

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException
from keboola.component.base import sync_action
from jinja2 import Template

from configuration import Configuration
from client import SMTPClient

# configuration variables
KEY_SENDER_EMAIL_ADDRESS = 'sender_email_address'
KEY_SENDER_PASSWORD = 'sender_password'
KEY_SERVER_HOST = 'server_host'
KEY_SERVER_PORT = 'server_port'
KEY_USE_SSL = 'use_ssl'
KEY_SHARED_ATTACHMENTS = 'shared_attachments'

# input table fields
KEY_SUBJECT = 'subject'
KEY_RECIPIENT_EMAIL_ADDRESS = 'recipient_email_address'
KEY_ATTACHMENTS = 'attachments'
SLEEP_INTERVAL = 0.1


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
        try:
            plaintext_template_path = in_files_paths_by_filename.pop('template.txt')
        except KeyError as e:
            raise UserException('template.txt not in input files')
        html_template_path = in_files_paths_by_filename.pop('template.html', None)

        # TODO: handle save_sent_emails
        self.send_emails(
            in_table_path,
            attachments_paths=in_files_paths_by_filename.values(),
            plaintext_template_path=plaintext_template_path,
            html_template_path=html_template_path,
            shared_attachments=self.configuration.parameters['shared_attachments'])

    def __init_configuration(self):
        try:
            self._validate_parameters(self.configuration.parameters, REQUIRED_PARAMETERS,'Row')
        except UserException as e:
            raise UserException(f"{e} The configuration is invalid. Please check that you added a configuration row.")
        self.cfg: Configuration = Configuration.fromDict(parameters=self.configuration.parameters)

    def init_client(self):
        # TODO: handle connection through proxy
        self._client = SMTPClient(
            sender_email_address=self.configuration.parameters.get(KEY_SENDER_EMAIL_ADDRESS),
            password=self.configuration.parameters.get(KEY_SENDER_PASSWORD),
            server_host=self.configuration.parameters.get(KEY_SERVER_HOST),
            server_port=self.configuration.parameters.get(KEY_SERVER_PORT),
            use_ssl=self.configuration.parameters.get(KEY_USE_SSL)
        )
        self._client.init_smtp_server()

    def send_emails(self, in_table_path: str, attachments_paths: List[str], plaintext_template_path: str,
                    html_template_path: Union[str, None] = None, shared_attachments: bool = True) -> None:

        with open(in_table_path) as in_table:
            reader = csv.DictReader(in_table)
            validation_columns = set(reader.fieldnames) - {KEY_RECIPIENT_EMAIL_ADDRESS, KEY_SUBJECT, KEY_ATTACHMENTS}
            plaintext_template_text = self._read_template_text(plaintext_template_path)
            self._validate_template_text(plaintext_template_text, validation_columns)

            if html_template_path is not None:
                html_template_text = self._read_template_text(html_template_path)
                self._validate_template_text(html_template_text, validation_columns)

            for row in reader:
                rendered_plaintext_message = Template(plaintext_template_text).render(row)
                rendered_html_message = None

                if html_template_path is not None:
                    rendered_html_message = Template(html_template_text).render(row)

                custom_attachments_paths = attachments_paths
                if not shared_attachments:
                    custom_attachments_paths = [
                        os.path.join(self.files_in_path, attachment_filename)
                        for attachment_filename in row['attachments'].split(';')
                    ]

                email_ = self._client.build_email(
                    recipient_email_address=row['recipient_email_address'],
                    subject=row['subject'],
                    attachments_paths=custom_attachments_paths,
                    rendered_plaintext_message=rendered_plaintext_message,
                    rendered_html_message=rendered_html_message
                )
                logging.info(f"Sending email with subject: `{email_['Subject']}` to `{email_['To']}`")
                self._client.send_email(email_)
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
        missing_placeholders = set(columns) - set(template_placeholders)
        missing_columns = set(template_placeholders) - set(columns)
        if missing_placeholders.union(missing_columns):
            raise UserException(f"missing placeholders: {missing_placeholders}, missing columns: {missing_columns}")

    @sync_action('validate_sender_email_address')
    def validate_sender_email_address(self) -> None:
        from email_validator import validate_email
        self.__init_configuration()
        email_address = self.configuration.parameters.sender_email_address
        validate_email(email_address, check_deliverability=False)

    @sync_action('validate_sender_email_deliverability')
    def validate_sender_email_deliverability(self) -> None:
        from email_validator import validate_email
        self.__init_configuration()
        email_address = self.configuration.parameters.sender_email_address
        validate_email(email_address, check_deliverability=True)

    @sync_action('testConnection')
    def test_smtp_server_connection(self) -> None:
        self.__init_configuration()
        self.init_client()

    @sync_action('validate_template')
    def validate_template(self) -> None:
        self.__init_configuration()
        in_tables = self.get_input_tables_definitions()
        in_table_path = in_tables[0].full_path
        with open(in_table_path) as in_table:
            reader = csv.DictReader(in_table)
            columns = set(reader.fieldnames)
        template_text = self.configuration.parameters.template_text
        self._validate_template_text(template_text, columns)


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
