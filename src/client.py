import logging
import os
from typing import List, Union

from email.message import EmailMessage
from email.mime.text import MIMEText

import smtplib
from frozendict import frozendict


EXTENSION_TO_ATTACHMENT_TYPES = frozendict({
    'txt': ('text', 'plain'),
    'json': ('text', 'json'),
    'csv': ('text', 'csv'),
    'xlsx': ('text', 'xlsx'),
    'xls': ('text', 'xls'),
    'jpg': ('image', 'jpeg'),
    'jpeg': ('image', 'jpeg'),
    'png': ('image', 'png'),
    'pdf': ('application', 'pdf')
})
ALLOWED_EXTENSIONS = set(EXTENSION_TO_ATTACHMENT_TYPES.keys())
# TODO: handle attachment extension validation


class SMTPClient:
    """
    CLient for sending emails
    """
    def __init__(self, sender_email_address, password, server_host, server_port, use_ssl=False):
        self.sender_email_address = sender_email_address
        self.password = password
        self.server_host = server_host
        self.server_port = server_port
        if use_ssl:
            logging.info('Using SSL SMTP server')
            self.init_smtp_server = self._init_ssl_smtp_server
            self.send_email = self._send_email_via_ssl_server
        else:
            logging.info('Using TLS SMTP server')
            self.init_smtp_server = self._init_tls_smtp_server
            self.send_email = self._send_email_via_tls_server

    def build_email(self, *, recipient_email_address: str, subject: str, rendered_plaintext_message: str,
                    rendered_html_message: Union[str, None] = None,
                    attachments_paths: List[str] = None) -> EmailMessage:
        """
        Prepares email message including html version (if selected) and adds attachments (if they exist)
        """
        email_ = EmailMessage()
        email_['From'] = self.sender_email_address
        email_['To'] = recipient_email_address
        email_['Subject'] = subject
        email_.set_content(MIMEText(rendered_plaintext_message, 'plain'))

        if rendered_html_message is not None:
            email_.add_alternative(rendered_html_message, subtype='html')

        if attachments_paths is not None:
            for attachment_path in attachments_paths:
                with open(attachment_path, 'rb') as file:
                    file_data = file.read()
                    file_name = os.path.split(attachment_path)[-1]
                    name, _, extension = attachment_path.rpartition('.')
                    main_type, sub_type = EXTENSION_TO_ATTACHMENT_TYPES[extension]
                    email_.add_attachment(file_data, maintype=main_type, subtype=sub_type, filename=file_name)
        return email_

    def _init_tls_smtp_server(self) -> None:
        server = smtplib.SMTP(self.server_host, self.server_port)
        server.connect()
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(self.sender_email_address, self.password)
        self._smtp_server = server

    def _init_ssl_smtp_server(self) -> None:
        server = smtplib.SMTP_SSL(host=self.server_host, port=self.server_port)
        server.login(self.sender_email_address, self.password)
        self._smtp_server = server

    def _send_email_via_tls_server(self, email: EmailMessage) -> None:
        self._smtp_server.sendmail(self.sender_email_address, email['To'], email)

    def _send_email_via_ssl_server(self, email: EmailMessage) -> None:
        self._smtp_server.send_message(email)
