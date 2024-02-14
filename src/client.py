import logging
import os
from typing import List, Union

from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders

import smtplib
import socket
import socks


class SMTPClient:
    """
    CLient for sending emails
    """
    def __init__(self, sender_email_address: str, password: str, server_host: str, server_port: int,
                 proxy_server_host: Union[str, None] = None, proxy_server_port: Union[int, None] = None,
                 proxy_server_username: Union[str, None] = None, proxy_server_password: Union[str, None] = None,
                 use_ssl: bool = False):
        self.sender_email_address = sender_email_address
        self.password = password
        self.server_host = server_host
        self.server_port = server_port

        if proxy_server_host:
            socks.setdefaultproxy(proxy_type=socks.PROXY_TYPE_SOCKS5, addr=proxy_server_host, port=proxy_server_port,
                                  username=proxy_server_username, password=proxy_server_password)
            socket.socket = socks.socksocket
            socks.wrapmodule(smtplib)

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
                    attachments_paths: List[str] = None) -> MIMEMultipart:
        """
        Prepares email message including html version (if selected) and adds attachments (if they exist)
        """
        email_ = MIMEMultipart('mixed')
        email_['From'] = self.sender_email_address
        email_['To'] = recipient_email_address
        email_['Subject'] = subject

        email_message = MIMEMultipart('alternative')
        email_message.attach(MIMEText(rendered_plaintext_message, 'plain'))
        if rendered_html_message is not None:
            email_message.attach(MIMEText(rendered_html_message, 'html'))

        email_.attach(email_message)

        if attachments_paths is not None:
            for attachment_path in attachments_paths:
                with open(attachment_path, 'rb') as file:
                    attachment = MIMEBase('application', 'octet-stream')
                    attachment.set_payload(file.read())
                    encoders.encode_base64(attachment)
                    file_name = os.path.split(attachment_path)[-1]
                    attachment.add_header('Content-Disposition', f'attachment; filename={file_name}')
                    email_.attach(attachment)
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

    def _send_email_via_tls_server(self, email: MIMEMultipart) -> None:
        self._smtp_server.sendmail(self.sender_email_address, email['To'], email)

    def _send_email_via_ssl_server(self, email: MIMEMultipart) -> None:
        self._smtp_server.send_message(email)
