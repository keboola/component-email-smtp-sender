import logging
from typing import Union, Dict, List
import os
import re
import json

from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders

import smtplib
import socket
import socks
from keboola.component import UserException
from O365 import Account, EnvTokenBackend
import msal


class SMTPClient:
    """
    CLient for sending emails
    """
    def __init__(self, sender_email_address: str, password: str, server_host: str, server_port: int,
                 proxy_server_host: Union[str, None] = None, proxy_server_port: Union[int, None] = None,
                 proxy_server_username: Union[str, None] = None, proxy_server_password: Union[str, None] = None,
                 connection_protocol: str = 'SSL', use_oauth: bool = False, tenant_id: Union[str, None] = None,
                 client_id: Union[str, None] = None, client_secret: Union[str, None] = None,
                 address_whitelist: List[str] = None, disable_attachments: bool = False) -> None:

        self.sender_email_address = sender_email_address
        self.password = password
        self.server_host = server_host
        self.server_port = server_port
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret

        # Customizations
        self.address_whitelist = address_whitelist
        self.disable_attachments = disable_attachments

        if proxy_server_host:
            socks.setdefaultproxy(proxy_type=socks.PROXY_TYPE_SOCKS5, addr=proxy_server_host, port=proxy_server_port,
                                  username=proxy_server_username, password=proxy_server_password)
            socket.socket = socks.socksocket
            socks.wrapmodule(smtplib)

        if use_oauth:
            logging.info('Using O365 SMTP server')
            self.init_smtp_server = self._init_o365_smtp_server
            self.send_email = self.send_email_via_o365_oauth
        elif connection_protocol == 'SSL':
            logging.info('Using SSL SMTP server')
            self.init_smtp_server = self._init_ssl_smtp_server
            self.send_email = self._send_email_via_ssl_server
        elif connection_protocol == 'TLS':
            logging.info('Using TLS SMTP server')
            self.init_smtp_server = self._init_tls_smtp_server
            self.send_email = self._send_email_via_tls_server
        else:
            raise UserException(f'Invalid connection protocol: {connection_protocol}')

    def build_email(self, *, recipient_email_address: str, subject: str, rendered_plaintext_message: str,
                    rendered_html_message: Union[str, None] = None,
                    attachments_paths_by_filename: Dict[str, str] = None) -> MIMEMultipart:
        """
        Prepares email message including html version (if selected) and adds attachments (if they exist)
        """
        if self.address_whitelist:
            self.check_email_mask(recipient_email_address)

        email_ = MIMEMultipart('mixed')
        email_['From'] = self.sender_email_address
        email_['To'] = recipient_email_address
        email_['Subject'] = subject

        email_message = MIMEMultipart('alternative')
        email_message.attach(MIMEText(rendered_plaintext_message, 'plain'))
        if rendered_html_message is not None:
            email_message.attach(MIMEText(rendered_html_message, 'html'))

        email_.attach(email_message)

        if attachments_paths_by_filename and not self.disable_attachments:
            for attachment_filename, attachment_path in attachments_paths_by_filename.items():
                with open(attachment_path, 'rb') as file:
                    attachment = MIMEBase('application', 'octet-stream')
                    attachment.set_payload(file.read())
                    encoders.encode_base64(attachment)
                    attachment.add_header(
                        'Content-Disposition', f'attachment; filename={attachment_filename}')
                    email_.attach(attachment)
        return email_

    def _init_tls_smtp_server(self) -> None:
        server = smtplib.SMTP(self.server_host, self.server_port)
        server.starttls()
        server.login(self.sender_email_address, self.password)
        self.smtp_server = server

    def _send_email_via_tls_server(self, email: MIMEMultipart, **kwargs) -> None:
        self.smtp_server.send_message(email)

    def _init_ssl_smtp_server(self) -> None:
        server = smtplib.SMTP_SSL(host=self.server_host, port=self.server_port)
        server.login(self.sender_email_address, self.password)
        self.smtp_server = server

    def _send_email_via_ssl_server(self, email: MIMEMultipart, **kwargs) -> None:
        self.smtp_server.send_message(email)

    def _init_o365_smtp_server(self) -> None:
        def get_access_token() -> Dict[str, Union[str, int]]:
            authority = f"https://login.microsoftonline.com/{self.tenant_id}"
            app = msal.ConfidentialClientApplication(self.client_id, authority=authority,
                                                     client_credential=self.client_secret)
            result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
            if "access_token" in result:
                return result
            raise Exception(f"Failed to acquire token: {result.get('error')}")

        access_token_result = get_access_token()
        os.environ['O365TOKEN'] = json.dumps(access_token_result)
        account = Account(credentials=(self.client_id, self.client_secret), auth_flow_type='credentials',
                          tenant_id=self.tenant_id, token_backend=EnvTokenBackend())
        account.authenticate()
        self.smtp_server = account

    def send_email_via_o365_oauth(self, email: MIMEMultipart, message_body: str,
                                  attachments_paths: List[str],
                                  html_message_body: Union[str, None] = None, **kwargs) -> None:
        email_ = self.smtp_server.new_message(resource=self.sender_email_address)
        email_.to.add(email['To'])
        email_.subject = email['Subject']
        email_.body = html_message_body if html_message_body is not None else message_body

        if not self.disable_attachments:
            for attachment in attachments_paths:
                email_.attachments.add(attachment)
        email_.send()

    def check_email_mask(self, email: str) -> None:
        """
        Checks whether the provided email or a comma-separated list of emails matches any of the
        patterns (masks) in the address whitelist. The masks may contain '*' as a wildcard character,
        which is translated into a regex pattern to match zero or more characters.

        Args:
            email (str): The email address or a comma-separated list of email addresses
                         to be checked against the whitelist.

        Raises:
            UserException: If any of the emails do not match any of the masks in the whitelist.

        """
        emails = [e.strip() for e in email.split(",")]

        for email in emails:
            matched = False
            for mask in self.address_whitelist:
                _mask = re.escape(mask).replace(r'\*', '.*')

                pattern = rf"^{_mask}$"

                if re.match(pattern, email):
                    matched = True
                    break

            if not matched:
                raise UserException(f"Email '{email}' does not match any of the allowed masks.")
