import dataclasses
from dataclasses import dataclass
from typing import List, Union
import json

from pyhocon import ConfigTree
import dataconf


class ConfigurationBase:
    @staticmethod
    def _convert_private_value(value: str):
        return value.replace('"#', '"pswd_')

    @staticmethod
    def _convert_private_value_inv(value: str):
        if value and value.startswith("pswd_"):
            return value.replace("pswd_", "#", 1)
        else:
            return value

    @classmethod
    def load_from_dict(cls, configuration: dict):
        """
        Initialize the configuration dataclass object from dictionary.
        Args:
            configuration: Dictionary loaded from json configuration.

        Returns:

        """
        json_conf = json.dumps(configuration, ensure_ascii=False)
        json_conf = ConfigurationBase._convert_private_value(json_conf)
        return dataconf.loads(json_conf, cls, ignore_unexpected=True)

    @classmethod
    def get_dataclass_required_parameters(cls) -> List[str]:
        """
        Return list of required parameters based on the dataclass definition (no default value)
        Returns: List[str]

        """
        return [cls._convert_private_value_inv(f.name)
                for f in dataclasses.fields(cls)
                if f.default == dataclasses.MISSING
                and f.default_factory == dataclasses.MISSING]

    def __getitem__(self, item, default=None):
        return getattr(self, item, default)

    def get(self, item, default=None):
        return getattr(self, item, default)


@dataclass
class OAuthConfig(ConfigurationBase):
    sender_email_address: Union[str, None] = None
    tenant_id: Union[str, None] = None
    client_id: Union[str, None] = None
    pswd_client_secret: Union[str, None] = None


@dataclass
class ProxyServerConfig(ConfigurationBase):
    proxy_server_host: Union[str, None] = None
    proxy_server_port: Union[int, None] = None
    proxy_server_username: Union[str, None] = None
    pswd_proxy_server_password: Union[str, None] = None


@dataclass
class CredentialsConfig(ConfigurationBase):
    sender_email_address: Union[str, None] = None
    pswd_sender_password: Union[str, None] = None
    server_host: str = 'smtp.gmail.com'
    server_port: int = 465
    connection_protocol: str = 'SSL'
    use_proxy_server: bool = False
    proxy_server_config: ProxyServerConfig = dataclasses.field(default_factory=lambda: ConfigTree({}))
    without_login: bool = False


@dataclass
class ConnectionConfig(ConfigurationBase):
    use_oauth: bool = False
    oauth_config: OAuthConfig = dataclasses.field(default_factory=lambda: ConfigTree({}))
    creds_config: CredentialsConfig = dataclasses.field(default_factory=lambda: ConfigTree({}))


@dataclass
class BasicEmailOptions(ConfigurationBase):
    """
    recipient_email_addresses: comma-delimited list of recipient email addresses
    subject: subject of the email
    message_body: body of the email
    """
    recipient_email_addresses: Union[str, None] = None
    subject: Union[str, None] = None
    message_body: Union[str, None] = None
    include_attachments: Union[bool, None] = None


@dataclass
class SubjectConfig(ConfigurationBase):
    """
    subject_source:
    "from_table" -> "subject_column"
    "from_template_definition" -> "subject_template_definition"
    """
    subject_source: Union[str, None] = None
    subject_column: Union[str, None] = None
    subject_template_definition: Union[str, None] = None


@dataclass
class MessageBodyConfig(ConfigurationBase):
    """
    message_body_source:
    "from_table" -> "plaintext_template_column" + "html_template_column"
    "from_template_file" -> "plaintext_template_filename" + "html_template_filename"
    "from_template_definition" -> "plaintext_template_definition" + "html_template_definition"
    """
    message_body_source: Union[str, None] = None
    use_html_template: bool = False
    plaintext_template_column: Union[str, None] = None
    html_template_column: Union[str, None] = None
    plaintext_template_filename: Union[str, None] = None
    html_template_filename: Union[str, None] = None
    plaintext_template_definition: Union[str, None] = None
    html_template_definition: Union[str, None] = None


@dataclass
class AttachmentsConfig(ConfigurationBase):
    """
    attachments_source:
    "all_input_files"
    "from_table" -> "attachments_column"
    """
    attachments_source: Union[str, None] = None
    attachments_column: Union[str, None] = None


@dataclass
class AdvancedEmailOptions(ConfigurationBase):
    email_data_table_name: Union[str, None] = None
    recipient_email_address_column: Union[str, None] = None
    subject_config: SubjectConfig = dataclasses.field(default_factory=lambda: ConfigTree({}))
    message_body_config: MessageBodyConfig = dataclasses.field(default_factory=lambda: ConfigTree({}))
    include_attachments: bool = True
    attachments_config: AttachmentsConfig = dataclasses.field(default_factory=lambda: ConfigTree({}))


@dataclass
class Configuration(ConfigurationBase):
    connection_config: ConnectionConfig = dataclasses.field(default_factory=lambda: ConfigTree({}))
    configuration_type: Union[str, None] = None
    basic_options: BasicEmailOptions = dataclasses.field(default_factory=lambda: ConfigTree({}))
    advanced_options: AdvancedEmailOptions = dataclasses.field(default_factory=lambda: ConfigTree({}))
    continue_on_error: bool = True
    dry_run: bool = False
