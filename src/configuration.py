import dataclasses
from dataclasses import dataclass
from typing import List, Union

import dataconf
from pyhocon import ConfigTree


class ConfigurationBase:
    @staticmethod
    def fromDict(parameters: dict):
        return dataconf.dict(parameters, Configuration, ignore_unexpected=True)
        pass

    @staticmethod
    def _convert_private_value_inv(value: str):
        if value and value.startswith('pswd_'):
            return value.replace('pswd_', '#', 1)
        else:
            return value

    @classmethod
    def get_dataclass_required_parameters(cls) -> List[str]:
        """
        Return list of required parameters based on the dataclass definition (no default value)
        Returns: List[str]

        """
        return [cls._convert_private_value_inv(f.name) for f in dataclasses.fields(cls)
                if f.default == dataclasses.MISSING
                and f.default_factory == dataclasses.MISSING]

    def __getitem__(self, item, default=None):
        return getattr(self, item)

    def get(self, item, default=None):
        return self.__getitem__(item, default)


@dataclass
class ConnectionConfig(ConfigurationBase):
    sender_email_address: str
    sender_password: str
    server_host: str = 'smtp.gmail.com'
    server_port: int = 465
    proxy_server_host: Union[str, None] = None
    proxy_server_port: Union[int, None] = None
    proxy_server_username: Union[str, None] = None
    proxy_server_password: Union[str, None] = None
    connection_protocol: str = 'SSL'


@dataclass
class SubjectConfig(ConfigurationBase):
    """
    subject_source:
    "In Table" -> "subject_column"
    "From Template" -> "subject_template"
    """
    subject_source: str
    subject_column: Union[str, None] = None
    subject_template: Union[str, None] = None


@dataclass
class MessageBodyConfig(ConfigurationBase):
    """
    message_body_source:
    "from_table" -> "plaintext_message_column" + "html_message_column"
    "from_template_file" -> "plaintext_template_filename" + "html_template_filename"
    "from_template_definition" -> "plaintext_template_text" + "html_template_text"
    """
    message_body_source: str = ''
    use_html_template: bool = False
    plaintext_template_column: str = 'plaintext_template_column'
    html_template_column: str = 'html_template_column'
    plaintext_template_filename: str = 'plaintext_template_filename'
    html_template_filename: str = 'html_template_filename'
    plaintext_template_definition: str = 'plaintext_template_definition'
    html_template_definition: str = 'html_template_definition'


@dataclass
class AttachmentsConfig(ConfigurationBase):
    """
    attachments_source:
    "all_input_files"
    "from_table" -> "attachments_column"
    """
    attachments_source: str = 'all_input_files'
    attachments_column: Union[str, None] = None


@dataclass
class Configuration(ConfigurationBase):
    connection_config: ConnectionConfig = dataclasses.field(default_factory=lambda: ConfigTree({}))
    recipient_email_address_column: str = 'recipient_email_address'
    subject_config: SubjectConfig = dataclasses.field(default_factory=lambda: ConfigTree({}))
    message_body_config: MessageBodyConfig = dataclasses.field(default_factory=lambda: ConfigTree({}))
    attachments_config: AttachmentsConfig = dataclasses.field(default_factory=lambda: ConfigTree({}))
    dry_run: bool = False
