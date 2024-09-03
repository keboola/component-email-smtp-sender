from dataclasses import dataclass, field
from typing import List


@dataclass
class AllowedHosts:
    host: str
    port: int


@dataclass
class StackOverridesParameters:
    allowed_hosts: List[AllowedHosts] = field(default_factory=list)
    address_whitelist: List[str] = field(default_factory=list)
    disable_attachments: bool = False
