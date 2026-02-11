"""msgvault_sdk - Python SDK for msgvault email archives."""

from msgvault_sdk.errors import (
    ChangeLogError,
    MsgvaultError,
    QueryError,
    VaultNotFoundError,
    VaultReadOnlyError,
)
from msgvault_sdk.models import (
    Account,
    Attachment,
    Conversation,
    Label,
    Message,
    Participant,
)
from msgvault_sdk.query import GroupedQuery, MessageQuery
from msgvault_sdk.vault import Vault

__all__ = [
    "Account",
    "Attachment",
    "ChangeLogError",
    "Conversation",
    "GroupedQuery",
    "Label",
    "Message",
    "MessageQuery",
    "MsgvaultError",
    "Participant",
    "QueryError",
    "Vault",
    "VaultNotFoundError",
    "VaultReadOnlyError",
]
