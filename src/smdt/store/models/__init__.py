"""
This module contains the data models for the SMDT store.
"""

try:
    from enum import StrEnum  # Python 3.11+
except ImportError:
    from enum import Enum

    class StrEnum(str, Enum):  # Fallback for Python <3.11
        pass


from .communities import Communities, CommunityType
from .accounts import Accounts
from .account_enrichments import AccountEnrichments
from .actions import Actions, ActionType
from .posts import Posts
from .entities import Entities, EntityType
from .post_enrichments import PostEnrichments
from .hashmap import HashMap


class ModelNames(StrEnum):
    Communities = "Communities"
    Accounts = "Accounts"
    Actions = "Actions"
    Posts = "Posts"
    Entities = "Entities"
    AccountEnrichments = "AccountEnrichments"
    PostEnrichments = "PostEnrichments"
    HashMap = "HashMap"


MODEL_REGISTRY = {
    AccountEnrichments: {
        "table": "account_enrichments",
        "jsonb_fields": {"body"},
    },
    PostEnrichments: {
        "table": "post_enrichments",
        "jsonb_fields": {"body"},
    },
    Communities: {
        "table": "communities",
        "jsonb_fields": set(),
    },
    Accounts: {
        "table": "accounts",
        "jsonb_fields": set(),
    },
    Posts: {
        "table": "posts",
        "jsonb_fields": set(),
    },
    Actions: {
        "table": "actions",
        "jsonb_fields": set(),
    },
    Entities: {
        "table": "entities",
        "jsonb_fields": {"body"},
    },
    HashMap: {
        "table": "hashmap",
        "jsonb_fields": set(),
    },
}


__all__ = [
    "Communities",
    "CommunityType",
    "Accounts",
    "Actions",
    "Posts",
    "Entities",
    "AccountEnrichments",
    "PostEnrichments",
    "HashMap",
    "ActionType",
    "EntityType",
    "ModelNames",
    "MODEL_REGISTRY",
]
