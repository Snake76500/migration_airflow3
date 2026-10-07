"""
Migration rules registry.
"""

from typing import List
from .base import BaseRule, MigrationIssue, IssueSeverity, IssueCategory
from .imports import ImportsMigrationRule
from .operators import OperatorsMigrationRule
from .dag_params import DagParamsMigrationRule
from .context_vars import ContextVarsMigrationRule
from .datasets import DatasetToAssetMigrationRule
from .db_access import DatabaseAccessMigrationRule
from .config_rules import ConfigMigrationRule
from .dependencies import DependenciesMigrationRule
from .product_action import ProductActionMigrationRule


def get_all_rules() -> List[BaseRule]:
    """Returns instantiated list of all available migration rules."""
    return [
        ImportsMigrationRule(),
        OperatorsMigrationRule(),
        DagParamsMigrationRule(),
        ContextVarsMigrationRule(),
        DatasetToAssetMigrationRule(),
        DatabaseAccessMigrationRule(),
        ConfigMigrationRule(),
        DependenciesMigrationRule(),
        ProductActionMigrationRule(),
    ]


__all__ = [
    "BaseRule",
    "MigrationIssue",
    "IssueSeverity",
    "IssueCategory",
    "ImportsMigrationRule",
    "OperatorsMigrationRule",
    "DagParamsMigrationRule",
    "ContextVarsMigrationRule",
    "DatasetToAssetMigrationRule",
    "DatabaseAccessMigrationRule",
    "ConfigMigrationRule",
    "DependenciesMigrationRule",
    "ProductActionMigrationRule",
    "get_all_rules",
]
