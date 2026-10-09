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
from .sqlalchemy_rules import SQLAlchemyMigrationRule


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
        SQLAlchemyMigrationRule(),
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
    "SQLAlchemyMigrationRule",
    "get_all_rules",
]
