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


def get_all_rules(dual_compat: bool = False) -> List[BaseRule]:
    """Returns instantiated list of all available migration rules."""
    return [
        ImportsMigrationRule(dual_compat=dual_compat),
        OperatorsMigrationRule(dual_compat=dual_compat),
        DagParamsMigrationRule(dual_compat=dual_compat),
        ContextVarsMigrationRule(dual_compat=dual_compat),
        DatasetToAssetMigrationRule(dual_compat=dual_compat),
        DatabaseAccessMigrationRule(dual_compat=dual_compat),
        ConfigMigrationRule(dual_compat=dual_compat),
        DependenciesMigrationRule(dual_compat=dual_compat),
        ProductActionMigrationRule(dual_compat=dual_compat),
        SQLAlchemyMigrationRule(dual_compat=dual_compat),
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
