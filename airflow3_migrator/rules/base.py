"""
Base data models and abstract class for Airflow migration rules.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional


class IssueSeverity(str, Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


class IssueCategory(str, Enum):
    IMPORT = "import"
    OPERATOR = "operator"
    PARAMETER = "parameter"
    CONTEXT = "context"
    DATASET = "dataset"
    DATABASE = "database"
    CONFIG = "config"
    DEPENDENCY = "dependency"


@dataclass
class MigrationIssue:
    rule_id: str
    category: IssueCategory
    severity: IssueSeverity
    title: str
    description: str
    line_number: int
    column: int = 0
    end_line_number: Optional[int] = None
    end_column: Optional[int] = None
    file_path: str = ""
    auto_fixable: bool = True
    original_code: str = ""
    suggested_code: str = ""
    documentation_url: str = ""

    def to_dict(self) -> dict:
        return {
            "rule_id": self.rule_id,
            "category": self.category.value,
            "severity": self.severity.value,
            "title": self.title,
            "description": self.description,
            "line_number": self.line_number,
            "column": self.column,
            "end_line_number": self.end_line_number,
            "end_column": self.end_column,
            "file_path": self.file_path,
            "auto_fixable": self.auto_fixable,
            "original_code": self.original_code,
            "suggested_code": self.suggested_code,
            "documentation_url": self.documentation_url,
        }


@dataclass
class RuleResult:
    issues: List[MigrationIssue] = field(default_factory=list)
    new_content: Optional[str] = None
    applied_fixes: int = 0


class BaseRule(ABC):
    """Abstract base class for all Airflow migration rules."""

    rule_id: str = "BASE"
    category: IssueCategory = IssueCategory.IMPORT
    title: str = "Base Migration Rule"
    description: str = "Base rule description"
    documentation_url: str = "https://airflow.apache.org/docs/apache-airflow/stable/upgrading-to-airflow-3.html"

    @abstractmethod
    def analyze(self, content: str, file_path: str) -> List[MigrationIssue]:
        """
        Analyzes file content and returns detected issues.
        Does not mutate the file.
        """
        pass

    @abstractmethod
    def fix(self, content: str, file_path: str) -> tuple[str, List[MigrationIssue]]:
        """
        Analyzes and applies fixes to file content.
        Returns (new_content, applied_issues).
        """
        pass
