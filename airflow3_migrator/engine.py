"""
Migration Engine: Coordinates analysis, rule execution, diff generation, and modifications.
"""

import difflib
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Dict, Optional, Tuple

from .rules.base import BaseRule, MigrationIssue, IssueSeverity
from .rules import get_all_rules
from .scanner import ProjectScanner, FileType
from .backup import BackupManager


@dataclass
class FileMigrationPlan:
    file_path: str
    relative_path: str
    file_type: str
    original_content: str
    migrated_content: str
    issues: List[MigrationIssue] = field(default_factory=list)
    applied_issues: List[MigrationIssue] = field(default_factory=list)
    diff: str = ""

    @property
    def has_changes(self) -> bool:
        return self.original_content != self.migrated_content

    @property
    def auto_fixable_count(self) -> int:
        return sum(1 for i in self.issues if i.auto_fixable)

    @property
    def manual_count(self) -> int:
        return sum(1 for i in self.issues if not i.auto_fixable)


@dataclass
class MigrationSummary:
    total_files_scanned: int = 0
    files_with_issues: int = 0
    files_modified: int = 0
    total_issues: int = 0
    critical_issues: int = 0
    warning_issues: int = 0
    info_issues: int = 0
    auto_fixable_issues: int = 0
    manual_issues: int = 0
    plans: List[FileMigrationPlan] = field(default_factory=list)
    backup_path: Optional[str] = None


class MigrationEngine:
    """Orchestrates scanning, detection, and application of Airflow 3 migrations."""

    def __init__(self, root_path: str, rules: Optional[List[BaseRule]] = None, dual_compat: bool = True):
        self.root_path = Path(root_path).resolve()
        self.dual_compat = dual_compat
        self.rules = rules or get_all_rules(dual_compat=dual_compat)
        self.scanner = ProjectScanner(str(self.root_path))
        self.backup_mgr = BackupManager(str(self.root_path))


    def analyze(self) -> MigrationSummary:
        """
        Performs a full scan and analysis without modifying any files.
        Returns a comprehensive MigrationSummary.
        """
        scanned_files = self.scanner.scan()
        summary = MigrationSummary(total_files_scanned=len(scanned_files))

        for item in scanned_files:
            file_path = item["path"]
            rel_path = item["relative_path"]
            file_type = item["type"].value

            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    content = f.read()
            except UnicodeDecodeError:
                try:
                    with open(file_path, "r", encoding="latin-1") as f:
                        content = f.read()
                except Exception:
                    continue
            except Exception:
                continue

            file_issues: List[MigrationIssue] = []
            current_content = content
            applied_issues: List[MigrationIssue] = []

            # Execute rules sequentially to simulate progressive fixes
            for rule in self.rules:
                issues = rule.analyze(current_content, file_path)
                file_issues.extend(issues)

                # Simulate fix to compute proposed changes
                new_content, applied = rule.fix(current_content, file_path)
                if new_content != current_content:
                    current_content = new_content
                    applied_issues.extend(applied)

            if file_issues:
                # Deduplicate issues by (rule_id, line_number, original_code)
                unique_issues: List[MigrationIssue] = []
                seen = set()
                for issue in file_issues:
                    key = (issue.rule_id, issue.line_number, issue.title)
                    if key not in seen:
                        seen.add(key)
                        unique_issues.append(issue)

                # Compute unified diff
                diff = ""
                if current_content != content:
                    diff_lines = list(difflib.unified_diff(
                        content.splitlines(keepends=True),
                        current_content.splitlines(keepends=True),
                        fromfile=f"a/{rel_path} (Airflow 2)",
                        tofile=f"b/{rel_path} (Airflow 3)",
                        lineterm="",
                    ))
                    diff = "\n".join(diff_lines)

                plan = FileMigrationPlan(
                    file_path=file_path,
                    relative_path=rel_path,
                    file_type=file_type,
                    original_content=content,
                    migrated_content=current_content,
                    issues=unique_issues,
                    applied_issues=applied_issues,
                    diff=diff,
                )
                summary.plans.append(plan)
                summary.files_with_issues += 1
                if plan.has_changes:
                    summary.files_modified += 1

                for iss in unique_issues:
                    summary.total_issues += 1
                    if iss.severity == IssueSeverity.CRITICAL:
                        summary.critical_issues += 1
                    elif iss.severity == IssueSeverity.WARNING:
                        summary.warning_issues += 1
                    else:
                        summary.info_issues += 1

                    if iss.auto_fixable:
                        summary.auto_fixable_issues += 1
                    else:
                        summary.manual_issues += 1

        return summary

    def apply(self, create_backup: bool = True) -> MigrationSummary:
        """
        Executes analysis and writes modified files to disk.
        Automatically creates a snapshot backup if requested.
        """
        summary = self.analyze()
        files_to_modify = [p for p in summary.plans if p.has_changes]

        if not files_to_modify:
            return summary

        if create_backup:
            file_paths = [p.file_path for p in files_to_modify]
            backup_path = self.backup_mgr.create_backup(file_paths)
            summary.backup_path = str(backup_path)

        for plan in files_to_modify:
            try:
                with open(plan.file_path, "w", encoding="utf-8") as f:
                    f.write(plan.migrated_content)
            except Exception as e:
                print(f"Erreur d'écriture pour {plan.file_path}: {e}")

        return summary

    def rollback(self) -> Optional[int]:
        """Restores the latest backup taken before migration."""
        return self.backup_mgr.restore_latest()
