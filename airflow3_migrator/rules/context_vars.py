"""
Rule for migrating execution_date and other deprecated context variables to Airflow 3 standards.
Supports dual-compatibility mode (Airflow 2 & 3).
"""

import ast
import re
from typing import List, Tuple
from .base import BaseRule, IssueCategory, IssueSeverity, MigrationIssue


# Direct context and Jinja replacements (standard mode)
CONTEXT_REPLACEMENTS = [
    # Jinja expressions
    (
        r"\{\{\s*execution_date\s*\}\}",
        "{{ logical_date }}",
        "Remplacement du template Jinja '{{ execution_date }}' par '{{ logical_date }}'",
    ),
    (
        r"\{\{\s*execution_date\.",
        "{{ logical_date.",
        "Remplacement des attributs de 'execution_date' par 'logical_date' dans Jinja",
    ),
    (
        r"\{\{\s*next_execution_date\s*\}\}",
        "{{ data_interval_end }}",
        "Remplacement de '{{ next_execution_date }}' par '{{ data_interval_end }}'",
    ),
    (
        r"\{\{\s*prev_execution_date\s*\}\}",
        "{{ data_interval_start }}",
        "Remplacement de '{{ prev_execution_date }}' par '{{ data_interval_start }}'",
    ),
    (
        r"\{\{\s*next_ds\s*\}\}",
        "{{ data_interval_end | ds }}",
        "Remplacement de '{{ next_ds }}' par '{{ data_interval_end | ds }}'",
    ),
    (
        r"\{\{\s*prev_ds\s*\}\}",
        "{{ data_interval_start | ds }}",
        "Remplacement de '{{ prev_ds }}' par '{{ data_interval_start | ds }}'",
    ),
    # Python dict lookups
    (
        r"\[(['\"])execution_date\1\]",
        r"[\1logical_date\1]",
        "Remplacement de l'accès context['execution_date'] par context['logical_date']",
    ),
    (
        r"\.get\((['\"])execution_date\1\)",
        r".get(\1logical_date\1)",
        "Remplacement de .get('execution_date') par .get('logical_date')",
    ),
    # ti.execution_date -> ti.logical_date
    (
        r"\b(ti|task_instance)\.execution_date\b",
        r"\1.logical_date",
        "Remplacement de ti.execution_date par ti.logical_date",
    ),
]

# Dual-compat Python replacements
DUAL_COMPAT_REPLACEMENTS = [
    (
        r"(\w+)\[(['\"])execution_date\2\]",
        r"(\1[\2logical_date\2] if AIRFLOW_V_3_0_PLUS else \1[\2execution_date\2])",
        "Accès bi-compatible à execution_date / logical_date (Airflow 2 & 3)",
    ),
    (
        r"(\w+)\.get\((['\"])execution_date\2\)",
        r"(\1.get(\2logical_date\2) if AIRFLOW_V_3_0_PLUS else \1.get(\2execution_date\2))",
        "Accès .get() bi-compatible à execution_date / logical_date (Airflow 2 & 3)",
    ),
    (
        r"\b(ti|task_instance)\.execution_date\b",
        r"(\1.logical_date if AIRFLOW_V_3_0_PLUS else \1.execution_date)",
        "Accès ti.logical_date bi-compatible (Airflow 2 & 3)",
    ),
]


class ContextVarsMigrationRule(BaseRule):
    rule_id = "AIR305"
    category = IssueCategory.CONTEXT
    title = "Remplacement de execution_date par logical_date / data_interval"
    description = (
        "Airflow 3 a complètement supprimé la variable 'execution_date'. "
        "Elle doit être remplacée par 'logical_date', ou gérée de façon bi-compatible "
        "via 'if AIRFLOW_V_3_0_PLUS'."
    )
    documentation_url = "https://airflow.apache.org/docs/apache-airflow/stable/upgrading-to-airflow-3.html"

    def analyze(self, content: str, file_path: str) -> List[MigrationIssue]:
        issues: List[MigrationIssue] = []
        lines = content.splitlines()

        for line_no, line in enumerate(lines, start=1):
            if "AIRFLOW_V_3_0_PLUS" in line and "logical_date" in line:
                # Already dual-compatible on this line
                continue

            if self.dual_compat:
                # First check dual-compat python dict patterns
                matched_dual = False
                for pattern, replacement, desc in DUAL_COMPAT_REPLACEMENTS:
                    if re.search(pattern, line):
                        suggested = re.sub(pattern, replacement, line)
                        issues.append(
                            MigrationIssue(
                                rule_id=self.rule_id,
                                category=self.category,
                                severity=IssueSeverity.WARNING,
                                title=desc,
                                description=f"{desc}. Compatible Airflow 2 et Airflow 3.",
                                line_number=line_no,
                                column=0,
                                file_path=file_path,
                                auto_fixable=True,
                                original_code=line,
                                suggested_code=suggested,
                                documentation_url=self.documentation_url,
                            )
                        )
                        matched_dual = True

                # If not matched dual python, check Jinja
                if not matched_dual:
                    for pattern, replacement, desc in CONTEXT_REPLACEMENTS[:6]:
                        if re.search(pattern, line):
                            suggested = re.sub(pattern, replacement, line)
                            issues.append(
                                MigrationIssue(
                                    rule_id=self.rule_id,
                                    category=self.category,
                                    severity=IssueSeverity.WARNING,
                                    title=desc,
                                    description=f"{desc}. Compatible Airflow 2.2+ et Airflow 3.",
                                    line_number=line_no,
                                    column=0,
                                    file_path=file_path,
                                    auto_fixable=True,
                                    original_code=line,
                                    suggested_code=suggested,
                                    documentation_url=self.documentation_url,
                                )
                            )
            else:
                for pattern, replacement, desc in CONTEXT_REPLACEMENTS:
                    if re.search(pattern, line):
                        suggested = re.sub(pattern, replacement, line)
                        issues.append(
                            MigrationIssue(
                                rule_id=self.rule_id,
                                category=self.category,
                                severity=IssueSeverity.WARNING,
                                title=desc,
                                description=(
                                    f"{desc}. 'execution_date' provoque une erreur dans Airflow 3."
                                ),
                                line_number=line_no,
                                column=0,
                                file_path=file_path,
                                auto_fixable=True,
                                original_code=line,
                                suggested_code=suggested,
                                documentation_url=self.documentation_url,
                            )
                        )

        return issues

    def fix(self, content: str, file_path: str) -> Tuple[str, List[MigrationIssue]]:
        issues = self.analyze(content, file_path)
        if not issues:
            return content, []

        lines = content.splitlines(keepends=True)
        applied: List[MigrationIssue] = []

        line_fixes = {}
        for issue in issues:
            if issue.auto_fixable:
                line_fixes.setdefault(issue.line_number, []).append(issue)

        for line_no in sorted(line_fixes.keys(), reverse=True):
            idx = line_no - 1
            if idx < len(lines):
                line_content = lines[idx]
                has_nl = line_content.endswith("\n")
                raw_line = line_content.rstrip("\r\n")

                if self.dual_compat:
                    for pattern, replacement, _ in DUAL_COMPAT_REPLACEMENTS:
                        raw_line = re.sub(pattern, replacement, raw_line)
                    for pattern, replacement, _ in CONTEXT_REPLACEMENTS[:6]:
                        raw_line = re.sub(pattern, replacement, raw_line)
                else:
                    for pattern, replacement, _ in CONTEXT_REPLACEMENTS:
                        raw_line = re.sub(pattern, replacement, raw_line)

                lines[idx] = raw_line + ("\n" if has_nl else "")
                applied.extend(line_fixes[line_no])

        result = "".join(lines)
        if self.dual_compat and applied:
            result = self.ensure_airflow_v3_compat_import(result)

        return result, applied

