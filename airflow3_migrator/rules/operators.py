"""
Rule for migrating operator usages and deprecated operator classes in Airflow 3.
"""

import ast
import re
from typing import List, Tuple
from .base import BaseRule, IssueCategory, IssueSeverity, MigrationIssue


class OperatorsMigrationRule(BaseRule):
    rule_id = "AIR303"
    category = IssueCategory.OPERATOR
    title = "Mise à niveau des opérateurs et suppression des arguments obsolètes"
    description = (
        "DummyOperator est remplacé par EmptyOperator. L'argument 'provide_context=True' "
        "dans PythonOperator est obsolète et supprimé dans Airflow 3."
    )
    documentation_url = "https://airflow.apache.org/docs/apache-airflow/stable/upgrading-to-airflow-3.html"

    def analyze(self, content: str, file_path: str) -> List[MigrationIssue]:
        issues: List[MigrationIssue] = []
        try:
            tree = ast.parse(content)
        except SyntaxError:
            return issues

        lines = content.splitlines()

        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func_name = ""
                if isinstance(node.func, ast.Name):
                    func_name = node.func.id
                elif isinstance(node.func, ast.Attribute):
                    func_name = node.func.attr

                # 1. DummyOperator call
                if func_name == "DummyOperator":
                    line_idx = node.lineno - 1
                    line_content = lines[line_idx] if line_idx < len(lines) else ""
                    # replace DummyOperator with EmptyOperator on that line
                    suggested = re.sub(r"\bDummyOperator\b", "EmptyOperator", line_content)
                    issues.append(
                        MigrationIssue(
                            rule_id="AIR301_DUMMY",
                            category=self.category,
                            severity=IssueSeverity.WARNING,
                            title="Remplacement de DummyOperator par EmptyOperator",
                            description=(
                                "DummyOperator a été supprimé dans Airflow 3. "
                                "Utilisez EmptyOperator de 'airflow.providers.standard.operators.empty'."
                            ),
                            line_number=node.lineno,
                            column=node.col_offset,
                            file_path=file_path,
                            auto_fixable=True,
                            original_code=line_content,
                            suggested_code=suggested,
                            documentation_url=self.documentation_url,
                        )
                    )

                # 2. provide_context=True in PythonOperator / BranchPythonOperator
                if func_name in ("PythonOperator", "BranchPythonOperator", "ShortCircuitOperator"):
                    for kw in node.keywords:
                        if kw.arg == "provide_context":
                            kw_line_idx = kw.lineno - 1
                            kw_line = lines[kw_line_idx] if kw_line_idx < len(lines) else ""
                            # Suggest removing provide_context
                            suggested = re.sub(r"\bprovide_context\s*=\s*(True|False)\s*,?\s*", "", kw_line)
                            # Clean up trailing whitespace or comma if needed
                            issues.append(
                                MigrationIssue(
                                    rule_id="AIR301_PROVIDE_CONTEXT",
                                    category=self.category,
                                    severity=IssueSeverity.WARNING,
                                    title="Suppression du paramètre obsolète 'provide_context'",
                                    description=(
                                        "L'argument 'provide_context' a été supprimé. Dans Airflow 2 et 3, "
                                        "les variables de contexte sont injectées automatiquement dans les callables."
                                    ),
                                    line_number=kw.lineno,
                                    column=kw.col_offset,
                                    file_path=file_path,
                                    auto_fixable=True,
                                    original_code=kw_line,
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

        # Sort in reverse line order
        fixable = sorted(
            [i for i in issues if i.auto_fixable],
            key=lambda x: (x.line_number, x.column),
            reverse=True,
        )

        for issue in fixable:
            idx = issue.line_number - 1
            if idx < len(lines):
                has_nl = lines[idx].endswith("\n")
                if issue.rule_id == "AIR301_DUMMY":
                    lines[idx] = re.sub(r"\bDummyOperator\b", "EmptyOperator", lines[idx])
                    applied.append(issue)
                elif issue.rule_id == "AIR301_PROVIDE_CONTEXT":
                    raw_line = lines[idx].rstrip("\r\n")
                    if re.match(r"^\s*provide_context\s*=\s*(True|False),?\s*$", raw_line):
                        # Line only contains provide_context, delete line entirely
                        lines[idx] = ""
                    else:
                        cleaned = re.sub(r",\s*provide_context\s*=\s*(True|False)", "", raw_line)
                        cleaned = re.sub(r"provide_context\s*=\s*(True|False)\s*,\s*", "", cleaned)
                        cleaned = re.sub(r"provide_context\s*=\s*(True|False)", "", cleaned)
                        lines[idx] = cleaned + ("\n" if has_nl else "")
                    applied.append(issue)

        return "".join(lines), applied
