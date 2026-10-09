"""
Rule for migrating DAG definition parameters (schedule_interval -> schedule, sla deprecation).
Supports dual-compatibility mode (Airflow 2 & 3).
"""

import ast
import re
from typing import List, Tuple
from .base import BaseRule, IssueCategory, IssueSeverity, MigrationIssue


class DagParamsMigrationRule(BaseRule):
    rule_id = "AIR304"
    category = IssueCategory.PARAMETER
    title = "Remplacement de schedule_interval par schedule"
    description = (
        "Le paramètre 'schedule_interval' a été supprimé dans Airflow 3 au profit de 'schedule'. "
        "Les définitions de DAGs (via DAG() ou @dag()) doivent utiliser 'schedule', "
        "ou le pattern bi-compatible 'if AIRFLOW_V_3_0_PLUS'."
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
            is_dag_call = False
            # Check DAG(...) or with DAG(...)
            if isinstance(node, ast.Call):
                func_name = ""
                if isinstance(node.func, ast.Name):
                    func_name = node.func.id
                elif isinstance(node.func, ast.Attribute):
                    func_name = node.func.attr

                if func_name in ("DAG", "dag"):
                    is_dag_call = True

                # Also check decorator @dag(...)
                if is_dag_call:
                    for kw in node.keywords:
                        if kw.arg == "schedule_interval":
                            kw_line_idx = kw.lineno - 1
                            kw_line = lines[kw_line_idx] if kw_line_idx < len(lines) else ""

                            if self.dual_compat:
                                suggested = re.sub(
                                    r'\bschedule_interval\s*=\s*((\"[^\"]+\")|(\'[^\']+\'))',
                                    r'**({"schedule": \1} if AIRFLOW_V_3_0_PLUS else {"schedule_interval": \1})',
                                    kw_line,
                                )
                                if suggested == kw_line:
                                    suggested = re.sub(
                                        r'\bschedule_interval\s*=\s*([^,\)\n]+)',
                                        r'**({"schedule": \1} if AIRFLOW_V_3_0_PLUS else {"schedule_interval": \1})',
                                        kw_line,
                                    )
                                title = "Paramétrage bi-compatible de schedule (Airflow 2 & 3)"
                                desc = "Utilisation de **({... if AIRFLOW_V_3_0_PLUS else ...}) pour supporter Airflow 2 et 3 simultanément."
                            else:
                                suggested = re.sub(r"\bschedule_interval\s*=", "schedule=", kw_line)
                                title = "Remplacement de 'schedule_interval' par 'schedule'"
                                desc = "Le paramètre 'schedule_interval' a été définitivement supprimé dans Airflow 3. Utilisez 'schedule' à la place."

                            issues.append(
                                MigrationIssue(
                                    rule_id=self.rule_id,
                                    category=self.category,
                                    severity=IssueSeverity.WARNING,
                                    title=title,
                                    description=desc,
                                    line_number=kw.lineno,
                                    column=kw.col_offset,
                                    file_path=file_path,
                                    auto_fixable=True,
                                    original_code=kw_line,
                                    suggested_code=suggested,
                                    documentation_url=self.documentation_url,
                                )
                            )
                        elif kw.arg == "sla":
                            kw_line_idx = kw.lineno - 1
                            kw_line = lines[kw_line_idx] if kw_line_idx < len(lines) else ""
                            issues.append(
                                MigrationIssue(
                                    rule_id="AIR304_SLA",
                                    category=self.category,
                                    severity=IssueSeverity.INFO,
                                    title="Dépréciation du paramètre SLA",
                                    description=(
                                        "Le mécanisme SLA historique d'Airflow est obsolète dans Airflow 3. "
                                        "Il est recommandé de basculer vers execution_timeout ou des alertes basées sur des métriques."
                                    ),
                                    line_number=kw.lineno,
                                    column=kw.col_offset,
                                    file_path=file_path,
                                    auto_fixable=False,
                                    original_code=kw_line,
                                    suggested_code=kw_line,
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

        fixable = sorted(
            [i for i in issues if i.auto_fixable and i.rule_id == self.rule_id],
            key=lambda x: (x.line_number, x.column),
            reverse=True,
        )

        for issue in fixable:
            idx = issue.line_number - 1
            if idx < len(lines):
                has_nl = lines[idx].endswith("\n")
                lines[idx] = issue.suggested_code + ("\n" if has_nl else "")
                applied.append(issue)

        result = "".join(lines)
        if self.dual_compat and applied:
            result = self.ensure_airflow_v3_compat_import(result)

        return result, applied

