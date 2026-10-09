"""
Rule for migrating Airflow 2 Datasets to Airflow 3 Assets.
"""

import ast
import re
from typing import List, Tuple
from .base import BaseRule, IssueCategory, IssueSeverity, MigrationIssue


class DatasetToAssetMigrationRule(BaseRule):
    rule_id = "AIR306"
    category = IssueCategory.DATASET
    title = "Migration des Datasets vers les Assets"
    description = (
        "Airflow 3 unifie les concepts orientés données sous le nom d'Asset. "
        "L'utilisation de 'Dataset' (airflow.datasets.Dataset) est dépréciée/remplacée "
        "par 'Asset' (depuis 'airflow.sdk' ou 'airflow.datasets')."
    )
    documentation_url = "https://airflow.apache.org/docs/apache-airflow/stable/upgrading-to-airflow-3.html"

    def analyze(self, content: str, file_path: str) -> List[MigrationIssue]:
        issues: List[MigrationIssue] = []
        try:
            tree = ast.parse(content)
        except SyntaxError:
            return issues

        lines = content.splitlines()

        # Check if already using AIRFLOW_V_3_0_PLUS compat block
        has_compat_if = any(
            isinstance(n, ast.If) and "AIRFLOW_V_3_0_PLUS" in ast.unparse(n.test)
            for n in ast.walk(tree)
        )

        for node in ast.walk(tree):
            # Check import from airflow.datasets import Dataset (only if not handled by dual_compat)
            if not self.dual_compat and not has_compat_if and isinstance(node, ast.ImportFrom) and node.module == "airflow.datasets":
                for alias in node.names:
                    if alias.name == "Dataset":
                        line_idx = node.lineno - 1
                        line_content = lines[line_idx] if line_idx < len(lines) else ""
                        indent = re.match(r"^\s*", line_content).group(0)

                        suggested = f"{indent}from airflow.sdk import Asset"
                        issues.append(
                            MigrationIssue(
                                rule_id=self.rule_id,
                                category=self.category,
                                severity=IssueSeverity.WARNING,
                                title="Remplacement de l'import 'Dataset' par 'Asset'",
                                description=(
                                    "Airflow 3 utilise 'Asset' à la place de 'Dataset'. "
                                    "Il est recommandé de l'importer depuis 'airflow.sdk' ou 'airflow.datasets'."
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

            # Check calls to Dataset(...)
            elif isinstance(node, ast.Call):
                func_name = ""
                if isinstance(node.func, ast.Name):
                    func_name = node.func.id
                elif isinstance(node.func, ast.Attribute):
                    func_name = node.func.attr

                if func_name == "Dataset":
                    line_idx = node.lineno - 1
                    line_content = lines[line_idx] if line_idx < len(lines) else ""
                    suggested = re.sub(r"\bDataset\(", "Asset(", line_content)
                    issues.append(
                        MigrationIssue(
                            rule_id=self.rule_id,
                            category=self.category,
                            severity=IssueSeverity.WARNING,
                            title="Remplacement de Dataset(...) par Asset(...)",
                            description=(
                                "Remplacez l'instanciation de 'Dataset' par 'Asset'."
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

        return issues

    def fix(self, content: str, file_path: str) -> Tuple[str, List[MigrationIssue]]:
        issues = self.analyze(content, file_path)
        if not issues:
            return content, []

        lines = content.splitlines(keepends=True)
        applied: List[MigrationIssue] = []

        fixable = sorted(
            [i for i in issues if i.auto_fixable],
            key=lambda x: (x.line_number, x.column),
            reverse=True,
        )

        for issue in fixable:
            idx = issue.line_number - 1
            if idx < len(lines):
                has_nl = lines[idx].endswith("\n")
                if "import" in issue.original_code and "Dataset" in issue.original_code:
                    lines[idx] = issue.suggested_code + ("\n" if has_nl else "")
                    applied.append(issue)
                elif "Dataset(" in lines[idx]:
                    lines[idx] = re.sub(r"\bDataset\(", "Asset(", lines[idx])
                    applied.append(issue)

        return "".join(lines), applied
