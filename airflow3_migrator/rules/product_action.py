"""
Rule for migrating @product_action decorators to dynamic action_id with Airflow 3 compatibility.
"""

import ast
import re
from typing import List, Tuple
from .base import BaseRule, IssueCategory, IssueSeverity, MigrationIssue


class ProductActionMigrationRule(BaseRule):
    rule_id = "AIR310_PRODUCT_ACTION"
    category = IssueCategory.OPERATOR
    title = "Migration du décorateur @product_action avec action_id dynamique"
    description = (
        "Mise à jour de @product_action pour utiliser un action_id dynamique basé sur "
        "Path(__file__).stem, AIRFLOW_V_3_0_PLUS et ENVIRONMENT."
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
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                for dec in node.decorator_list:
                    if isinstance(dec, ast.Call):
                        func_name = getattr(dec.func, "id", "") or getattr(dec.func, "attr", "")
                        if func_name == "product_action":
                            # Check if already migrated
                            already_migrated = False
                            for kw in dec.keywords:
                                if kw.arg == "action_id":
                                    kw_text = ast.unparse(kw.value)
                                    if "Path(__file__).stem" in kw_text or "AIRFLOW_V_3_0_PLUS" in kw_text:
                                        already_migrated = True
                                        break

                            if already_migrated:
                                continue

                            start_idx = dec.lineno - 1
                            end_idx = dec.end_lineno if dec.end_lineno else dec.lineno
                            orig_snippet = "\n".join(lines[start_idx:end_idx])

                            # Determine indentation
                            first_line = lines[start_idx] if start_idx < len(lines) else ""
                            indent = re.match(r"^\s*", first_line).group(0)

                            # Build suggested replacement
                            new_kwargs = []
                            new_kwargs.append(
                                f"{indent}    action_id=(\n"
                                f'{indent}        Path(__file__).stem.replace(".v1.", ".v2")\n'
                                f'{indent}        if AIRFLOW_V_3_0_PLUS and not ENVIRONMENT.endswith("prod")\n'
                                f"{indent}        else Path(__file__).stem\n"
                                f"{indent}    )"
                            )

                            for kw in dec.keywords:
                                if kw.arg != "action_id":
                                    val_str = ast.get_source_segment(content, kw.value) or ast.unparse(kw.value)
                                    new_kwargs.append(f"{indent}    {kw.arg}={val_str}")

                            suggested_block = (
                                f"{indent}@product_action(\n"
                                + ",\n".join(new_kwargs)
                                + f"\n{indent})"
                            )

                            issues.append(
                                MigrationIssue(
                                    rule_id=self.rule_id,
                                    category=self.category,
                                    severity=IssueSeverity.WARNING,
                                    title="Mise à niveau du décorateur @product_action (action_id dynamique)",
                                    description=(
                                        "Le décorateur @product_action doit être mis à niveau pour calculer "
                                        "dynamiquement 'action_id' selon la version d'Airflow et l'environnement."
                                    ),
                                    line_number=dec.lineno,
                                    column=dec.col_offset,
                                    end_line_number=dec.end_lineno,
                                    end_column=dec.end_col_offset,
                                    file_path=file_path,
                                    auto_fixable=True,
                                    original_code=orig_snippet,
                                    suggested_code=suggested_block,
                                    documentation_url=self.documentation_url,
                                )
                            )

        return issues

    def fix(self, content: str, file_path: str) -> Tuple[str, List[MigrationIssue]]:
        issues = self.analyze(content, file_path)
        if not issues:
            return content, []

        try:
            tree = ast.parse(content)
        except SyntaxError:
            return content, []

        lines = content.splitlines(keepends=True)
        applied: List[MigrationIssue] = []

        # Find all target decorators
        decorators_to_fix = []
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                for dec in node.decorator_list:
                    if isinstance(dec, ast.Call):
                        func_name = getattr(dec.func, "id", "") or getattr(dec.func, "attr", "")
                        if func_name == "product_action":
                            # Check if already migrated
                            already_migrated = False
                            for kw in dec.keywords:
                                if kw.arg == "action_id":
                                    kw_text = ast.unparse(kw.value)
                                    if "Path(__file__).stem" in kw_text or "AIRFLOW_V_3_0_PLUS" in kw_text:
                                        already_migrated = True
                                        break
                            if not already_migrated:
                                decorators_to_fix.append(dec)

        if not decorators_to_fix:
            return content, []

        # Sort in reverse line order to preserve line indices
        decorators_to_fix.sort(key=lambda d: d.lineno, reverse=True)

        for dec in decorators_to_fix:
            start_idx = dec.lineno - 1
            end_idx = dec.end_lineno if dec.end_lineno else dec.lineno
            orig_first_line = lines[start_idx]
            indent = re.match(r"^\s*", orig_first_line).group(0)

            new_kwargs = []
            new_kwargs.append(
                f"{indent}    action_id=(\n"
                f'{indent}        Path(__file__).stem.replace(".v1.", ".v2")\n'
                f'{indent}        if AIRFLOW_V_3_0_PLUS and not ENVIRONMENT.endswith("prod")\n'
                f"{indent}        else Path(__file__).stem\n"
                f"{indent}    )"
            )

            for kw in dec.keywords:
                if kw.arg != "action_id":
                    val_str = ast.get_source_segment(content, kw.value) or ast.unparse(kw.value)
                    new_kwargs.append(f"{indent}    {kw.arg}={val_str}")

            rep = (
                f"{indent}@product_action(\n"
                + ",\n".join(new_kwargs)
                + f"\n{indent})\n"
            )

            lines[start_idx:end_idx] = [rep]

        # Insert missing imports
        interim_code = "".join(lines)
        try:
            tree_after = ast.parse(interim_code)
        except SyntaxError:
            return interim_code, issues

        has_path = any(
            isinstance(n, ast.ImportFrom) and n.module == "pathlib" and any(a.name == "Path" for a in n.names)
            for n in ast.walk(tree_after)
        )
        has_env = any(
            isinstance(n, ast.ImportFrom) and any(a.name == "ENVIRONMENT" for a in n.names)
            for n in ast.walk(tree_after)
        )
        has_compat = any(
            isinstance(n, ast.ImportFrom) and any(a.name == "AIRFLOW_V_3_0_PLUS" for a in n.names)
            for n in ast.walk(tree_after)
        )

        missing_imports: List[str] = []
        if not has_path:
            missing_imports.append("from pathlib import Path\n")
        if not has_env:
            missing_imports.append("from bp2i_airflow_library.config import ENVIRONMENT\n")
        if not has_compat:
            missing_imports.append("from bp2i_airflow_library.version_compat import AIRFLOW_V_3_0_PLUS\n")

        if missing_imports:
            # Find the best insertion line
            last_import_line = 0
            for n in ast.walk(tree_after):
                if isinstance(n, (ast.Import, ast.ImportFrom)):
                    if n.end_lineno and n.end_lineno > last_import_line:
                        last_import_line = n.end_lineno

            if last_import_line > 0:
                lines[last_import_line:last_import_line] = missing_imports
            else:
                # Check for module docstring
                docstring_end = 0
                if tree_after.body and isinstance(tree_after.body[0], ast.Expr) and isinstance(tree_after.body[0].value, ast.Constant):
                    docstring_end = tree_after.body[0].end_lineno or 1

                if docstring_end > 0:
                    lines[docstring_end:docstring_end] = ["\n"] + missing_imports
                else:
                    lines[0:0] = missing_imports

        applied.extend([i for i in issues if i.auto_fixable])
        return "".join(lines), applied
