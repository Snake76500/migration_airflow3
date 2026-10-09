"""
Rule for migrating SQLAlchemy 1.4 to SQLAlchemy 2.0 compatibility in Airflow 3.1 projects.
"""

import ast
import re
from typing import List, Tuple
from .base import BaseRule, IssueCategory, IssueSeverity, MigrationIssue


class SQLAlchemyMigrationRule(BaseRule):
    rule_id = "SQLA20"
    category = IssueCategory.DATABASE
    title = "Migration de compatibilité SQLAlchemy 1.4 vers SQLAlchemy 2.0"
    description = (
        "Airflow 3.1 s'aligne sur SQLAlchemy 2.0. "
        "Les modifications majeures incluent : "
        "- Les requêtes brutes 'execute(\"SELECT ...\")' doivent obligatoirement utiliser 'execute(text(\"...\"))' ; "
        "- La méthode 'Engine.execute()' a été supprimée au profit de 'with engine.connect() as conn: conn.execute(...)' ; "
        "- 'from sqlalchemy.ext.declarative import declarative_base' est déplacé vers 'from sqlalchemy.orm import declarative_base' ; "
        "- L'option 'autocommit=True' est supprimée (passage au 'commit as you go')."
    )
    documentation_url = "https://docs.sqlalchemy.org/en/20/changelog/migration_20.html"

    def analyze(self, content: str, file_path: str) -> List[MigrationIssue]:
        issues: List[MigrationIssue] = []
        try:
            tree = ast.parse(content)
        except SyntaxError:
            return issues

        lines = content.splitlines()

        # Identify nodes already inside an 'if AIRFLOW_V_3_0_PLUS:' statement
        compat_if_node_ids = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.If) and "AIRFLOW_V_3_0_PLUS" in ast.unparse(node.test):
                for child in ast.walk(node):
                    if child is not node:
                        compat_if_node_ids.add(id(child))

        for node in ast.walk(tree):
            # 1. declarative imports from sqlalchemy.ext.declarative
            if isinstance(node, ast.ImportFrom) and node.module == "sqlalchemy.ext.declarative":
                if id(node) in compat_if_node_ids:
                    continue
                line_idx = node.lineno - 1
                line_content = lines[line_idx] if line_idx < len(lines) else ""
                indent = re.match(r"^\s*", line_content).group(0)
                names_str = ", ".join(
                    f"{a.name} as {a.asname}" if a.asname else a.name for a in node.names
                )

                if self.dual_compat:
                    suggested = (
                        f"{indent}if AIRFLOW_V_3_0_PLUS:\n"
                        f"{indent}    from sqlalchemy.orm import {names_str}\n"
                        f"{indent}else:\n"
                        f"{indent}    from sqlalchemy.ext.declarative import {names_str}"
                    )
                    title = "Import bi-compatible de SQLAlchemy declarative (Airflow 2 & 3)"
                    desc = (
                        "Importation conditionnelle de 'sqlalchemy.orm' (Airflow 3 / SQLAlchemy 2.0) "
                        "ou 'sqlalchemy.ext.declarative' (Airflow 2 / SQLAlchemy 1.4)."
                    )
                else:
                    suggested = f"{indent}from sqlalchemy.orm import {names_str}"
                    title = "Déplacement de declarative_base vers sqlalchemy.orm"
                    desc = (
                        "Dans SQLAlchemy 2.0, les éléments de 'sqlalchemy.ext.declarative' "
                        "ont été déplacés vers 'sqlalchemy.orm'."
                    )

                issues.append(
                    MigrationIssue(
                        rule_id="SQLA20_DECLARATIVE_BASE",
                        category=self.category,
                        severity=IssueSeverity.WARNING,
                        title=title,
                        description=desc,
                        line_number=node.lineno,
                        column=node.col_offset,
                        file_path=file_path,
                        auto_fixable=True,
                        original_code=line_content,
                        suggested_code=suggested,
                        documentation_url=self.documentation_url,
                    )
                )

            # 2. engine.execute(...) or conn.execute("raw sql") / session.execute("raw sql")
            elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "execute":
                caller_name = getattr(node.func.value, "id", "") or getattr(node.func.value, "attr", "")

                # 2.a engine.execute(...) removed in 2.0
                if caller_name == "engine":
                    line_idx = node.lineno - 1
                    line_content = lines[line_idx] if line_idx < len(lines) else ""
                    issues.append(
                        MigrationIssue(
                            rule_id="SQLA20_ENGINE_EXECUTE",
                            category=self.category,
                            severity=IssueSeverity.CRITICAL,
                            title="Suppression de 'Engine.execute()' dans SQLAlchemy 2.0",
                            description=(
                                "La méthode 'Engine.execute()' est complètement supprimée en SQLAlchemy 2.0. "
                                "Utilisez : 'with engine.connect() as conn: conn.execute(text(...))' ou 'with engine.begin() as conn:'."
                            ),
                            line_number=node.lineno,
                            column=node.col_offset,
                            file_path=file_path,
                            auto_fixable=False,
                            original_code=line_content,
                            suggested_code="# with engine.connect() as conn: conn.execute(text(...))",
                            documentation_url=self.documentation_url,
                        )
                    )

                # 2.b raw string in execute(...)
                if node.args:
                    first_arg = node.args[0]
                    # Check if argument is a string literal or f-string and not wrapped in text(...)
                    if isinstance(first_arg, (ast.Constant, ast.JoinedStr)):
                        line_idx = node.lineno - 1
                        line_content = lines[line_idx] if line_idx < len(lines) else ""
                        raw_arg = ast.get_source_segment(content, first_arg) or ast.unparse(first_arg)

                        # Check if already wrapped in text
                        if not raw_arg.strip().startswith("text("):
                            suggested = line_content.replace(raw_arg, f"text({raw_arg})", 1)
                            issues.append(
                                MigrationIssue(
                                    rule_id="SQLA20_RAW_SQL_TEXT",
                                    category=self.category,
                                    severity=IssueSeverity.WARNING,
                                    title="Obligation de 'text()' pour les requêtes textuelles dans execute()",
                                    description=(
                                        "Dans SQLAlchemy 2.0, les chaînes SQL brutes transmises à execute() "
                                        "doivent être explicitement enveloppées dans 'text(\"...\")' "
                                        "pour des raisons de sécurité et de typage strict."
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

            # 3. autocommit=True in execution_options
            elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "execution_options":
                for kw in node.keywords:
                    if kw.arg == "autocommit":
                        line_idx = node.lineno - 1
                        line_content = lines[line_idx] if line_idx < len(lines) else ""
                        issues.append(
                            MigrationIssue(
                                rule_id="SQLA20_AUTOCOMMIT",
                                category=self.category,
                                severity=IssueSeverity.WARNING,
                                title="Suppression de l'option 'autocommit' dans SQLAlchemy 2.0",
                                description=(
                                    "L'option 'autocommit=True' dans execution_options a été supprimée. "
                                    "SQLAlchemy 2.0 utilise le mode 'commit as you go' : "
                                    "effectuez des 'conn.commit()' explicites ou utilisez 'with engine.begin() as conn:'."
                                ),
                                line_number=node.lineno,
                                column=node.col_offset,
                                file_path=file_path,
                                auto_fixable=False,
                                original_code=line_content,
                                suggested_code="# Utilisez 'conn.commit()' ou 'with engine.begin() as conn:'",
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

        try:
            tree = ast.parse(content)
        except SyntaxError:
            return content, []

        # Fix declarative_base
        for issue in issues:
            if issue.auto_fixable and issue.rule_id == "SQLA20_DECLARATIVE_BASE":
                idx = issue.line_number - 1
                if idx < len(lines):
                    has_nl = lines[idx].endswith("\n")
                    lines[idx] = issue.suggested_code + ("\n" if has_nl else "")
                    applied.append(issue)

        # Fix raw SQL in execute()
        raw_sql_issues = [i for i in issues if i.auto_fixable and i.rule_id == "SQLA20_RAW_SQL_TEXT"]
        if raw_sql_issues:
            # Sort in reverse line order
            raw_sql_issues.sort(key=lambda x: x.line_number, reverse=True)
            for issue in raw_sql_issues:
                idx = issue.line_number - 1
                if idx < len(lines):
                    has_nl = lines[idx].endswith("\n")
                    # Replace in that line
                    node_tree = ast.parse(lines[idx])
                    for subnode in ast.walk(node_tree):
                        if (
                            isinstance(subnode, ast.Call)
                            and isinstance(subnode.func, ast.Attribute)
                            and subnode.func.attr == "execute"
                            and subnode.args
                        ):
                            first_arg = subnode.args[0]
                            if isinstance(first_arg, (ast.Constant, ast.JoinedStr)):
                                arg_str = ast.get_source_segment(lines[idx], first_arg) or ast.unparse(first_arg)
                                if not arg_str.strip().startswith("text("):
                                    lines[idx] = lines[idx].replace(arg_str, f"text({arg_str})", 1)
                                    applied.append(issue)

            # Ensure 'from sqlalchemy import text' is present
            interim_code = "".join(lines)
            try:
                tree_after = ast.parse(interim_code)
                has_text_import = any(
                    isinstance(n, ast.ImportFrom)
                    and n.module in ("sqlalchemy", "sqlalchemy.sql")
                    and any(a.name == "text" for a in n.names)
                    for n in ast.walk(tree_after)
                )
                if not has_text_import:
                    # Find insertion point
                    last_import_line = max(
                        (n.end_lineno for n in ast.walk(tree_after) if isinstance(n, (ast.Import, ast.ImportFrom))),
                        default=0,
                    )
                    lines.insert(last_import_line, "from sqlalchemy import text\n")
            except SyntaxError:
                pass

        result = "".join(lines)
        if self.dual_compat and any(i.rule_id == "SQLA20_DECLARATIVE_BASE" for i in applied):
            result = self.ensure_airflow_v3_compat_import(result)

        return result, applied

