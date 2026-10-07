"""
Rule for detecting direct metadata database / ORM access (forbidden in Airflow 3 Task Execution API).
"""

import ast
import re
from typing import List, Tuple
from .base import BaseRule, IssueCategory, IssueSeverity, MigrationIssue


class DatabaseAccessMigrationRule(BaseRule):
    rule_id = "AIR307"
    category = IssueCategory.DATABASE
    title = "Détection d'accès direct à la base de métadonnées (Interdit en Airflow 3)"
    description = (
        "Airflow 3 introduit une architecture découplée avec un serveur d'API d'exécution (Task Execution API). "
        "Les workers et les tâches n'ont plus d'accès direct par base de données/ORM à la base de métadonnées. "
        "Toute utilisation directe de 'provide_session', 'airflow.settings.Session' ou de requêtes ORM directes "
        "dans le code des tâches échouera en production."
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
            # 1. Imports like provide_session or Session
            if isinstance(node, ast.ImportFrom) and node.module:
                if node.module in ("airflow.utils.db", "airflow.settings"):
                    for alias in node.names:
                        if alias.name in ("provide_session", "Session"):
                            line_idx = node.lineno - 1
                            line_content = lines[line_idx] if line_idx < len(lines) else ""
                            issues.append(
                                MigrationIssue(
                                    rule_id=self.rule_id,
                                    category=self.category,
                                    severity=IssueSeverity.CRITICAL,
                                    title=f"Accès direct à la session de base de données ({alias.name})",
                                    description=(
                                        f"L'import '{alias.name}' depuis '{node.module}' indique un accès direct "
                                        "à la base de données Airflow. Les workers d'Airflow 3 n'ont plus d'accès direct à la base. "
                                        "Utilisez l'API REST Airflow, XComs, ou le Task SDK à la place."
                                    ),
                                    line_number=node.lineno,
                                    column=node.col_offset,
                                    file_path=file_path,
                                    auto_fixable=False,
                                    original_code=line_content,
                                    suggested_code="# REFACTOR: Remplacer l'accès direct ORM par le Task SDK ou l'API REST",
                                    documentation_url=self.documentation_url,
                                )
                            )

            # 2. Direct session.query calls
            elif isinstance(node, ast.Call):
                if isinstance(node.func, ast.Attribute) and node.func.attr == "query":
                    # Check if querying Airflow models
                    caller = getattr(node.func.value, "id", "")
                    if caller in ("session", "Session"):
                        line_idx = node.lineno - 1
                        line_content = lines[line_idx] if line_idx < len(lines) else ""
                        issues.append(
                            MigrationIssue(
                                rule_id="AIR307_QUERY",
                                category=self.category,
                                severity=IssueSeverity.CRITICAL,
                                title="Requête ORM directe détectée",
                                description=(
                                    "Une requête ORM directe 'session.query(...)' a été détectée. "
                                    "En Airflow 3, l'accès à la base de données doit transiter par l'Execution API."
                                ),
                                line_number=node.lineno,
                                column=node.col_offset,
                                file_path=file_path,
                                auto_fixable=False,
                                original_code=line_content,
                                suggested_code="# Utiliser le Task SDK ou l'API REST pour interroger les métadonnées",
                                documentation_url=self.documentation_url,
                            )
                        )

        return issues

    def fix(self, content: str, file_path: str) -> Tuple[str, List[MigrationIssue]]:
        # This rule requires architectural refactoring, so it's not automatically rewritten.
        return content, []
