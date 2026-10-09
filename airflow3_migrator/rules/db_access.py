"""
Rule for detecting direct Airflow metadata database access (forbidden in Airflow 3.1 Task Execution API).
Note: Access to private/external business databases is completely permitted and not flagged.
"""

import ast
from typing import List, Tuple, Set
from .base import BaseRule, IssueCategory, IssueSeverity, MigrationIssue


# Known Airflow internal metadata models
AIRFLOW_METADATA_MODELS = {
    "DagRun",
    "TaskInstance",
    "DagModel",
    "Log",
    "XCom",
    "Variable",
    "Connection",
    "Pool",
    "Job",
    "SlotPool",
    "Trigger",
    "DagWarning",
    "SerializedDagModel",
}


class DatabaseAccessMigrationRule(BaseRule):
    rule_id = "AIR307"
    category = IssueCategory.DATABASE
    title = "Détection d'accès direct à la base de métadonnées Airflow (Interdit en Airflow 3.1)"
    description = (
        "Airflow 3.1 isole strictement la base de métadonnées interne via la Task Execution API. "
        "Les tâches ne doivent plus requêter directement les tables de métadonnées d'Airflow "
        "(DagRun, TaskInstance, Variable, etc.) ni utiliser 'provide_session' ou 'airflow.settings.Session'. "
        "Les requêtes vers vos propres bases de données privées/métier restent parfaitement autorisées."
    )
    documentation_url = "https://airflow.apache.org/docs/apache-airflow/stable/upgrading-to-airflow-3.html"

    def analyze(self, content: str, file_path: str) -> List[MigrationIssue]:
        issues: List[MigrationIssue] = []
        try:
            tree = ast.parse(content)
        except SyntaxError:
            return issues

        lines = content.splitlines()

        # Step 1: Detect if Airflow metadata models or Airflow session helpers are imported
        airflow_models_imported: Set[str] = set()
        has_airflow_session_import: bool = False

        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                if node.module.startswith("airflow.models"):
                    for alias in node.names:
                        if alias.name in AIRFLOW_METADATA_MODELS:
                            airflow_models_imported.add(alias.asname or alias.name)

                if node.module in ("airflow.utils.db", "airflow.settings"):
                    for alias in node.names:
                        if alias.name in ("provide_session", "Session"):
                            has_airflow_session_import = True
                            line_idx = node.lineno - 1
                            line_content = lines[line_idx] if line_idx < len(lines) else ""
                            issues.append(
                                MigrationIssue(
                                    rule_id=self.rule_id,
                                    category=self.category,
                                    severity=IssueSeverity.CRITICAL,
                                    title=f"Accès direct à la session de métadonnées Airflow ({alias.name})",
                                    description=(
                                        f"L'import '{alias.name}' depuis '{node.module}' cible la base de métadonnées interne d'Airflow. "
                                        "Les workers d'Airflow 3.1 n'ont plus d'accès direct à la base de métadonnées. "
                                        "Utilisez le Task SDK ou l'API REST Airflow à la place."
                                    ),
                                    line_number=node.lineno,
                                    column=node.col_offset,
                                    file_path=file_path,
                                    auto_fixable=False,
                                    original_code=line_content,
                                    suggested_code="# Utiliser le Task SDK ou l'API REST pour accéder aux métadonnées Airflow",
                                    documentation_url=self.documentation_url,
                                )
                            )

        # Step 2: Detect queries specifically targeting Airflow metadata models
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                # e.g. session.query(...)
                if isinstance(node.func, ast.Attribute) and node.func.attr == "query":
                    # Check if querying Airflow metadata models
                    is_airflow_target = False
                    targeted_model_name = ""

                    for arg in node.args:
                        arg_id = getattr(arg, "id", "")
                        arg_attr = getattr(arg, "attr", "")
                        if arg_id in airflow_models_imported or arg_id in AIRFLOW_METADATA_MODELS:
                            is_airflow_target = True
                            targeted_model_name = arg_id
                            break
                        elif arg_attr in AIRFLOW_METADATA_MODELS:
                            is_airflow_target = True
                            targeted_model_name = arg_attr
                            break

                    # Also flag if decorated with @provide_session or using Airflow internal session
                    if not is_airflow_target and has_airflow_session_import:
                        caller = getattr(node.func.value, "id", "")
                        if caller in ("session", "Session"):
                            # Only flag if not an explicit private model
                            is_airflow_target = True
                            targeted_model_name = "Session Airflow interne"

                    if is_airflow_target:
                        line_idx = node.lineno - 1
                        line_content = lines[line_idx] if line_idx < len(lines) else ""
                        issues.append(
                            MigrationIssue(
                                rule_id="AIR307_QUERY",
                                category=self.category,
                                severity=IssueSeverity.CRITICAL,
                                title=f"Requête ORM directe sur métadonnées Airflow ({targeted_model_name})",
                                description=(
                                    f"Une requête ORM directe sur '{targeted_model_name}' a été détectée. "
                                    "En Airflow 3.1, l'accès aux métadonnées Airflow doit impérativement transiter par l'Execution API / Task SDK. "
                                    "(Note : les requêtes vers vos bases de données métier privées restent autorisées)."
                                ),
                                line_number=node.lineno,
                                column=node.col_offset,
                                file_path=file_path,
                                auto_fixable=False,
                                original_code=line_content,
                                suggested_code="# Utiliser le Task SDK ou l'API REST pour interroger les métadonnées Airflow",
                                documentation_url=self.documentation_url,
                            )
                        )

        return issues

    def fix(self, content: str, file_path: str) -> Tuple[str, List[MigrationIssue]]:
        # Architectural migration, manual refactoring advised
        return content, []
