"""
Rule for migrating Airflow configuration files (airflow.cfg, .env, docker-compose).
"""

import re
from typing import List, Tuple
from .base import BaseRule, IssueCategory, IssueSeverity, MigrationIssue


class ConfigMigrationRule(BaseRule):
    rule_id = "AIR308"
    category = IssueCategory.CONFIG
    title = "Migration de la configuration airflow.cfg et variables d'environnement"
    description = (
        "Airflow 3 a supprimé plusieurs paramètres de configuration critiques : "
        "- 'enable_xcom_pickling' a été supprimé (failles de sécurité) ; "
        "- 'SequentialExecutor' est remplacé par 'LocalExecutor' ; "
        "- 'CeleryKubernetesExecutor' et 'LocalKubernetesExecutor' sont remplacés par le multi-executor."
    )
    documentation_url = "https://airflow.apache.org/docs/apache-airflow/stable/upgrading-to-airflow-3.html"

    def analyze(self, content: str, file_path: str) -> List[MigrationIssue]:
        issues: List[MigrationIssue] = []
        lines = content.splitlines()

        for line_no, line in enumerate(lines, start=1):
            # 1. enable_xcom_pickling
            if re.search(r"^\s*enable_xcom_pickling\s*=\s*(True|true|1)", line, re.IGNORECASE):
                issues.append(
                    MigrationIssue(
                        rule_id="AIR308_XCOM_PICKLE",
                        category=self.category,
                        severity=IssueSeverity.CRITICAL,
                        title="Suppression de 'enable_xcom_pickling'",
                        description=(
                            "'enable_xcom_pickling' a été définitivement supprimé pour des raisons de sécurité. "
                            "Pour sérialiser des objets complexes, configurez un custom XCom backend "
                            "(ex: S3, GCS, ou backend sérialisé JSON/Cloud)."
                        ),
                        line_number=line_no,
                        column=0,
                        file_path=file_path,
                        auto_fixable=True,
                        original_code=line,
                        suggested_code=f"# SUPPRIMÉ EN AIRFLOW 3: {line.strip()} -> Utilisez un custom XCom backend",
                        documentation_url=self.documentation_url,
                    )
                )

            # 2. SequentialExecutor
            if re.search(r"^\s*executor\s*=\s*SequentialExecutor\b", line):
                suggested = re.sub(r"SequentialExecutor", "LocalExecutor", line)
                issues.append(
                    MigrationIssue(
                        rule_id="AIR308_EXECUTOR_SEQ",
                        category=self.category,
                        severity=IssueSeverity.WARNING,
                        title="Remplacement de SequentialExecutor par LocalExecutor",
                        description=(
                            "Dans Airflow 3, SequentialExecutor a été supprimé au profit de LocalExecutor, "
                            "qui prend désormais en charge SQLite pour les environnements de test et développement."
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

            # 3. CeleryKubernetesExecutor / LocalKubernetesExecutor
            if re.search(r"^\s*executor\s*=\s*(CeleryKubernetesExecutor|LocalKubernetesExecutor)\b", line):
                issues.append(
                    MigrationIssue(
                        rule_id="AIR308_HYBRID_EXECUTOR",
                        category=self.category,
                        severity=IssueSeverity.WARNING,
                        title="Migration des exécuteurs hybrides vers le multi-executor",
                        description=(
                            "CeleryKubernetesExecutor et LocalKubernetesExecutor sont remplacés par la nouvelle "
                            "configuration multi-exécuteur d'Airflow (ex: core.executor = CeleryExecutor,KubernetesExecutor)."
                        ),
                        line_number=line_no,
                        column=0,
                        file_path=file_path,
                        auto_fixable=False,
                        original_code=line,
                        suggested_code="# core.executor = CeleryExecutor,KubernetesExecutor",
                        documentation_url=self.documentation_url,
                    )
                )

            # 4. AIRFLOW__CORE__ENABLE_XCOM_PICKLING in env or docker-compose
            if "AIRFLOW__CORE__ENABLE_XCOM_PICKLING" in line:
                issues.append(
                    MigrationIssue(
                        rule_id="AIR308_ENV_PICKLE",
                        category=self.category,
                        severity=IssueSeverity.CRITICAL,
                        title="Variable d'environnement AIRFLOW__CORE__ENABLE_XCOM_PICKLING supprimée",
                        description=(
                            "La variable d'environnement pour XCom pickling n'a plus d'effet en Airflow 3 "
                            "et doit être retirée de vos configurations de conteneur."
                        ),
                        line_number=line_no,
                        column=0,
                        file_path=file_path,
                        auto_fixable=True,
                        original_code=line,
                        suggested_code=f"# {line.strip()} (Supprimé en Airflow 3)",
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
            key=lambda x: x.line_number,
            reverse=True,
        )

        for issue in fixable:
            idx = issue.line_number - 1
            if idx < len(lines):
                has_nl = lines[idx].endswith("\n")
                lines[idx] = issue.suggested_code + ("\n" if has_nl else "")
                applied.append(issue)

        return "".join(lines), applied
