"""
Rule for migrating project dependency files (requirements.txt, pyproject.toml, Pipfile) for Airflow 3.1 and SQLAlchemy 2.0.
"""

import re
from typing import List, Tuple
from .base import BaseRule, IssueCategory, IssueSeverity, MigrationIssue


class DependenciesMigrationRule(BaseRule):
    rule_id = "AIR309"
    category = IssueCategory.DEPENDENCY
    title = "Mise à jour des dépendances du projet pour Airflow 3.1 et SQLAlchemy 2.0"
    description = (
        "Airflow 3.1 nécessite d'actualiser la version de 'apache-airflow' vers 3.1, "
        "d'ajouter 'apache-airflow-providers-standard' pour les opérateurs usuels "
        "et de mettre à niveau SQLAlchemy vers 2.0."
    )
    documentation_url = "https://airflow.apache.org/docs/apache-airflow/stable/upgrading-to-airflow-3.html"

    def analyze(self, content: str, file_path: str) -> List[MigrationIssue]:
        issues: List[MigrationIssue] = []
        filename = file_path.lower()

        if "requirements" in filename or filename.endswith(".txt"):
            lines = content.splitlines()
            has_standard_provider = False
            has_airflow = False
            airflow_line_no = -1

            for line_no, line in enumerate(lines, start=1):
                clean = line.strip()
                if "apache-airflow-providers-standard" in clean:
                    has_standard_provider = True

                # Airflow dependency check (target Airflow 3.1 strictly)
                if re.match(r"^apache-airflow\b", clean) and not clean.startswith("apache-airflow-providers-"):
                    has_airflow = True
                    airflow_line_no = line_no
                    if not re.search(r"3\.1", clean):
                        suggested = "apache-airflow~=3.1.0"
                        issues.append(
                            MigrationIssue(
                                rule_id="AIR309_AIRFLOW_VER",
                                category=self.category,
                                severity=IssueSeverity.WARNING,
                                title="Mise à niveau de 'apache-airflow' vers Airflow 3.1",
                                description=(
                                    f"La version actuelle spécifiée ('{clean}') doit cibler Airflow 3.1. "
                                    "Mettez à niveau vers 'apache-airflow~=3.1.0'."
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

                # SQLAlchemy 1.4 -> 2.0 check
                if re.match(r"^sqlalchemy\b", clean, re.IGNORECASE):
                    if not re.search(r"[>=~]\s*2\.", clean):
                        suggested = "sqlalchemy>=2.0.0"
                        issues.append(
                            MigrationIssue(
                                rule_id="SQLA20_DEP",
                                category=self.category,
                                severity=IssueSeverity.WARNING,
                                title="Mise à niveau de SQLAlchemy vers 2.0 pour Airflow 3.1",
                                description=(
                                    f"La version spécifiée ('{clean}') cible SQLAlchemy 1.x. "
                                    "Airflow 3.1 impose la compatibilité avec SQLAlchemy 2.0. "
                                    "Mettez à niveau vers 'sqlalchemy>=2.0.0'."
                                ),
                                line_number=line_no,
                                column=0,
                                file_path=file_path,
                                auto_fixable=True,
                                original_code=line,
                                suggested_code=suggested,
                                documentation_url="https://docs.sqlalchemy.org/en/20/changelog/migration_20.html",
                            )
                        )

            # Check if standard provider is missing
            if has_airflow and not has_standard_provider:
                issues.append(
                    MigrationIssue(
                        rule_id="AIR309_MISSING_STANDARD_PROVIDER",
                        category=self.category,
                        severity=IssueSeverity.WARNING,
                        title="Ajout indispensable de 'apache-airflow-providers-standard'",
                        description=(
                            "Airflow 3.1 a extrait les opérateurs fondamentaux (BashOperator, PythonOperator, etc.) "
                            "dans le package 'apache-airflow-providers-standard'. Ce package doit être ajouté à vos dépendances."
                        ),
                        line_number=airflow_line_no if airflow_line_no > 0 else 1,
                        column=0,
                        file_path=file_path,
                        auto_fixable=True,
                        original_code="",
                        suggested_code="apache-airflow-providers-standard>=1.0.0",
                        documentation_url=self.documentation_url,
                    )
                )

        elif "pyproject.toml" in filename:
            lines = content.splitlines()
            has_standard_provider = "apache-airflow-providers-standard" in content

            for line_no, line in enumerate(lines, start=1):
                if re.search(r'["\']apache-airflow(?:\s*[<>=~!][^"\']*)?["\']', line) and "providers" not in line:
                    if "3.1" not in line:
                        suggested = re.sub(
                            r'["\']apache-airflow(?:\s*[<>=~!][^"\']*)?["\']',
                            '"apache-airflow~=3.1.0"',
                            line,
                        )
                        issues.append(
                            MigrationIssue(
                                rule_id="AIR309_PYPROJECT_VER",
                                category=self.category,
                                severity=IssueSeverity.WARNING,
                                title="Mise à niveau de apache-airflow vers Airflow 3.1 dans pyproject.toml",
                                description="Mettez à niveau la dépendance apache-airflow vers la version 3.1.",
                                line_number=line_no,
                                column=0,
                                file_path=file_path,
                                auto_fixable=True,
                                original_code=line,
                                suggested_code=suggested,
                                documentation_url=self.documentation_url,
                            )
                        )
                    if not has_standard_provider:
                        issues.append(
                            MigrationIssue(
                                rule_id="AIR309_PYPROJECT_MISSING_STANDARD",
                                category=self.category,
                                severity=IssueSeverity.WARNING,
                                title="Ajout de apache-airflow-providers-standard dans pyproject.toml",
                                description=(
                                    "Ajoutez 'apache-airflow-providers-standard>=1.0.0' dans vos dépendances."
                                ),
                                line_number=line_no,
                                column=0,
                                file_path=file_path,
                                auto_fixable=False,
                                original_code=line,
                                suggested_code='    "apache-airflow-providers-standard>=1.0.0",',
                                documentation_url=self.documentation_url,
                            )
                        )

                # SQLAlchemy check in pyproject.toml
                if re.search(r'["\']sqlalchemy(?:\s*[<>=~!][^"\']*)?["\']', line, re.IGNORECASE):
                    if not re.search(r'["\']sqlalchemy.*2\.', line, re.IGNORECASE):
                        suggested = re.sub(
                            r'["\']sqlalchemy(?:\s*[<>=~!][^"\']*)?["\']',
                            '"sqlalchemy>=2.0.0"',
                            line,
                            flags=re.IGNORECASE,
                        )
                        issues.append(
                            MigrationIssue(
                                rule_id="SQLA20_PYPROJECT_DEP",
                                category=self.category,
                                severity=IssueSeverity.WARNING,
                                title="Mise à niveau de sqlalchemy vers 2.0 dans pyproject.toml",
                                description="Mettez à niveau la dépendance sqlalchemy vers la version 2.0.",
                                line_number=line_no,
                                column=0,
                                file_path=file_path,
                                auto_fixable=True,
                                original_code=line,
                                suggested_code=suggested,
                                documentation_url="https://docs.sqlalchemy.org/en/20/changelog/migration_20.html",
                            )
                        )

        return issues

    def fix(self, content: str, file_path: str) -> Tuple[str, List[MigrationIssue]]:
        issues = self.analyze(content, file_path)
        if not issues:
            return content, []

        filename = file_path.lower()
        lines = content.splitlines(keepends=True)
        applied: List[MigrationIssue] = []

        if "requirements" in filename or filename.endswith(".txt"):
            fixable = [i for i in issues if i.auto_fixable]
            missing_std = [i for i in fixable if i.rule_id == "AIR309_MISSING_STANDARD_PROVIDER"]
            ver_fixes = [i for i in fixable if i.rule_id in ("AIR309_AIRFLOW_VER", "SQLA20_DEP")]

            for issue in ver_fixes:
                idx = issue.line_number - 1
                if idx < len(lines):
                    has_nl = lines[idx].endswith("\n")
                    lines[idx] = issue.suggested_code + ("\n" if has_nl else "")
                    applied.append(issue)

            if missing_std:
                issue = missing_std[0]
                idx = issue.line_number - 1
                if 0 <= idx < len(lines):
                    lines.insert(idx + 1, issue.suggested_code + "\n")
                else:
                    lines.append(issue.suggested_code + "\n")
                applied.append(issue)

        elif "pyproject.toml" in filename:
            ver_fixes = [i for i in issues if i.rule_id in ("AIR309_PYPROJECT_VER", "SQLA20_PYPROJECT_DEP") and i.auto_fixable]
            for issue in ver_fixes:
                idx = issue.line_number - 1
                if idx < len(lines):
                    has_nl = lines[idx].endswith("\n")
                    lines[idx] = issue.suggested_code + ("\n" if has_nl else "")
                    applied.append(issue)

        return "".join(lines), applied
