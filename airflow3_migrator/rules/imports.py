"""
Rule for migrating imports to Airflow 3 standard providers and SDK.
"""

import ast
import re
from typing import List, Tuple, Dict, Set
from .base import BaseRule, IssueCategory, IssueSeverity, MigrationIssue


# Direct module remappings
MODULE_MAPPINGS = {
    # Bash
    "airflow.operators.bash": "airflow.providers.standard.operators.bash",
    "airflow.operators.bash_operator": "airflow.providers.standard.operators.bash",
    # Python
    "airflow.operators.python": "airflow.providers.standard.operators.python",
    "airflow.operators.python_operator": "airflow.providers.standard.operators.python",
    # Empty / Dummy
    "airflow.operators.empty": "airflow.providers.standard.operators.empty",
    "airflow.operators.dummy": "airflow.providers.standard.operators.empty",
    "airflow.operators.dummy_operator": "airflow.providers.standard.operators.empty",
    # Trigger Dag Run
    "airflow.operators.trigger_dagrun": "airflow.providers.standard.operators.trigger_dagrun",
    "airflow.operators.datetime": "airflow.providers.standard.operators.datetime",
    # Standard Sensors
    "airflow.sensors.filesystem": "airflow.providers.standard.sensors.filesystem",
    "airflow.sensors.time_sensor": "airflow.providers.standard.sensors.time",
    "airflow.sensors.time_delta": "airflow.providers.standard.sensors.time_delta",
    "airflow.sensors.date_time": "airflow.providers.standard.sensors.date_time",
    "airflow.sensors.bash": "airflow.providers.standard.sensors.bash",
    "airflow.sensors.python": "airflow.providers.standard.sensors.python",
}

# Specific symbol transformations
SYMBOL_MAPPINGS = {
    "DummyOperator": "EmptyOperator",
}


class ImportsMigrationRule(BaseRule):
    rule_id = "AIR302"
    category = IssueCategory.IMPORT
    title = "Mise à jour des imports vers les providers standard et le Task SDK"
    description = (
        "Airflow 3 a déplacé les opérateurs et capteurs standard (Bash, Python, Empty, etc.) "
        "vers le package séparé 'apache-airflow-providers-standard'. "
        "De plus, DummyOperator a été supprimé au profit de EmptyOperator."
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
            # Check 'from X import Y'
            if isinstance(node, ast.ImportFrom) and node.module:
                module = node.module
                line_idx = node.lineno - 1
                line_content = lines[line_idx] if line_idx < len(lines) else ""

                # Check subdag operator (Removed in Airflow 3)
                if module in ("airflow.operators.subdag", "airflow.operators.subdag_operator") or any(
                    alias.name == "SubDagOperator" for alias in node.names
                ):
                    issues.append(
                        MigrationIssue(
                            rule_id="AIR301_SUBDAG",
                            category=IssueCategory.OPERATOR,
                            severity=IssueSeverity.CRITICAL,
                            title="Suppression de SubDagOperator",
                            description=(
                                "SubDagOperator a été complètement supprimé dans Airflow 3. "
                                "Vous devez migrer vos SubDAGs vers des TaskGroups (airflow.utils.task_group.TaskGroup ou airflow.sdk.TaskGroup)."
                            ),
                            line_number=node.lineno,
                            column=node.col_offset,
                            file_path=file_path,
                            auto_fixable=False,
                            original_code=line_content,
                            suggested_code="# TODO: Migrer ce SubDAG vers un TaskGroup",
                            documentation_url="https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/task-groups.html",
                        )
                    )
                    continue

                # Check legacy airflow.contrib
                if module.startswith("airflow.contrib."):
                    issues.append(
                        MigrationIssue(
                            rule_id="AIR301_CONTRIB",
                            category=IssueCategory.IMPORT,
                            severity=IssueSeverity.CRITICAL,
                            title="Import obsolète airflow.contrib",
                            description=(
                                f"Le module '{module}' appartient à l'ancienne hiérarchie Airflow 1/2 obsolète. "
                                "Il doit être remplacé par le provider officiel correspondant (ex: airflow.providers.google, etc.)."
                            ),
                            line_number=node.lineno,
                            column=node.col_offset,
                            file_path=file_path,
                            auto_fixable=False,
                            original_code=line_content,
                            suggested_code=f"# Migrer vers le provider approprié",
                            documentation_url="https://airflow.apache.org/docs/apache-airflow-providers/",
                        )
                    )
                    continue

                # Check module mapping
                if module in MODULE_MAPPINGS:
                    new_module = MODULE_MAPPINGS[module]
                    has_dummy = any(alias.name == "DummyOperator" for alias in node.names)

                    # Build suggested replacement
                    new_names = []
                    for alias in node.names:
                        target_name = SYMBOL_MAPPINGS.get(alias.name, alias.name)
                        if alias.asname:
                            new_names.append(f"{target_name} as {alias.asname}")
                        else:
                            new_names.append(target_name)

                    indent = re.match(r"^\s*", line_content).group(0)
                    suggested_line = f"{indent}from {new_module} import {', '.join(new_names)}"

                    title = f"Déplacement du module '{module}' vers '{new_module}'"
                    if has_dummy:
                        title += " et remplacement de DummyOperator par EmptyOperator"

                    issues.append(
                        MigrationIssue(
                            rule_id=self.rule_id,
                            category=self.category,
                            severity=IssueSeverity.WARNING,
                            title=title,
                            description=(
                                f"L'import depuis '{module}' doit être mis à jour vers '{new_module}'. "
                                "Nécessite le package 'apache-airflow-providers-standard'."
                            ),
                            line_number=node.lineno,
                            column=node.col_offset,
                            file_path=file_path,
                            auto_fixable=True,
                            original_code=line_content,
                            suggested_code=suggested_line,
                            documentation_url=self.documentation_url,
                        )
                    )

            # Check direct 'import X'
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name in MODULE_MAPPINGS:
                        new_module = MODULE_MAPPINGS[alias.name]
                        line_idx = node.lineno - 1
                        line_content = lines[line_idx] if line_idx < len(lines) else ""
                        indent = re.match(r"^\s*", line_content).group(0)
                        suggested_line = (
                            f"{indent}import {new_module} as {alias.asname}"
                            if alias.asname
                            else f"{indent}import {new_module}"
                        )

                        issues.append(
                            MigrationIssue(
                                rule_id=self.rule_id,
                                category=self.category,
                                severity=IssueSeverity.WARNING,
                                title=f"Déplacement de l'import {alias.name} vers {new_module}",
                                description=f"Le module {alias.name} se trouve maintenant dans {new_module}.",
                                line_number=node.lineno,
                                column=node.col_offset,
                                file_path=file_path,
                                auto_fixable=True,
                                original_code=line_content,
                                suggested_code=suggested_line,
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

        # Sort issues in reverse order of line number to prevent offset shifts
        fixable_issues = sorted(
            [issue for issue in issues if issue.auto_fixable],
            key=lambda x: x.line_number,
            reverse=True,
        )

        for issue in fixable_issues:
            line_idx = issue.line_number - 1
            if line_idx < len(lines):
                # Check for line endings
                has_newline = lines[line_idx].endswith("\n")
                lines[line_idx] = issue.suggested_code + ("\n" if has_newline else "")
                applied.append(issue)

        return "".join(lines), applied
