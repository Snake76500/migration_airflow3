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
    "airflow.sensors.external_task": "airflow.providers.standard.sensors.external_task",
    "airflow.sensors.external_task_sensor": "airflow.providers.standard.sensors.external_task",
    # Standard Hooks
    "airflow.hooks.subprocess": "airflow.providers.standard.hooks.subprocess",
    "airflow.hooks.filesystem": "airflow.providers.standard.hooks.filesystem",
    # Context & Decorators moving to Task SDK
    "airflow.utils.context": "airflow.sdk",
    "airflow.decorators": "airflow.sdk",
    "airflow.utils.task_group": "airflow.sdk",
}

# Specific symbol transformations
SYMBOL_MAPPINGS = {
    "DummyOperator": "EmptyOperator",
}

# Symbols that belong to the stable Task SDK (airflow.sdk) rather than provider operators
SYMBOL_DEST_OVERRIDES = {
    "get_current_context": "airflow.sdk",
    "task": "airflow.sdk",
    "dag": "airflow.sdk",
    "TaskGroup": "airflow.sdk",
}


class ImportsMigrationRule(BaseRule):
    rule_id = "AIR302"
    category = IssueCategory.IMPORT
    title = "Mise à jour des imports vers les providers standard et le Task SDK"
    description = (
        "Airflow 3 a déplacé les opérateurs, capteurs et hooks standard vers 'apache-airflow-providers-standard'. "
        "Les fonctionnalités du Task SDK (get_current_context, @task, @dag, TaskGroup) "
        "doivent désormais être importées depuis 'airflow.sdk'. "
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
                end_line_idx = node.end_lineno if node.end_lineno else node.lineno
                orig_snippet = "\n".join(lines[line_idx:end_line_idx]) if line_idx < len(lines) else ""

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
                                "Vous devez migrer vos SubDAGs vers des TaskGroups (airflow.sdk.TaskGroup)."
                            ),
                            line_number=node.lineno,
                            column=node.col_offset,
                            end_line_number=node.end_lineno,
                            end_column=node.end_col_offset,
                            file_path=file_path,
                            auto_fixable=False,
                            original_code=orig_snippet,
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
                            end_line_number=node.end_lineno,
                            end_column=node.end_col_offset,
                            file_path=file_path,
                            auto_fixable=False,
                            original_code=orig_snippet,
                            suggested_code="# Migrer vers le provider approprié",
                            documentation_url="https://airflow.apache.org/docs/apache-airflow-providers/",
                        )
                    )
                    continue

                # Route symbols to appropriate modules (Task SDK vs Providers Standard)
                symbols_by_dest: Dict[str, List[str]] = {}
                needs_migration = False

                for alias in node.names:
                    target_name = SYMBOL_MAPPINGS.get(alias.name, alias.name)
                    dest_mod = None

                    # Check symbol-specific override (e.g. get_current_context, task, dag, TaskGroup -> airflow.sdk)
                    if alias.name in SYMBOL_DEST_OVERRIDES:
                        dest_mod = SYMBOL_DEST_OVERRIDES[alias.name]
                    # Check module-level remapping
                    elif module in MODULE_MAPPINGS:
                        dest_mod = MODULE_MAPPINGS[module]

                    if dest_mod:
                        if dest_mod != module or target_name != alias.name:
                            needs_migration = True
                        formatted = f"{target_name} as {alias.asname}" if alias.asname else target_name
                        symbols_by_dest.setdefault(dest_mod, []).append(formatted)
                    else:
                        formatted = f"{target_name} as {alias.asname}" if alias.asname else target_name
                        symbols_by_dest.setdefault(module, []).append(formatted)

                if needs_migration:
                    indent = re.match(r"^\s*", lines[line_idx]).group(0) if line_idx < len(lines) else ""
                    replacement_lines = []
                    for dest_mod, sym_list in symbols_by_dest.items():
                        replacement_lines.append(f"{indent}from {dest_mod} import {', '.join(sym_list)}")
                    suggested_code = "\n".join(replacement_lines)

                    has_sdk = any(alias.name in SYMBOL_DEST_OVERRIDES for alias in node.names) or any(
                        d == "airflow.sdk" for d in symbols_by_dest
                    )
                    has_dummy = any(alias.name == "DummyOperator" for alias in node.names)

                    title = f"Mise à jour de l'import depuis '{module}'"
                    if has_sdk:
                        title += " (redirection vers airflow.sdk)"
                    elif has_dummy:
                        title += " (remplacement de DummyOperator par EmptyOperator)"

                    issues.append(
                        MigrationIssue(
                            rule_id=self.rule_id,
                            category=self.category,
                            severity=IssueSeverity.WARNING,
                            title=title,
                            description=(
                                f"L'import depuis '{module}' a été restructuré pour Airflow 3 : "
                                "les opérateurs standard sont dans 'apache-airflow-providers-standard' "
                                "et les composants Task SDK (get_current_context, @task, TaskGroup) sont dans 'airflow.sdk'."
                            ),
                            line_number=node.lineno,
                            column=node.col_offset,
                            end_line_number=node.end_lineno,
                            end_column=node.end_col_offset,
                            file_path=file_path,
                            auto_fixable=True,
                            original_code=orig_snippet,
                            suggested_code=suggested_code,
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
                                end_line_number=node.end_lineno,
                                end_column=node.end_col_offset,
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
            start_idx = issue.line_number - 1
            end_idx = issue.end_line_number if issue.end_line_number else issue.line_number
            if start_idx < len(lines):
                # Check for line endings
                has_newline = lines[min(end_idx - 1, len(lines) - 1)].endswith("\n")
                rep = issue.suggested_code + ("\n" if has_newline else "")
                lines[start_idx:end_idx] = [rep]
                applied.append(issue)

        return "".join(lines), applied
