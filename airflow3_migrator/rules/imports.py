"""
Rule for migrating imports to Airflow 3 standard providers and SDK.
Supports both direct Airflow 3 migration and dual-compatibility mode (if AIRFLOW_V_3_0_PLUS: ... else: ...).
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
    "airflow.datasets": "airflow.sdk",
}

# Specific symbol transformations
SYMBOL_MAPPINGS = {
    "DummyOperator": "EmptyOperator",
    "Dataset": "Asset",
}

# Symbols that belong to the stable Task SDK (airflow.sdk) rather than provider operators
SYMBOL_DEST_OVERRIDES = {
    "get_current_context": "airflow.sdk",
    "task": "airflow.sdk",
    "dag": "airflow.sdk",
    "TaskGroup": "airflow.sdk",
    "Dataset": "airflow.sdk",
    "Asset": "airflow.sdk",
}


class ImportsMigrationRule(BaseRule):
    rule_id = "AIR302"
    category = IssueCategory.IMPORT
    title = "Mise à jour des imports vers les providers standard et le Task SDK"
    description = (
        "Airflow 3 a déplacé les opérateurs, capteurs et hooks standard vers 'apache-airflow-providers-standard'. "
        "Les fonctionnalités du Task SDK (get_current_context, @task, @dag, TaskGroup, Asset) "
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

        # Check if already using AIRFLOW_V_3_0_PLUS compat block
        has_compat_if = any(
            isinstance(n, ast.If) and "AIRFLOW_V_3_0_PLUS" in ast.unparse(n.test)
            for n in ast.walk(tree)
        )

        # Check subdag operator (Removed in Airflow 3)
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                module = node.module
                line_idx = node.lineno - 1
                end_line_idx = node.end_lineno if node.end_lineno else node.lineno
                orig_snippet = "\n".join(lines[line_idx:end_line_idx]) if line_idx < len(lines) else ""

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

        if has_compat_if:
            # Already dual-compatible, no further import rewrite needed
            return issues

        # Collect version-dependent imports
        version_nodes = []
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                if node.module in MODULE_MAPPINGS or any(
                    a.name in SYMBOL_DEST_OVERRIDES or a.name in SYMBOL_MAPPINGS for a in node.names
                ):
                    version_nodes.append(node)

        if not version_nodes:
            return issues

        version_nodes.sort(key=lambda n: n.lineno)

        if self.dual_compat:
            # Group v2 and v3 imports across all version_nodes
            v2_grouped: Dict[str, List[str]] = {}
            v3_grouped: Dict[str, List[str]] = {}

            for n in version_nodes:
                for a in n.names:
                    # v2 mapping
                    if a.name == "DummyOperator" and not a.asname:
                        v2_grouped.setdefault(n.module, []).extend(["DummyOperator", "DummyOperator as EmptyOperator"])
                    elif a.name == "Dataset" and not a.asname:
                        v2_grouped.setdefault(n.module, []).extend(["Dataset", "Dataset as Asset"])
                    else:
                        v2_grouped.setdefault(n.module, []).append(f"{a.name} as {a.asname}" if a.asname else a.name)

                    # v3 mapping
                    t_name = SYMBOL_MAPPINGS.get(a.name, a.name)
                    d_mod = SYMBOL_DEST_OVERRIDES.get(a.name, MODULE_MAPPINGS.get(n.module, n.module))
                    if a.name == "DummyOperator" and not a.asname:
                        v3_grouped.setdefault(d_mod, []).extend(["EmptyOperator", "EmptyOperator as DummyOperator"])
                    elif a.name == "Dataset" and not a.asname:
                        v3_grouped.setdefault(d_mod, []).extend(["Asset", "Asset as Dataset"])
                    else:
                        formatted = f"{t_name} as {a.asname}" if a.asname else t_name
                        v3_grouped.setdefault(d_mod, []).append(formatted)

            v3_lines = []
            for d_mod, syms in v3_grouped.items():
                unique_syms = list(dict.fromkeys(syms))
                v3_lines.append(f"from {d_mod} import {', '.join(unique_syms)}")

            v2_lines = []
            for s_mod, syms in v2_grouped.items():
                unique_syms = list(dict.fromkeys(syms))
                v2_lines.append(f"from {s_mod} import {', '.join(unique_syms)}")

            v3_block = "\n    ".join(v3_lines)
            v2_block = "\n    ".join(v2_lines)

            full_compat_block = (
                "try:\n"
                "    from bp2i_airflow_library.version_compat import AIRFLOW_V_3_0_PLUS\n"
                "except ImportError:\n"
                "    from airflow.version import version\n"
                '    AIRFLOW_V_3_0_PLUS = version.startswith("3")\n\n'
                f"if AIRFLOW_V_3_0_PLUS:\n"
                f"    {v3_block}\n"
                f"else:\n"
                f"    {v2_block}"
            )

            first_node = version_nodes[0]
            start_idx = first_node.lineno - 1
            orig_snippet = "\n".join(lines[start_idx : first_node.end_lineno or first_node.lineno])

            issues.append(
                MigrationIssue(
                    rule_id="AIR302_DUAL_COMPAT",
                    category=self.category,
                    severity=IssueSeverity.WARNING,
                    title="Mise en place du bloc d'imports bi-compatible (Airflow 2 & 3)",
                    description=(
                        "Création d'un bloc conditionnel 'if AIRFLOW_V_3_0_PLUS:' permettant au DAG "
                        "de s'exécuter de façon transparente sur Airflow 2 et Airflow 3."
                    ),
                    line_number=first_node.lineno,
                    column=first_node.col_offset,
                    end_line_number=first_node.end_lineno,
                    end_column=first_node.end_col_offset,
                    file_path=file_path,
                    auto_fixable=True,
                    original_code=orig_snippet,
                    suggested_code=full_compat_block,
                    documentation_url=self.documentation_url,
                )
            )

            # Mark other version nodes for cleanup
            for other_node in version_nodes[1:]:
                issues.append(
                    MigrationIssue(
                        rule_id="AIR302_DUAL_CLEANUP",
                        category=self.category,
                        severity=IssueSeverity.INFO,
                        title=f"Fusion de l'import {other_node.module} dans le bloc bi-compatible",
                        description="Cet import est fusionné dans le bloc principal 'if AIRFLOW_V_3_0_PLUS:'.",
                        line_number=other_node.lineno,
                        column=other_node.col_offset,
                        end_line_number=other_node.end_lineno,
                        end_column=other_node.end_col_offset,
                        file_path=file_path,
                        auto_fixable=True,
                        original_code="\n".join(lines[other_node.lineno - 1 : other_node.end_lineno or other_node.lineno]),
                        suggested_code="",
                        documentation_url=self.documentation_url,
                    )
                )

        else:
            # Direct migration mode (pure Airflow 3)
            for node in version_nodes:
                line_idx = node.lineno - 1
                end_line_idx = node.end_lineno if node.end_lineno else node.lineno
                orig_snippet = "\n".join(lines[line_idx:end_line_idx]) if line_idx < len(lines) else ""
                indent = re.match(r"^\s*", lines[line_idx]).group(0) if line_idx < len(lines) else ""

                symbols_by_dest: Dict[str, List[str]] = {}
                for a in node.names:
                    t_name = SYMBOL_MAPPINGS.get(a.name, a.name)
                    d_mod = SYMBOL_DEST_OVERRIDES.get(a.name, MODULE_MAPPINGS.get(node.module, node.module))
                    formatted = f"{t_name} as {a.asname}" if a.asname else t_name
                    symbols_by_dest.setdefault(d_mod, []).append(formatted)

                replacement_lines = []
                for dest_mod, sym_list in symbols_by_dest.items():
                    replacement_lines.append(f"{indent}from {dest_mod} import {', '.join(sym_list)}")
                suggested_code = "\n".join(replacement_lines)

                issues.append(
                    MigrationIssue(
                        rule_id=self.rule_id,
                        category=self.category,
                        severity=IssueSeverity.WARNING,
                        title=f"Mise à jour de l'import depuis '{node.module}'",
                        description="L'import a été mis à jour vers les providers standard / Task SDK d'Airflow 3.",
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

        return issues

    def fix(self, content: str, file_path: str) -> Tuple[str, List[MigrationIssue]]:
        issues = self.analyze(content, file_path)
        if not issues:
            return content, []

        lines = content.splitlines(keepends=True)
        applied: List[MigrationIssue] = []

        if self.dual_compat:
            dual_issue = next((i for i in issues if i.rule_id == "AIR302_DUAL_COMPAT" and i.auto_fixable), None)
            cleanup_issues = [i for i in issues if i.rule_id == "AIR302_DUAL_CLEANUP" and i.auto_fixable]

            if dual_issue:
                # 1. Clear out secondary nodes in reverse order
                for ci in sorted(cleanup_issues, key=lambda x: x.line_number, reverse=True):
                    s_idx = ci.line_number - 1
                    e_idx = ci.end_line_number if ci.end_line_number else ci.line_number
                    lines[s_idx:e_idx] = []
                    applied.append(ci)

                # 2. Replace primary node
                start_idx = dual_issue.line_number - 1
                end_idx = dual_issue.end_line_number if dual_issue.end_line_number else dual_issue.line_number
                lines[start_idx:end_idx] = [dual_issue.suggested_code + "\n\n"]
                applied.append(dual_issue)

                return "".join(lines), applied

        # Direct mode
        fixable_issues = sorted(
            [issue for issue in issues if issue.auto_fixable and issue.rule_id == self.rule_id],
            key=lambda x: x.line_number,
            reverse=True,
        )

        for issue in fixable_issues:
            start_idx = issue.line_number - 1
            end_idx = issue.end_line_number if issue.end_line_number else issue.line_number
            if start_idx < len(lines):
                has_newline = lines[min(end_idx - 1, len(lines) - 1)].endswith("\n")
                rep = issue.suggested_code + ("\n" if has_newline else "")
                lines[start_idx:end_idx] = [rep]
                applied.append(issue)

        return "".join(lines), applied
