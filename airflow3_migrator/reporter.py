"""
Comprehensive reporting for Airflow 2 -> Airflow 3 migration.
Generates Console (ANSI), Markdown, HTML (modern visual report), and JSON reports.
"""

import json
import html
from pathlib import Path
from typing import List, Optional
from .engine import MigrationSummary, FileMigrationPlan
from .rules.base import IssueSeverity


class ConsoleColor:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    WHITE = "\033[37m"
    BG_RED = "\033[41m"
    BG_GREEN = "\033[42m"


class NoColor:
    RESET = ""
    BOLD = ""
    DIM = ""
    RED = ""
    GREEN = ""
    YELLOW = ""
    BLUE = ""
    MAGENTA = ""
    CYAN = ""
    WHITE = ""


class MigrationReporter:
    """Generates detailed reports in multiple formats."""

    def __init__(self, summary: MigrationSummary):
        self.summary = summary

    def print_console_summary(self, show_diffs: bool = True, use_color: bool = True):
        """Prints a rich, formatted summary to standard output."""
        c = ConsoleColor if use_color else NoColor

        print(f"\n{c.BOLD}{c.CYAN}{'=' * 75}{c.RESET}")
        print(f"{c.BOLD}{c.CYAN} 🚀 RAPPORT D'ANALYSE DE MIGRATION AIRFLOW 2 ➔ AIRFLOW 3 {c.RESET}")
        print(f"{c.BOLD}{c.CYAN}{'=' * 75}{c.RESET}\n")

        # Metrics box
        print(f"{c.BOLD}Statistiques Générales :{c.RESET}")
        print(f"  • Fichiers scannés         : {c.BOLD}{self.summary.total_files_scanned}{c.RESET}")
        print(f"  • Fichiers avec anomalies  : {c.YELLOW}{self.summary.files_with_issues}{c.RESET}")
        print(f"  • Fichiers modifiés/à maj  : {c.GREEN}{self.summary.files_modified}{c.RESET}")
        print(f"  • Total anomalies détectées: {c.BOLD}{self.summary.total_issues}{c.RESET}")
        print(f"    - {c.RED}Critiques (Bloquants)   : {self.summary.critical_issues}{c.RESET}")
        print(f"    - {c.YELLOW}Avertissements (Règles) : {self.summary.warning_issues}{c.RESET}")
        print(f"    - {c.BLUE}Informations / Recomms  : {self.summary.info_issues}{c.RESET}")
        print(f"    - {c.GREEN}Corrections automatiques: {self.summary.auto_fixable_issues}{c.RESET}")
        print(f"    - {c.MAGENTA}Corrections manuelles   : {self.summary.manual_issues}{c.RESET}\n")

        if self.summary.backup_path:
            print(f"{c.GREEN}✓ Sauvegarde de sécurité créée dans : {self.summary.backup_path}{c.RESET}\n")

        if not self.summary.plans:
            print(f"{c.GREEN}✓ Félicitations ! Aucune anomalie Airflow 3 détectée dans ce projet.{c.RESET}\n")
            return

        print(f"{c.BOLD}{'-' * 75}{c.RESET}")
        print(f"{c.BOLD}Détails par fichier :{c.RESET}\n")

        for idx, plan in enumerate(self.summary.plans, start=1):
            status = f"{c.GREEN}[MODIFIABLE AUTO]{c.RESET}" if plan.has_changes else f"{c.YELLOW}[ACTION MANUELLE REQUISE]{c.RESET}"
            print(f"{c.BOLD}{idx}. {plan.relative_path}{c.RESET}  {status}")
            print(f"   Type: {plan.file_type} | {len(plan.issues)} problème(s)")

            for issue in plan.issues:
                sev_color = c.RED if issue.severity == IssueSeverity.CRITICAL else (c.YELLOW if issue.severity == IssueSeverity.WARNING else c.BLUE)
                fix_tag = f"{c.GREEN}[Auto-fixable]{c.RESET}" if issue.auto_fixable else f"{c.MAGENTA}[Manuel]{c.RESET}"
                print(f"     • {sev_color}[{issue.severity.value}]{c.RESET} Ligne {issue.line_number}: {issue.title} {fix_tag}")
                if issue.original_code.strip():
                    print(f"       {c.DIM}- Actuel  : {issue.original_code.strip()}{c.RESET}")
                if issue.auto_fixable and issue.suggested_code.strip():
                    print(f"       {c.GREEN}+ Proposé : {issue.suggested_code.strip()}{c.RESET}")

            if show_diffs and plan.diff:
                print(f"\n   {c.BOLD}Diff des modifications :{c.RESET}")
                for line in plan.diff.splitlines():
                    if line.startswith("+") and not line.startswith("+++"):
                        print(f"   {c.GREEN}{line}{c.RESET}")
                    elif line.startswith("-") and not line.startswith("---"):
                        print(f"   {c.RED}{line}{c.RESET}")
                    elif line.startswith("@"):
                        print(f"   {c.CYAN}{line}{c.RESET}")
                    else:
                        print(f"   {c.DIM}{line}{c.RESET}")
                print()
            else:
                print()

    def generate_json(self) -> str:
        """Returns JSON representation of the migration summary."""
        data = {
            "summary": {
                "total_files_scanned": self.summary.total_files_scanned,
                "files_with_issues": self.summary.files_with_issues,
                "files_modified": self.summary.files_modified,
                "total_issues": self.summary.total_issues,
                "critical_issues": self.summary.critical_issues,
                "warning_issues": self.summary.warning_issues,
                "info_issues": self.summary.info_issues,
                "auto_fixable_issues": self.summary.auto_fixable_issues,
                "manual_issues": self.summary.manual_issues,
                "backup_path": self.summary.backup_path,
            },
            "files": [
                {
                    "file_path": plan.file_path,
                    "relative_path": plan.relative_path,
                    "file_type": plan.file_type,
                    "has_changes": plan.has_changes,
                    "issues": [i.to_dict() for i in plan.issues],
                    "diff": plan.diff,
                }
                for plan in self.summary.plans
            ],
        }
        return json.dumps(data, indent=2, ensure_ascii=False)

    def generate_markdown(self) -> str:
        """Returns Markdown report formatted with tables and diff blocks."""
        md = []
        md.append("# 🚀 Rapport de Migration : Airflow 2 ➔ Airflow 3\n")
        md.append("Ce rapport récapitule les incompatibilités détectées dans votre projet et détaille les correctifs appliqués ou recommandés.\n")

        md.append("## 📊 Statistiques Générales\n")
        md.append("| Métrique | Valeur |")
        md.append("| :--- | :--- |")
        md.append(f"| Fichiers analysés | **{self.summary.total_files_scanned}** |")
        md.append(f"| Fichiers concernés | **{self.summary.files_with_issues}** |")
        md.append(f"| Fichiers modifiés / modifiables | **{self.summary.files_modified}** |")
        md.append(f"| Total des anomalies | **{self.summary.total_issues}** |")
        md.append(f"| 🚨 Critiques (Bloquants Airflow 3) | **{self.summary.critical_issues}** |")
        md.append(f"| ⚠️ Avertissements (Règles & Providers) | **{self.summary.warning_issues}** |")
        md.append(f"| ℹ️ Recommandations | **{self.summary.info_issues}** |")
        md.append(f"| ⚡ Auto-corrigibles | **{self.summary.auto_fixable_issues}** |")
        md.append(f"| 🛠️ Interventions manuelles | **{self.summary.manual_issues}** |\n")

        if self.summary.backup_path:
            md.append(f"> [!NOTE]\n> Une sauvegarde complète a été générée dans `{self.summary.backup_path}`.\n")

        md.append("## 🔍 Détail par Fichier\n")

        for plan in self.summary.plans:
            status_badge = "🟢 Prêt à appliquer" if plan.has_changes else "🟡 Action manuelle requise"
            md.append(f"### `{plan.relative_path}` ({status_badge})\n")
            md.append(f"- **Type de fichier** : `{plan.file_type}`")
            md.append(f"- **Nombre d'anomalies** : `{len(plan.issues)}`\n")

            md.append("| Règle | Sévérité | Ligne | Description | Auto-fix |")
            md.append("| :--- | :--- | :--- | :--- | :--- |")
            for iss in plan.issues:
                fixable = "Oui ✅" if iss.auto_fixable else "Non 🛠️"
                sev_icon = "🚨 Critique" if iss.severity == IssueSeverity.CRITICAL else ("⚠️ Warning" if iss.severity == IssueSeverity.WARNING else "ℹ️ Info")
                md.append(f"| `{iss.rule_id}` | {sev_icon} | L.{iss.line_number} | {iss.title} | {fixable} |")

            if plan.diff:
                md.append("\n**Différences proposées :**\n")
                md.append("```diff")
                md.append(plan.diff)
                md.append("```\n")
            md.append("\n---\n")

        md.append("## 📚 Guide des Changements Majeurs Airflow 3\n")
        md.append("1. **Déplacement vers `apache-airflow-providers-standard`** : Tous les opérateurs fondamentaux (`BashOperator`, `PythonOperator`, etc.) sont maintenant dans ce provider.")
        md.append("2. **Suppression de `DummyOperator`** : Remplacé obligatoirement par `EmptyOperator`.")
        md.append("3. **Suppression de `schedule_interval`** : Utilisez désormais le paramètre `schedule`.")
        md.append("4. **Suppression de `execution_date`** : Remplacé par `logical_date` ou `data_interval_start`.")
        md.append("5. **Suppression de `SubDagOperator`** : Les SubDAGs doivent être migrés vers des `TaskGroup`.")
        md.append("6. **Task Execution API & Isolation DB** : Les workers ne peuvent plus exécuter de requêtes ORM directes vers les métadonnées.\n")

        return "\n".join(md)

    def generate_html(self) -> str:
        """Generates a modern, interactive, and responsive HTML report with diff highlighting."""
        plans_html = []

        for idx, plan in enumerate(self.summary.plans, start=1):
            issues_rows = []
            for iss in plan.issues:
                badge_class = (
                    "badge-critical"
                    if iss.severity == IssueSeverity.CRITICAL
                    else ("badge-warning" if iss.severity == IssueSeverity.WARNING else "badge-info")
                )
                auto_badge = (
                    '<span class="badge badge-success">Auto-Fix</span>'
                    if iss.auto_fixable
                    else '<span class="badge badge-manual">Manuel</span>'
                )
                orig_code_snippet = (
                    f"<code>{html.escape(iss.original_code.strip())}</code>"
                    if iss.original_code.strip()
                    else "<em>N/A</em>"
                )
                sugg_code_snippet = (
                    f"<code>{html.escape(iss.suggested_code.strip())}</code>"
                    if iss.suggested_code.strip()
                    else "<em>N/A</em>"
                )

                issues_rows.append(f"""
                <tr>
                    <td><code>{html.escape(iss.rule_id)}</code></td>
                    <td><span class="badge {badge_class}">{iss.severity.value}</span></td>
                    <td><strong>L.{iss.line_number}</strong></td>
                    <td>
                        <div class="issue-title">{html.escape(iss.title)}</div>
                        <div class="issue-desc">{html.escape(iss.description)}</div>
                    </td>
                    <td>{orig_code_snippet}</td>
                    <td>{sugg_code_snippet}</td>
                    <td>{auto_badge}</td>
                </tr>
                """)

            diff_view = ""
            if plan.diff:
                diff_formatted_lines = []
                for line in plan.diff.splitlines():
                    escaped = html.escape(line)
                    if line.startswith("+") and not line.startswith("+++"):
                        diff_formatted_lines.append(f'<span class="diff-add">{escaped}</span>')
                    elif line.startswith("-") and not line.startswith("---"):
                        diff_formatted_lines.append(f'<span class="diff-del">{escaped}</span>')
                    elif line.startswith("@"):
                        diff_formatted_lines.append(f'<span class="diff-hunk">{escaped}</span>')
                    else:
                        diff_formatted_lines.append(f'<span class="diff-ctx">{escaped}</span>')

                diff_content = "\n".join(diff_formatted_lines)
                diff_view = f"""
                <div class="diff-container">
                    <div class="diff-header">Différences proposées (Airflow 2 ➔ Airflow 3)</div>
                    <pre class="diff-pre"><code>{diff_content}</code></pre>
                </div>
                """

            status_chip = (
                '<span class="status-chip ready">Prêt à migrer</span>'
                if plan.has_changes
                else '<span class="status-chip manual">Action Manuelle</span>'
            )

            plans_html.append(f"""
            <div class="file-card">
                <div class="file-card-header" onclick="toggleCard({idx})">
                    <div class="file-info">
                        <span class="file-icon">📄</span>
                        <span class="file-name">{html.escape(plan.relative_path)}</span>
                        <span class="file-type-pill">{html.escape(plan.file_type)}</span>
                    </div>
                    <div class="file-meta">
                        {status_chip}
                        <span class="issue-count">{len(plan.issues)} problème(s)</span>
                        <span class="chevron" id="chevron-{idx}">▼</span>
                    </div>
                </div>
                <div class="file-card-body" id="body-{idx}">
                    <div class="table-responsive">
                        <table class="issues-table">
                            <thead>
                                <tr>
                                    <th>Règle</th>
                                    <th>Sévérité</th>
                                    <th>Ligne</th>
                                    <th>Description</th>
                                    <th>Code Actuel</th>
                                    <th>Suggestion Airflow 3</th>
                                    <th>Type</th>
                                </tr>
                            </thead>
                            <tbody>
                                {"".join(issues_rows)}
                            </tbody>
                        </table>
                    </div>
                    {diff_view}
                </div>
            </div>
            """)

        html_template = f"""<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Rapport de Migration Airflow 3</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
    <style>
        :root {{
            --bg-primary: #0f172a;
            --bg-secondary: #1e293b;
            --bg-card: #1e293b;
            --bg-card-hover: #24344d;
            --border-color: #334155;
            --text-primary: #f8fafc;
            --text-secondary: #94a3b8;
            --text-muted: #64748b;
            --accent-blue: #38bdf8;
            --accent-green: #34d399;
            --accent-red: #f87171;
            --accent-yellow: #fbbf24;
            --accent-purple: #c084fc;
            --diff-add-bg: rgba(52, 211, 153, 0.15);
            --diff-add-color: #4ade80;
            --diff-del-bg: rgba(248, 113, 113, 0.15);
            --diff-del-color: #f87171;
            --diff-hunk-color: #38bdf8;
        }}

        * {{
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }}

        body {{
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            background-color: var(--bg-primary);
            color: var(--text-primary);
            line-height: 1.6;
            padding: 2rem 1.5rem;
        }}

        .container {{
            max-width: 1280px;
            margin: 0 auto;
        }}

        header {{
            margin-bottom: 2.5rem;
            border-bottom: 1px solid var(--border-color);
            padding-bottom: 1.5rem;
        }}

        .hero-title {{
            display: flex;
            align-items: center;
            gap: 1rem;
            font-size: 2.2rem;
            font-weight: 700;
            background: linear-gradient(135deg, #38bdf8, #818cf8, #c084fc);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }}

        .subtitle {{
            color: var(--text-secondary);
            font-size: 1.05rem;
            margin-top: 0.5rem;
        }}

        /* Metrics Grid */
        .metrics-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1rem;
            margin-bottom: 2.5rem;
        }}

        .metric-card {{
            background: var(--bg-secondary);
            border: 1px solid var(--border-color);
            border-radius: 12px;
            padding: 1.25rem;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.2);
            transition: transform 0.2s ease, border-color 0.2s ease;
        }}

        .metric-card:hover {{
            transform: translateY(-2px);
            border-color: var(--accent-blue);
        }}

        .metric-label {{
            color: var(--text-secondary);
            font-size: 0.85rem;
            font-weight: 500;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }}

        .metric-value {{
            font-size: 2.2rem;
            font-weight: 700;
            margin-top: 0.25rem;
        }}

        .val-total {{ color: var(--accent-blue); }}
        .val-critical {{ color: var(--accent-red); }}
        .val-warning {{ color: var(--accent-yellow); }}
        .val-fixable {{ color: var(--accent-green); }}

        /* Files Section */
        .section-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 1.25rem;
        }}

        .section-title {{
            font-size: 1.4rem;
            font-weight: 600;
        }}

        .file-card {{
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 10px;
            margin-bottom: 1.25rem;
            overflow: hidden;
            transition: border-color 0.2s;
        }}

        .file-card:hover {{
            border-color: #475569;
        }}

        .file-card-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 1rem 1.25rem;
            background: rgba(30, 41, 59, 0.8);
            cursor: pointer;
            user-select: none;
        }}

        .file-info {{
            display: flex;
            align-items: center;
            gap: 0.75rem;
        }}

        .file-icon {{
            font-size: 1.2rem;
        }}

        .file-name {{
            font-weight: 600;
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.95rem;
            color: var(--text-primary);
        }}

        .file-type-pill {{
            background: #334155;
            color: #cbd5e1;
            font-size: 0.75rem;
            padding: 0.15rem 0.5rem;
            border-radius: 6px;
        }}

        .file-meta {{
            display: flex;
            align-items: center;
            gap: 1rem;
        }}

        .status-chip {{
            font-size: 0.8rem;
            font-weight: 600;
            padding: 0.2rem 0.6rem;
            border-radius: 6px;
        }}

        .status-chip.ready {{
            background: rgba(52, 211, 153, 0.2);
            color: var(--accent-green);
            border: 1px solid rgba(52, 211, 153, 0.3);
        }}

        .status-chip.manual {{
            background: rgba(251, 191, 36, 0.2);
            color: var(--accent-yellow);
            border: 1px solid rgba(251, 191, 36, 0.3);
        }}

        .issue-count {{
            font-size: 0.85rem;
            color: var(--text-secondary);
        }}

        .chevron {{
            font-size: 0.75rem;
            color: var(--text-muted);
            transition: transform 0.2s ease;
        }}

        .file-card-body {{
            padding: 1.25rem;
            border-top: 1px solid var(--border-color);
        }}

        /* Table */
        .table-responsive {{
            overflow-x: auto;
            margin-bottom: 1.25rem;
        }}

        .issues-table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 0.88rem;
        }}

        .issues-table th {{
            text-align: left;
            padding: 0.6rem 0.75rem;
            background: #0f172a;
            color: var(--text-secondary);
            font-weight: 600;
            border-bottom: 1px solid var(--border-color);
        }}

        .issues-table td {{
            padding: 0.75rem;
            border-bottom: 1px solid #24344d;
            vertical-align: top;
        }}

        .issue-title {{
            font-weight: 600;
            color: var(--text-primary);
        }}

        .issue-desc {{
            font-size: 0.8rem;
            color: var(--text-secondary);
            margin-top: 0.2rem;
        }}

        /* Badges */
        .badge {{
            display: inline-block;
            font-size: 0.75rem;
            font-weight: 600;
            padding: 0.15rem 0.45rem;
            border-radius: 4px;
        }}

        .badge-critical {{ background: rgba(248, 113, 113, 0.2); color: var(--accent-red); border: 1px solid rgba(248, 113, 113, 0.4); }}
        .badge-warning {{ background: rgba(251, 191, 36, 0.2); color: var(--accent-yellow); border: 1px solid rgba(251, 191, 36, 0.4); }}
        .badge-info {{ background: rgba(56, 189, 248, 0.2); color: var(--accent-blue); border: 1px solid rgba(56, 189, 248, 0.4); }}
        .badge-success {{ background: rgba(52, 211, 153, 0.2); color: var(--accent-green); }}
        .badge-manual {{ background: rgba(192, 132, 252, 0.2); color: var(--accent-purple); }}

        code {{
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.82rem;
            background: #090d16;
            padding: 0.15rem 0.4rem;
            border-radius: 4px;
            color: #e2e8f0;
        }}

        /* Diff Section */
        .diff-container {{
            background: #090d16;
            border: 1px solid var(--border-color);
            border-radius: 8px;
            overflow: hidden;
            margin-top: 1rem;
        }}

        .diff-header {{
            background: #131d2e;
            padding: 0.5rem 1rem;
            font-size: 0.8rem;
            font-weight: 600;
            color: var(--text-secondary);
            border-bottom: 1px solid var(--border-color);
        }}

        .diff-pre {{
            margin: 0;
            padding: 0.75rem 1rem;
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.82rem;
            line-height: 1.5;
            overflow-x: auto;
        }}

        .diff-add {{ display: block; background: var(--diff-add-bg); color: var(--diff-add-color); }}
        .diff-del {{ display: block; background: var(--diff-del-bg); color: var(--diff-del-color); }}
        .diff-hunk {{ display: block; color: var(--diff-hunk-color); font-weight: 600; }}
        .diff-ctx {{ display: block; color: var(--text-secondary); }}

        footer {{
            margin-top: 3rem;
            text-align: center;
            color: var(--text-muted);
            font-size: 0.85rem;
            border-top: 1px solid var(--border-color);
            padding-top: 1.5rem;
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <div class="hero-title">
                <span>🚀</span>
                <span>Airflow 2 ➔ Airflow 3 Migration</span>
            </div>
            <p class="subtitle">Analyse automatique du code, des DAGs et de la configuration du projet</p>
        </header>

        <section class="metrics-grid">
            <div class="metric-card">
                <div class="metric-label">Fichiers Scannés</div>
                <div class="metric-value val-total">{self.summary.total_files_scanned}</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Fichiers à Mettre à Jour</div>
                <div class="metric-value val-total">{self.summary.files_with_issues}</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Anomalies Bloquantes</div>
                <div class="metric-value val-critical">{self.summary.critical_issues}</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Avertissements</div>
                <div class="metric-value val-warning">{self.summary.warning_issues}</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Auto-Corrigibles</div>
                <div class="metric-value val-fixable">{self.summary.auto_fixable_issues}</div>
            </div>
        </section>

        <section>
            <div class="section-header">
                <h2 class="section-title">Analyse détaillée par fichier</h2>
            </div>
            {"".join(plans_html) if plans_html else '<div class="metric-card">✓ Aucun problème détecté !</div>'}
        </section>

        <footer>
            Généré automatiquement par l'outil de migration Airflow 3.
        </footer>
    </div>

    <script>
        function toggleCard(idx) {{
            const body = document.getElementById('body-' + idx);
            const chevron = document.getElementById('chevron-' + idx);
            if (body.style.display === 'none') {{
                body.style.display = 'block';
                chevron.style.transform = 'rotate(0deg)';
            }} else {{
                body.style.display = 'none';
                chevron.style.transform = 'rotate(-90deg)';
            }}
        }}
    </script>
</body>
</html>
"""
        return html_template
