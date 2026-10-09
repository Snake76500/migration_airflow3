"""
Command Line Interface (CLI) for Airflow 3 Migration Tool.
"""

import argparse
import sys
from pathlib import Path
from typing import Optional

from .engine import MigrationEngine
from .reporter import MigrationReporter


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="airflow3-migrate",
        description="🚀 Outil complet d'analyse, de rapport et de migration d'Airflow 2 vers Airflow 3.",
    )

    subparsers = parser.add_subparsers(dest="command", help="Commandes disponibles")

    # Command: scan (dry run analysis)
    scan_parser = subparsers.add_parser("scan", help="Analyser le projet sans modifier les fichiers (Dry-run)")
    scan_parser.add_argument("path", nargs="?", default=".", help="Chemin du projet ou fichier à analyser (défaut: .)")
    scan_parser.add_argument("--no-diff", action="store_true", help="Masquer le diff de code dans la console")
    scan_parser.add_argument("--no-color", action="store_true", help="Désactiver les couleurs ANSI")
    scan_parser.add_argument(
        "--dual-compat",
        action="store_true",
        help="Mode bi-compatible Airflow 2 et Airflow 3 (génère des blocs 'if AIRFLOW_V_3_0_PLUS:')",
    )

    # Command: report (generate report files)
    report_parser = subparsers.add_parser("report", help="Générer un rapport complet (HTML, Markdown, JSON)")
    report_parser.add_argument("path", nargs="?", default=".", help="Chemin du projet ou fichier (défaut: .)")
    report_parser.add_argument(
        "--format",
        choices=["html", "md", "json", "all"],
        default="html",
        help="Format du rapport à générer (défaut: html)",
    )
    report_parser.add_argument(
        "--output-dir",
        default=".",
        help="Répertoire de sortie pour enregistrer les rapports (défaut: .)",
    )
    report_parser.add_argument(
        "--dual-compat",
        action="store_true",
        help="Mode bi-compatible Airflow 2 et Airflow 3 (génère des blocs 'if AIRFLOW_V_3_0_PLUS:')",
    )

    # Command: apply (apply fixes)
    apply_parser = subparsers.add_parser("apply", help="Appliquer les modifications automatiques au code")
    apply_parser.add_argument("path", nargs="?", default=".", help="Chemin du projet à modifier (défaut: .)")
    apply_parser.add_argument("--no-backup", action="store_true", help="Ne pas créer de sauvegarde avant modification")
    apply_parser.add_argument("--interactive", action="store_true", help="Demander confirmation avant d'appliquer")
    apply_parser.add_argument(
        "--dual-compat",
        action="store_true",
        help="Appliquer les modifications en mode bi-compatible Airflow 2 et Airflow 3 (if AIRFLOW_V_3_0_PLUS:)",
    )

    # Command: rollback (restore previous backup)
    rollback_parser = subparsers.add_parser("rollback", help="Restaurer la dernière sauvegarde de sécurité")
    rollback_parser.add_argument("path", nargs="?", default=".", help="Chemin du projet (défaut: .)")

    # Command: check (CI/CD mode)
    check_parser = subparsers.add_parser("check", help="Vérifier la conformité Airflow 3 (code retour > 0 si anomalie)")
    check_parser.add_argument("path", nargs="?", default=".", help="Chemin du projet (défaut: .)")
    check_parser.add_argument(
        "--dual-compat",
        action="store_true",
        help="Vérifier la conformité avec prise en compte du mode bi-compatible",
    )

    return parser


def handle_scan(args: argparse.Namespace) -> int:
    dual_compat = getattr(args, "dual_compat", False)
    engine = MigrationEngine(args.path, dual_compat=dual_compat)
    summary = engine.analyze()
    reporter = MigrationReporter(summary)
    reporter.print_console_summary(show_diffs=not args.no_diff, use_color=not args.no_color)
    return 0


def handle_report(args: argparse.Namespace) -> int:
    dual_compat = getattr(args, "dual_compat", False)
    engine = MigrationEngine(args.path, dual_compat=dual_compat)
    summary = engine.analyze()
    reporter = MigrationReporter(summary)
    out_dir = Path(args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    generated = []

    if args.format in ("html", "all"):
        html_file = out_dir / "migration_report.html"
        with open(html_file, "w", encoding="utf-8") as f:
            f.write(reporter.generate_html())
        generated.append(str(html_file))

    if args.format in ("md", "all"):
        md_file = out_dir / "migration_report.md"
        with open(md_file, "w", encoding="utf-8") as f:
            f.write(reporter.generate_markdown())
        generated.append(str(md_file))

    if args.format in ("json", "all"):
        json_file = out_dir / "migration_report.json"
        with open(json_file, "w", encoding="utf-8") as f:
            f.write(reporter.generate_json())
        generated.append(str(json_file))

    # Also display brief summary in console
    reporter.print_console_summary(show_diffs=False)

    print("\n📄 Rapports générés avec succès :")
    for p in generated:
        print(f"  • {p}")
    print()
    return 0


def handle_apply(args: argparse.Namespace) -> int:
    dual_compat = getattr(args, "dual_compat", False)
    engine = MigrationEngine(args.path, dual_compat=dual_compat)

    # First analyze to show what will be changed
    summary = engine.analyze()
    files_to_modify = [p for p in summary.plans if p.has_changes]

    if not files_to_modify:
        print("✓ Aucune modification automatique requise ou applicable.")
        return 0

    reporter = MigrationReporter(summary)
    reporter.print_console_summary(show_diffs=True)

    if args.interactive:
        confirm = input(f"\n❓ Souhaitez-vous appliquer ces modifications à {len(files_to_modify)} fichier(s) ? [O/n] : ")
        if confirm.strip().lower() not in ("o", "oui", "y", "yes", ""):
            print("Modification annulée par l'utilisateur.")
            return 0

    apply_summary = engine.apply(create_backup=not args.no_backup)
    print(f"\n✅ {apply_summary.files_modified} fichier(s) mis à jour avec succès pour Airflow 3 !")
    if apply_summary.backup_path:
        print(f"📦 Sauvegarde créée dans : {apply_summary.backup_path}")
        print("   (Pour annuler à tout moment : python migrate.py rollback)")
    print()
    return 0


def handle_rollback(args: argparse.Namespace) -> int:
    engine = MigrationEngine(args.path)
    restored = engine.rollback()
    if restored is None:
        print("❌ Aucune sauvegarde trouvée à restaurer.")
        return 1
    print(f"✅ Restauration effectuée avec succès : {restored} fichier(s) restaurés depuis la dernière sauvegarde.")
    return 0


def handle_check(args: argparse.Namespace) -> int:
    dual_compat = getattr(args, "dual_compat", False)
    engine = MigrationEngine(args.path, dual_compat=dual_compat)
    summary = engine.analyze()
    reporter = MigrationReporter(summary)
    reporter.print_console_summary(show_diffs=False)


    if summary.critical_issues > 0 or summary.warning_issues > 0:
        print(f"❌ Échec de la vérification : {summary.total_issues} anomalie(s) détectée(s).")
        return 1

    print("✅ Vérification réussie : Projet 100% compatible Airflow 3 !")
    return 0


def main(argv=None) -> int:
    parser = create_parser()
    args = parser.parse_args(argv)

    if not args.command:
        parser.print_help()
        return 0

    if args.command == "scan":
        return handle_scan(args)
    elif args.command == "report":
        return handle_report(args)
    elif args.command == "apply":
        return handle_apply(args)
    elif args.command == "rollback":
        return handle_rollback(args)
    elif args.command == "check":
        return handle_check(args)

    return 0


if __name__ == "__main__":
    sys.exit(main())
