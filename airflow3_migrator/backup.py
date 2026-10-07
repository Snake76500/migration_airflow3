"""
Backup and Rollback Manager for Airflow migration.
"""

import json
import shutil
import time
from pathlib import Path
from typing import List, Dict, Optional


class BackupManager:
    """Manages snapshots of files prior to migration for safe rollbacks."""

    def __init__(self, workspace_root: str):
        self.workspace_root = Path(workspace_root).resolve()
        self.backup_dir = self.workspace_root / ".airflow_migration_backups"

    def create_backup(self, files_to_backup: List[str]) -> Path:
        """
        Creates a timestamped snapshot of specified files.
        Returns the path to the created backup directory.
        """
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        snapshot_dir = self.backup_dir / f"backup_{timestamp}"
        snapshot_dir.mkdir(parents=True, exist_ok=True)

        manifest: Dict[str, any] = {
            "timestamp": timestamp,
            "created_at": time.asctime(),
            "files": [],
        }

        for file_str in files_to_backup:
            file_path = Path(file_str).resolve()
            if not file_path.exists():
                continue

            try:
                rel_path = file_path.relative_to(self.workspace_root)
            except ValueError:
                rel_path = Path(file_path.name)

            dest_path = snapshot_dir / rel_path
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(file_path, dest_path)

            manifest["files"].append({
                "original_path": str(file_path),
                "relative_path": str(rel_path),
            })

        with open(snapshot_dir / "manifest.json", "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)

        return snapshot_dir

    def list_backups(self) -> List[Dict[str, any]]:
        """Returns list of available backups with details."""
        if not self.backup_dir.exists():
            return []

        backups = []
        for item in sorted(self.backup_dir.iterdir(), reverse=True):
            if item.is_dir() and item.name.startswith("backup_"):
                manifest_path = item / "manifest.json"
                if manifest_path.exists():
                    try:
                        with open(manifest_path, "r", encoding="utf-8") as f:
                            data = json.load(f)
                            data["backup_dir"] = str(item)
                            backups.append(data)
                    except Exception:
                        pass
        return backups

    def restore_latest(self) -> Optional[int]:
        """Restores files from the most recent backup. Returns number of restored files."""
        backups = self.list_backups()
        if not backups:
            return None

        latest = backups[0]
        backup_path = Path(latest["backup_dir"])
        restored_count = 0

        for file_info in latest["files"]:
            orig = Path(file_info["original_path"])
            src = backup_path / file_info["relative_path"]
            if src.exists():
                orig.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src, orig)
                restored_count += 1

        return restored_count
