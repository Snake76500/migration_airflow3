"""
Project file scanner for Airflow projects.
"""

import os
from pathlib import Path
from typing import List, Set, Dict
from enum import Enum


class FileType(str, Enum):
    PYTHON_DAG = "python_dag"
    PYTHON_MODULE = "python_module"
    CONFIG = "config"
    DEPENDENCY = "dependency"
    OTHER = "other"


IGNORE_DIRS = {
    ".git",
    "__pycache__",
    ".pytest_cache",
    ".venv",
    "venv",
    "env",
    ".idea",
    ".vscode",
    "node_modules",
    "dist",
    "build",
    ".tox",
    ".mypy_cache",
    ".ruff_cache",
}

DEPENDENCY_FILENAMES = {
    "requirements.txt",
    "requirements-dev.txt",
    "pyproject.toml",
    "pipfile",
    "setup.py",
}


class ProjectScanner:
    """Scans project directory and categorizes files to be migrated."""

    def __init__(self, root_path: str, custom_ignore: Set[str] = None):
        self.root_path = Path(root_path).resolve()
        self.ignore_dirs = set(IGNORE_DIRS)
        if custom_ignore:
            self.ignore_dirs.update(custom_ignore)

    def scan(self) -> List[Dict[str, any]]:
        """
        Scans directory tree and returns list of candidate files.
        Each file entry contains path, file_type, and size.
        """
        scanned_files = []

        if self.root_path.is_file():
            # Single file scan
            file_type = self._categorize_file(self.root_path)
            scanned_files.append({
                "path": str(self.root_path),
                "type": file_type,
                "relative_path": self.root_path.name,
                "size": self.root_path.stat().st_size,
            })
            return scanned_files

        for root, dirs, files in os.walk(self.root_path):
            # Modify dirs in-place to avoid scanning ignored directories
            dirs[:] = [d for d in dirs if d not in self.ignore_dirs and not d.startswith(".")]

            for filename in files:
                file_path = Path(root) / filename
                if file_path.is_symlink() and not file_path.exists():
                    continue

                file_type = self._categorize_file(file_path)
                if file_type != FileType.OTHER:
                    try:
                        rel_path = file_path.relative_to(self.root_path)
                    except ValueError:
                        rel_path = file_path

                    scanned_files.append({
                        "path": str(file_path),
                        "type": file_type,
                        "relative_path": str(rel_path),
                        "size": file_path.stat().st_size,
                    })

        return scanned_files

    def _categorize_file(self, file_path: Path) -> FileType:
        name = file_path.name.lower()

        # Dependencies
        if name in DEPENDENCY_FILENAMES or (name.startswith("requirements") and name.endswith(".txt")):
            return FileType.DEPENDENCY

        # Configs
        if name in ("airflow.cfg", ".env", "docker-compose.yml", "docker-compose.yaml"):
            return FileType.CONFIG

        # Python files
        if name.endswith(".py"):
            # Check if likely a DAG
            if self._is_likely_dag(file_path):
                return FileType.PYTHON_DAG
            return FileType.PYTHON_MODULE

        return FileType.OTHER

    def _is_likely_dag(self, file_path: Path) -> bool:
        """Heuristic check to identify if a Python file defines Airflow DAGs."""
        try:
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                head = f.read(4096)
                if any(k in head for k in ("DAG(", "@dag", "airflow", "BaseOperator", "TaskGroup")):
                    return True
        except Exception:
            pass
        return False
