#!/usr/bin/env python3
"""
Airflow 2 to Airflow 3 Migration CLI.

Point d'entrée exécutable principal.
"""

import sys
from airflow3_migrator.cli import main

if __name__ == "__main__":
    sys.exit(main())
