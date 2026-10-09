# 🚀 Airflow 2 ➔ Airflow 3 Migrator

Un outil complet en Python conçu pour scanner un projet Apache Airflow complet, détecter toutes les incompatibilités avec **Airflow 3**, générer des rapports détaillés (Console, HTML interactif, Markdown, JSON) et **appliquer automatiquement les modifications au code avec sauvegarde et rollback instantané**.

---

## 📋 Table des matières
1. [Fonctionnalités principales](#-fonctionnalités-principales)
2. [Règles de migration prises en charge](#-règles-de-migration-prises-en-charge)
3. [Installation](#-installation)
4. [Guide d'utilisation](#-guide-dutilisation)
   - [1. Analyser un projet (Dry-run)](#1-analyser-un-projet-dry-run)
   - [2. Générer des rapports (HTML, MD, JSON)](#2-générer-des-rapports)
   - [3. Appliquer les modifications](#3-appliquer-les-modifications)
   - [4. Restaurer une sauvegarde (Rollback)](#4-restaurer-une-sauvegarde)
   - [5. Intégration CI/CD (Check)](#5-intégration-cicd)
5. [Structure du projet](#-structure-du-projet)
6. [Tests et validation](#-tests-et-validation)

---

## 🌟 Fonctionnalités principales

- **Scan récursif intelligent** : Détecte les DAGs, modules Python, configurations (`airflow.cfg`), fichiers de variables (`.env`, `docker-compose.yml`) et dépendances (`requirements.txt`, `pyproject.toml`).
- **Analyse syntaxique (AST + Regex Contextuelle)** : Préserve les commentaires, le formatage, l'indentation et les docstrings sans corruption de code.
- **Diff unifié en direct** : Visualisation colorée instantanée dans le terminal des lignes modifiées avant/après (`+`/`-`).
- **Rapports multi-formats** :
  - **Console ANSI** : Synthèse immédiate avec indicateurs visuels et compteurs de sévérité.
  - **HTML interactif** : Tableau de bord moderne avec cartes de métriques, tableaux dépliables et diffs syntaxiques.
  - **Markdown & JSON** : Idéal pour les pull requests GitHub/GitLab et l'automatisation.
- **Sécurité et Rollback** : Création automatique d'un snapshot horodaté (`.airflow_migration_backups/`) avant toute écriture sur le disque. Possibilité de revenir en arrière en une commande.
- **Zéro dépendance obligatoire** : Développé entièrement avec la bibliothèque standard Python 3.9+ (fonctionne immédiatement sur n'importe quel conteneur ou machine sans installer 50 paquets).

---

## 🧩 Règles de migration prises en charge

| ID Règle | Catégorie | Description & Comportement Airflow 3 | Auto-fix |
| :--- | :--- | :--- | :---: |
| **`AIR302`** | Imports & Providers | Redirection des opérateurs standards (`BashOperator`, `PythonOperator`, etc.) vers `apache-airflow-providers-standard`. | ✅ Oui |
| **`AIR301_DUMMY`** | Opérateurs | Remplacement obligatoire de `DummyOperator` par `EmptyOperator`. | ✅ Oui |
| **`AIR301_PROVIDE_CTX`** | Opérateurs | Suppression du paramètre obsolète `provide_context=True` dans `PythonOperator`. | ✅ Oui |
| **`AIR304`** | DAGs | Remplacement de `schedule_interval=` par `schedule=` dans `DAG(...)` et `@dag(...)`. | ✅ Oui |
| **`AIR305`** | Variables / Jinja | Remplacement de `execution_date` par `logical_date` et `next/prev_execution_date` par `data_interval_end/start` (Python + templates Jinja `{{ ... }}`). | ✅ Oui |
| **`AIR306`** | Datasets / Assets | Migration de `Dataset(...)` vers `Asset(...)` (`airflow.sdk`). | ✅ Oui |
| **`AIR308_CFG`** | Configuration | Remplacement de `SequentialExecutor` par `LocalExecutor`, suppression sécurisée de `enable_xcom_pickling`. | ✅ Oui |
| **`AIR309`** | Dépendances | Mise à jour de `apache-airflow~=3.1.0` et ajout de `apache-airflow-providers-standard>=1.0.0` dans `requirements.txt`. | ✅ Oui |
| **`AIR310_PRODUCT_ACTION`** | Décorateur | Migration de `@product_action` avec calcul dynamique de `action_id` (`Path(__file__).stem`) et ajout des imports `bp2i_airflow_library`. | ✅ Oui |
| **`SQLA20`** | SQLAlchemy 2.0 | Migration vers SQLAlchemy 2.0 (`execute(text(...))`, `from sqlalchemy.orm import declarative_base`, suppression `Engine.execute()`). | ✅ Oui |
| **`AIR301_SUBDAG`** | Architecture | Détection critique de `SubDagOperator` (supprimé dans Airflow 3) avec guide de migration vers `TaskGroup`. | 🛠️ Manuel |
| **`AIR307`** | Task Execution API | Détection d'accès direct à la base de métadonnées interne Airflow (`provide_session`, `session.query(DagRun)`). Les accès aux bases privées/métier restent autorisés. | 🛠️ Manuel |

---

## 📥 Installation

```bash
# Cloner ou se placer dans le projet
cd migration_airflow3

# Option 1 : Utilisation directe (aucun paquet requis)
python3 migrate.py --help

# Option 2 : Installation dans l'environnement Python
pip install .
```

---

## 💡 Guide d'utilisation

### 1. Analyser un projet (Dry-run)
Analyse vos fichiers sans rien modifier et affiche les anomalies ainsi que les diffs dans le terminal :
```bash
python3 migrate.py scan /chemin/vers/votre/projet
```

Options utiles :
- `--no-diff` : Affiche uniquement la liste des anomalies sans les blocs diff.
- `--no-color` : Désactive les couleurs ANSI pour les logs bruts.

---

### 2. Générer des rapports
Génère des rapports détaillés dans le répertoire de votre choix :
```bash
# Générer le rapport HTML interactif
python3 migrate.py report /chemin/vers/votre/projet --format html --output-dir ./reports

# Générer tous les formats (HTML, Markdown, JSON)
python3 migrate.py report /chemin/vers/votre/projet --format all --output-dir ./reports
```

Le fichier `reports/migration_report.html` peut être ouvert directement dans votre navigateur web pour présenter un audit complet.

---

### 3. Appliquer les modifications
Applique automatiquement les correctifs de code sur l'ensemble de votre projet avec création automatique d'une sauvegarde de sécurité :
```bash
python3 migrate.py apply /chemin/vers/votre/projet
```

Options utiles :
- `--dual-compat` : **Active le mode bi-compatible (Airflow 2 & 3)**. Génère des blocs `if AIRFLOW_V_3_0_PLUS:` pour les imports et adapte le code (`schedule`, `execution_date`/`logical_date`) pour s'exécuter sur les deux versions simultanément.
- `--interactive` : Demande confirmation avant d'appliquer.
- `--no-backup` : Désactive la sauvegarde automatique.

---

### 🌟 Mode Bi-Compatible Airflow 2 & Airflow 3 (`--dual-compat`)

Pour les projets en cours de transition devant **s'exécuter à la fois sur un cluster Airflow 2 et un cluster Airflow 3**, utilisez le flag `--dual-compat` :

```bash
# Analyse en mode bi-compatible
python3 migrate.py scan /chemin/vers/projet --dual-compat

# Application des modifications bi-compatibles
python3 migrate.py apply /chemin/vers/projet --dual-compat
```

#### Ce que le mode `--dual-compat` génère dans vos DAGs :
1. **Bloc d'imports conditionnel unifié en tête de fichier** :
```python
try:
    from bp2i_airflow_library.version_compat import AIRFLOW_V_3_0_PLUS
except ImportError:
    from airflow.version import version
    AIRFLOW_V_3_0_PLUS = version.startswith("3")

if AIRFLOW_V_3_0_PLUS:
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.standard.operators.python import PythonOperator
    from airflow.providers.standard.operators.empty import EmptyOperator, EmptyOperator as DummyOperator
    from airflow.sdk import Asset, Asset as Dataset, get_current_context
    from sqlalchemy.orm import declarative_base
else:
    from airflow.operators.bash_operator import BashOperator
    from airflow.operators.python_operator import PythonOperator
    from airflow.operators.dummy_operator import DummyOperator, DummyOperator as EmptyOperator
    from airflow.datasets import Dataset, Dataset as Asset
    from airflow.operators.python import get_current_context
    from sqlalchemy.ext.declarative import declarative_base
```

2. **Différences dans le code gérées dynamiquement** :
- **Paramètre schedule / schedule_interval** :
  ```python
  **({"schedule": "0 2 * * *"} if AIRFLOW_V_3_0_PLUS else {"schedule_interval": "0 2 * * *"})
  ```
- **Variables de contexte `execution_date` / `logical_date`** :
  ```python
  exec_dt = (kwargs["logical_date"] if AIRFLOW_V_3_0_PLUS else kwargs["execution_date"])
  ```
- **Décorateur `@product_action`** :
  ```python
  action_id=(
      Path(__file__).stem.replace(".v1.", ".v2")
      if AIRFLOW_V_3_0_PLUS and not ENVIRONMENT.endswith("prod")
      else Path(__file__).stem
  )
  ```
- **Alias bidirectionnels** : `EmptyOperator` et `DummyOperator`, `Asset` et `Dataset` sont déclarés dans les deux branches pour éviter tout `NameError`.

---

### 4. Restaurer une sauvegarde (Rollback)
Si vous souhaitez annuler la dernière migration appliquée :
```bash
python3 migrate.py rollback /chemin/vers/votre/projet
```
Tous les fichiers modifiés sont instantanément restaurés à leur état d'origine.

---

### 5. Intégration CI/CD (Check)
Pour valider automatiquement dans votre pipeline GitHub Actions, GitLab CI ou pre-commit qu'un projet est compatible Airflow 3 :
```bash
# Vérification Airflow 3 standard
python3 migrate.py check /chemin/vers/votre/projet

# Vérification avec tolérance pour le code bi-compatible
python3 migrate.py check /chemin/vers/votre/projet --dual-compat
```
- Renvoie **`0`** si le projet est conforme.
- Renvoie **`1`** si des incompatibilités sont détectées.


---

## 📂 Structure du projet

```
migration_airflow3/
├── airflow3_migrator/
│   ├── __init__.py
│   ├── cli.py                  # Interface en ligne de commande (CLI)
│   ├── scanner.py              # Explorateur de fichiers & détection des DAGs
│   ├── engine.py               # Moteur d'analyse & orchestration des diffs
│   ├── reporter.py             # Générateur de rapports (Terminal, HTML, MD, JSON)
│   ├── backup.py               # Gestionnaire de snapshots & rollback
│   └── rules/                  # Règles modulaires de migration
│       ├── __init__.py
│       ├── base.py             # Classe abstraite & modèles de données
│       ├── imports.py          # Redirection imports standard & providers
│       ├── operators.py        # DummyOperator -> EmptyOperator, provide_context
│       ├── dag_params.py       # schedule_interval -> schedule, SLA
│       ├── context_vars.py     # execution_date -> logical_date (Code & Jinja)
│       ├── datasets.py         # Dataset -> Asset (Airflow 3 Task SDK)
│       ├── db_access.py        # Détection isolation Task Execution API
│       ├── config_rules.py     # airflow.cfg, exécuteurs, pickling XCom
│       └── dependencies.py     # requirements.txt, pyproject.toml
├── tests/                      # Suite de tests unitaires et d'intégration
│   ├── fixtures/               # Exemples de DAGs Airflow 2, configs et requirements
│   ├── test_rules.py
│   ├── test_engine.py
│   └── test_reporter.py
├── migrate.py                  # Point d'entrée script exécutable
├── pyproject.toml
└── README.md
```

---

## 🧪 Tests et validation

Pour exécuter l'ensemble de la suite de tests :
```bash
python3 -m unittest discover tests
```
Résultat : **17 tests unitaires et d'intégration réussis (100% passants)**.
