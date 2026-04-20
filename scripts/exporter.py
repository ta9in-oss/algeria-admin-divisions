#!/usr/bin/env python3
"""
Exporter for Algeria administrative divisions data.

Generates export formats:
  - CSV: wilayas.csv, communes.csv, dairas.csv
  - SQL: algeria_divisions.sql (SQLite/PostgreSQL compatible)
"""

import csv
import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
EXPORTS_DIR = PROJECT_ROOT / "data" / "exports"


def _load_json(path: Path) -> list[dict]:
    """Load a JSON file."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _escape_sql(value: str) -> str:
    """Escape a string for SQL insertion."""
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


# ---------------------------------------------------------------------------
# CSV Export
# ---------------------------------------------------------------------------

def export_csv(wilayas: list[dict], communes: list[dict], dairas: list[dict]):
    """Export all data to CSV files."""
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

    # --- Wilayas CSV ---
    wilaya_fields = [
        "code", "name_ar", "name_fr", "name_en",
        "phone", "fax", "email", "website", "address_ar",
        "created_year", "parent_wilaya", "data_completeness", "source", "last_updated",
    ]
    wilayas_path = EXPORTS_DIR / "wilayas.csv"
    with open(wilayas_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=wilaya_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(wilayas)
    logger.info(f"  Exported {len(wilayas)} wilayas → {wilayas_path}")

    # --- Communes CSV ---
    commune_fields = [
        "code", "name_ar", "name_fr", "name_en",
        "wilaya_code", "wilaya_name_ar", "daira_name_ar",
        "phone", "fax", "email", "website", "address_ar", "source",
    ]
    communes_path = EXPORTS_DIR / "communes.csv"
    with open(communes_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=commune_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(communes)
    logger.info(f"  Exported {len(communes)} communes → {communes_path}")

    # --- Dairas CSV ---
    daira_fields = [
        "name_ar", "name_fr", "name_en",
        "wilaya_code", "phone", "fax", "email", "communes", "source",
    ]
    dairas_path = EXPORTS_DIR / "dairas.csv"
    with open(dairas_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=daira_fields, extrasaction="ignore")
        writer.writeheader()
        for d in dairas:
            row = dict(d)
            # Convert communes list to comma-separated string
            row["communes"] = ",".join(row.get("communes", []))
            writer.writerow(row)
    logger.info(f"  Exported {len(dairas)} dairas → {dairas_path}")


# ---------------------------------------------------------------------------
# SQL Export
# ---------------------------------------------------------------------------

def export_sql(wilayas: list[dict], communes: list[dict], dairas: list[dict]):
    """Export all data to a SQL file (SQLite/PostgreSQL compatible)."""
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    sql_path = EXPORTS_DIR / "algeria_divisions.sql"

    lines = [
        "-- Algeria Administrative Divisions Database",
        "-- Auto-generated — do not edit manually",
        "--",
        "-- Compatible with SQLite and PostgreSQL",
        "",
        "-- =========================================",
        "-- Drop existing tables (if re-importing)",
        "-- =========================================",
        "DROP TABLE IF EXISTS commune_daira;",
        "DROP TABLE IF EXISTS communes;",
        "DROP TABLE IF EXISTS dairas;",
        "DROP TABLE IF EXISTS wilayas;",
        "",
        "-- =========================================",
        "-- Wilayas",
        "-- =========================================",
        "CREATE TABLE wilayas (",
        "    code            TEXT PRIMARY KEY,",
        "    name_ar         TEXT NOT NULL,",
        "    name_fr         TEXT,",
        "    name_en         TEXT,",
        "    phone           TEXT,",
        "    fax             TEXT,",
        "    email           TEXT,",
        "    website         TEXT,",
        "    address_ar      TEXT,",
        "    created_year    INTEGER,",
        "    parent_wilaya   TEXT REFERENCES wilayas(code),",
        "    data_completeness TEXT,",
        "    source          TEXT,",
        "    last_updated    TEXT",
        ");",
        "",
    ]

    for w in wilayas:
        vals = ", ".join([
            _escape_sql(w.get("code")),
            _escape_sql(w.get("name_ar")),
            _escape_sql(w.get("name_fr")),
            _escape_sql(w.get("name_en")),
            _escape_sql(w.get("phone")),
            _escape_sql(w.get("fax")),
            _escape_sql(w.get("email")),
            _escape_sql(w.get("website")),
            _escape_sql(w.get("address_ar")),
            str(w.get("created_year")) if w.get("created_year") else "NULL",
            _escape_sql(w.get("parent_wilaya")) if w.get("parent_wilaya") else "NULL",
            _escape_sql(w.get("data_completeness")),
            _escape_sql(w.get("source")),
            _escape_sql(w.get("last_updated")),
        ])
        lines.append(f"INSERT INTO wilayas VALUES ({vals});")

    lines.extend([
        "",
        "-- =========================================",
        "-- Communes",
        "-- =========================================",
        "CREATE TABLE communes (",
        "    code            TEXT PRIMARY KEY,",
        "    name_ar         TEXT NOT NULL,",
        "    name_fr         TEXT,",
        "    name_en         TEXT,",
        "    wilaya_code     TEXT NOT NULL REFERENCES wilayas(code),",
        "    wilaya_name_ar  TEXT,",
        "    daira_name_ar   TEXT,",
        "    phone           TEXT,",
        "    fax             TEXT,",
        "    email           TEXT,",
        "    website         TEXT,",
        "    address_ar      TEXT,",
        "    source          TEXT",
        ");",
        "",
    ])

    for c in communes:
        vals = ", ".join([
            _escape_sql(c.get("code")),
            _escape_sql(c.get("name_ar")),
            _escape_sql(c.get("name_fr")),
            _escape_sql(c.get("name_en")),
            _escape_sql(c.get("wilaya_code")),
            _escape_sql(c.get("wilaya_name_ar")),
            _escape_sql(c.get("daira_name_ar")),
            _escape_sql(c.get("phone")),
            _escape_sql(c.get("fax")),
            _escape_sql(c.get("email")),
            _escape_sql(c.get("website")),
            _escape_sql(c.get("address_ar")),
            _escape_sql(c.get("source")),
        ])
        lines.append(f"INSERT INTO communes VALUES ({vals});")

    lines.extend([
        "",
        "-- =========================================",
        "-- Dairas",
        "-- =========================================",
        "CREATE TABLE dairas (",
        "    id              INTEGER PRIMARY KEY AUTOINCREMENT,",
        "    name_ar         TEXT NOT NULL,",
        "    name_fr         TEXT,",
        "    name_en         TEXT,",
        "    wilaya_code     TEXT NOT NULL REFERENCES wilayas(code),",
        "    phone           TEXT,",
        "    fax             TEXT,",
        "    email           TEXT,",
        "    source          TEXT",
        ");",
        "",
    ])

    for i, d in enumerate(dairas, 1):
        vals = ", ".join([
            str(i),
            _escape_sql(d.get("name_ar")),
            _escape_sql(d.get("name_fr")),
            _escape_sql(d.get("name_en")),
            _escape_sql(d.get("wilaya_code")),
            _escape_sql(d.get("phone")),
            _escape_sql(d.get("fax")),
            _escape_sql(d.get("email")),
            _escape_sql(d.get("source")),
        ])
        lines.append(f"INSERT INTO dairas VALUES ({vals});")

    # Junction table for daira-commune relationship
    lines.extend([
        "",
        "-- =========================================",
        "-- Daira-Commune mapping",
        "-- =========================================",
        "CREATE TABLE commune_daira (",
        "    daira_id        INTEGER REFERENCES dairas(id),",
        "    commune_code    TEXT REFERENCES communes(code),",
        "    PRIMARY KEY (daira_id, commune_code)",
        ");",
        "",
    ])

    for i, d in enumerate(dairas, 1):
        for cc in d.get("communes", []):
            lines.append(
                f"INSERT INTO commune_daira VALUES ({i}, {_escape_sql(cc)});"
            )

    lines.append("")
    lines.append("-- End of file")

    with open(sql_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    logger.info(f"  Exported SQL → {sql_path}")


# ---------------------------------------------------------------------------
# Main export function
# ---------------------------------------------------------------------------

def export(wilayas: list[dict] | None = None,
           communes: list[dict] | None = None,
           dairas: list[dict] | None = None):
    """
    Run all exports.

    If no data is passed, loads from data/processed/*.json.
    """
    if wilayas is None:
        wilayas = _load_json(PROCESSED_DIR / "wilayas.json")
    if communes is None:
        communes = _load_json(PROCESSED_DIR / "communes.json")
    if dairas is None:
        dairas = _load_json(PROCESSED_DIR / "dairas.json")

    logger.info("Exporting data...")
    export_csv(wilayas, communes, dairas)
    export_sql(wilayas, communes, dairas)
    logger.info("Export complete!")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    export()
