#!/usr/bin/env python3
"""
Main orchestrator for the Algeria Administrative Divisions pipeline.

Pipeline steps:
  1. Scrape data from apcsali-adrar.dz (wilayas 1-58)
  2. Merge all sources (legacy + scraped + manual)
  3. Translate names to English
  4. Validate data integrity
  5. Export to CSV and SQL
  6. Update README stats
"""

import argparse
import json
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

# Import pipeline modules
from scraper import scrape
from merger import merge
from translator import translate_wilayas, translate_communes, translate_dairas
from validator import validate
from exporter import export

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
README_PATH = PROJECT_ROOT / "README.md"


def _save_json(data, path: Path):
    """Save data as JSON."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def update_readme_stats(wilayas: list[dict], communes: list[dict], dairas: list[dict]):
    """Update the README.md with current data statistics."""
    if not README_PATH.exists():
        logger.warning("README.md not found, skipping stats update")
        return

    readme = README_PATH.read_text(encoding="utf-8")

    # Calculate stats
    total_wilayas = len(wilayas)
    total_communes = len(communes)
    total_dairas = len(dairas)

    full = sum(1 for w in wilayas if w.get("data_completeness") == "full")
    partial = sum(1 for w in wilayas if w.get("data_completeness") == "partial")
    names_only = sum(1 for w in wilayas if w.get("data_completeness") == "names_only")

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # Replace stats markers in README
    stats_block = f"""<!-- STATS_START -->
| Metric | Count |
|--------|-------|
| Wilayas | {total_wilayas} |
| Communes | {total_communes} |
| Dairas | {total_dairas} |
| Full data | {full} wilayas |
| Partial data | {partial} wilayas |
| Names only | {names_only} wilayas |

> Last updated: {now}
<!-- STATS_END -->"""

    # Replace between markers, or append if markers not found
    pattern = r'<!-- STATS_START -->.*?<!-- STATS_END -->'
    if re.search(pattern, readme, re.DOTALL):
        readme = re.sub(pattern, stats_block, readme, flags=re.DOTALL)
    else:
        readme += "\n\n" + stats_block

    README_PATH.write_text(readme, encoding="utf-8")
    logger.info(f"Updated README.md with stats ({total_wilayas} wilayas, {total_communes} communes)")


def main():
    """Run the full pipeline."""
    # Fix Windows console encoding
    if sys.platform == "win32":
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(
        description="Algeria Administrative Divisions — Data Pipeline"
    )
    parser.add_argument(
        "--skip-scrape", action="store_true",
        help="Skip web scraping, use cached scraped data"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable verbose logging"
    )
    args = parser.parse_args()

    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    logger.info("=" * 60)
    logger.info("Algeria Administrative Divisions — Pipeline")
    logger.info("=" * 60)

    # ---------------------------------------------------------------
    # Step 1: Scrape
    # ---------------------------------------------------------------
    if args.skip_scrape:
        logger.info("\n📥 Step 1: SKIPPED (--skip-scrape)")
        scraped_path = PROJECT_ROOT / "data" / "raw" / "scraped_58.json"
        if not scraped_path.exists():
            logger.error("No cached scraped data found! Run without --skip-scrape first.")
            sys.exit(1)
    else:
        logger.info("\n📥 Step 1: Scraping apcsali-adrar.dz...")
        try:
            scrape()
        except Exception as e:
            logger.error(f"Scraping failed: {e}")
            logger.info("Attempting to continue with cached data...")
            scraped_path = PROJECT_ROOT / "data" / "raw" / "scraped_58.json"
            if not scraped_path.exists():
                logger.error("No cached data available. Aborting.")
                sys.exit(1)

    # ---------------------------------------------------------------
    # Step 2: Merge
    # ---------------------------------------------------------------
    logger.info("\n🔀 Step 2: Merging data sources...")
    wilayas, communes, dairas = merge()

    # ---------------------------------------------------------------
    # Step 3: Translate
    # ---------------------------------------------------------------
    logger.info("\n🌐 Step 3: Translating to English...")
    wilayas = translate_wilayas(wilayas)
    communes = translate_communes(communes)
    dairas = translate_dairas(dairas)

    # Save translated data back
    _save_json(wilayas, PROCESSED_DIR / "wilayas.json")
    _save_json(communes, PROCESSED_DIR / "communes.json")
    _save_json(dairas, PROCESSED_DIR / "dairas.json")

    # ---------------------------------------------------------------
    # Step 4: Validate
    # ---------------------------------------------------------------
    logger.info("\n✅ Step 4: Validating data...")
    report = validate(wilayas, communes, dairas)
    logger.info("\n" + report.summary())

    if not report.is_valid:
        logger.error("Validation FAILED! Check errors above.")
        # Don't exit — still export what we have, but warn
        logger.warning("Continuing with export despite validation errors...")

    # Save validation report
    report_path = PROCESSED_DIR / "validation_report.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report.to_dict(), f, ensure_ascii=False, indent=2)

    # ---------------------------------------------------------------
    # Step 5: Export
    # ---------------------------------------------------------------
    logger.info("\n📤 Step 5: Exporting to CSV and SQL...")
    export(wilayas, communes, dairas)

    # ---------------------------------------------------------------
    # Step 6: Update README
    # ---------------------------------------------------------------
    logger.info("\n📝 Step 6: Updating README stats...")
    update_readme_stats(wilayas, communes, dairas)

    # ---------------------------------------------------------------
    # Done
    # ---------------------------------------------------------------
    logger.info("\n" + "=" * 60)
    logger.info("✨ Pipeline complete!")
    logger.info(f"   Wilayas:  {len(wilayas)}")
    logger.info(f"   Communes: {len(communes)}")
    logger.info(f"   Dairas:   {len(dairas)}")
    logger.info(f"   Status:   {'✅ Valid' if report.is_valid else '⚠️ Has validation errors'}")
    logger.info("=" * 60)

    return 0 if report.is_valid else 1


if __name__ == "__main__":
    sys.exit(main())
