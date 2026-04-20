#!/usr/bin/env python3
"""
Scraper for Algeria commune data from apcsali-adrar.dz.

Scrapes commune information (codes, AR/FR names, fax numbers) for all 58 wilayas
from https://apcsali-adrar.dz/communes/{1-58}/wilaya.
"""

import json
import os
import re
import time
import logging
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_URL = "https://apcsali-adrar.dz/communes/{code}/wilaya"
WILAYA_RANGE = range(1, 59)  # 1 to 58 inclusive
REQUEST_DELAY = 1.5          # seconds between requests
MAX_RETRIES = 3
RETRY_BACKOFF = 2.0          # exponential backoff multiplier
REQUEST_TIMEOUT = 30         # seconds

# Paths (relative to project root)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
CACHE_DIR = RAW_DIR / "cache"
OUTPUT_FILE = RAW_DIR / "scraped_58.json"

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def ensure_dirs():
    """Create necessary directories if they don't exist."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)


def fetch_page(wilaya_code: int) -> str | None:
    """
    Fetch a wilaya page with retry logic.
    Returns HTML string or None on failure.
    """
    url = BASE_URL.format(code=wilaya_code)
    cache_file = CACHE_DIR / f"wilaya_{wilaya_code:02d}.html"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(f"  Fetching wilaya {wilaya_code:02d} (attempt {attempt}/{MAX_RETRIES})...")
            response = requests.get(url, timeout=REQUEST_TIMEOUT, headers={
                "User-Agent": "AlgeriaAdminDivisions/1.0 (data collection)",
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "ar,fr;q=0.9,en;q=0.8",
            })
            response.raise_for_status()
            html = response.text

            # Cache the successful response
            cache_file.write_text(html, encoding="utf-8")
            logger.info(f"  ✓ Cached wilaya {wilaya_code:02d}")
            return html

        except requests.RequestException as e:
            logger.warning(f"  ✗ Attempt {attempt} failed for wilaya {wilaya_code:02d}: {e}")
            if attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF ** attempt
                logger.info(f"    Retrying in {wait:.1f}s...")
                time.sleep(wait)

    # All retries failed – try cache
    if cache_file.exists():
        logger.warning(f"  ⚠ Using cached HTML for wilaya {wilaya_code:02d}")
        return cache_file.read_text(encoding="utf-8")

    logger.error(f"  ✗✗ Failed to fetch wilaya {wilaya_code:02d} and no cache available")
    return None


def parse_wilaya_page(html: str, wilaya_code: int) -> dict:
    """
    Parse a wilaya page and extract commune data.

    Returns:
        {
            "code": "01",
            "name_ar": "أدرار",
            "communes": [
                {
                    "code": "0101",
                    "name_ar": "أدرار",
                    "name_fr": "ADRAR",
                    "fax": "(049) 36 - 78 - 27"
                },
                ...
            ]
        }
    """
    soup = BeautifulSoup(html, "html.parser")

    # --- Extract wilaya Arabic name from sidebar ---
    wilaya_name_ar = ""
    # Look for the selected link in the sidebar
    selected_link = soup.select_one('a.blockLink.is-selected')
    if selected_link:
        wilaya_name_ar = selected_link.get_text(strip=True)
    else:
        # Fallback: try to find from header
        header = soup.select_one('h3.block-header')
        if header:
            text = header.get_text(strip=True)
            # Extract wilaya name from "بلديات ولاية أدرار و عددها 16 بلدية"
            match = re.search(r'بلديات ولاية (.+?) و عددها', text)
            if match:
                wilaya_name_ar = match.group(1).strip()

    # --- Extract communes ---
    communes = []
    rows = soup.select('li.block-row.block-row--separated')

    for row in rows:
        commune = {}

        # Commune code: from span.contentRow-figure
        figure = row.select_one('span.contentRow-figure')
        if figure:
            # Text is like " 0101" (with icon text before)
            text = figure.get_text(strip=True)
            # Extract 4-digit code
            code_match = re.search(r'(\d{4})', text)
            if code_match:
                commune["code"] = code_match.group(1)

        # Commune Arabic name: from h3.contentRow-header
        header = row.select_one('h3.contentRow-header')
        if header:
            commune["name_ar"] = header.get_text(strip=True)

        # Commune French name: from div.contentRow-lesser.frtxt
        fr_div = row.select_one('div.contentRow-lesser.frtxt')
        if fr_div:
            commune["name_fr"] = fr_div.get_text(strip=True)

        # Fax number: from span[dir=ltr].frtxt
        fax_span = row.select_one('span[dir="ltr"].frtxt')
        if fax_span:
            commune["fax"] = fax_span.get_text(strip=True)

        # Only add if we got at least a code
        if "code" in commune:
            communes.append(commune)

    return {
        "code": f"{wilaya_code:02d}",
        "name_ar": wilaya_name_ar,
        "communes": communes,
    }


# ---------------------------------------------------------------------------
# Main scraper function
# ---------------------------------------------------------------------------

def scrape(skip_if_recent: bool = False) -> dict:
    """
    Scrape all 58 wilayas and return the combined data.

    Args:
        skip_if_recent: If True, skip scraping if output file exists and is
                        less than 24 hours old.

    Returns:
        The full scraped data dict.
    """
    ensure_dirs()

    # Check if we can skip
    if skip_if_recent and OUTPUT_FILE.exists():
        mtime = datetime.fromtimestamp(OUTPUT_FILE.stat().st_mtime, tz=timezone.utc)
        age_hours = (datetime.now(timezone.utc) - mtime).total_seconds() / 3600
        if age_hours < 24:
            logger.info(f"Scraped data is {age_hours:.1f}h old, skipping scrape.")
            with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
                return json.load(f)

    logger.info("=" * 60)
    logger.info("Starting scrape of apcsali-adrar.dz (wilayas 1-58)")
    logger.info("=" * 60)

    wilayas = []
    total_communes = 0
    failed = []

    for code in WILAYA_RANGE:
        html = fetch_page(code)
        if html is None:
            failed.append(code)
            continue

        wilaya_data = parse_wilaya_page(html, code)
        wilayas.append(wilaya_data)
        n = len(wilaya_data["communes"])
        total_communes += n
        logger.info(f"  → Wilaya {code:02d} ({wilaya_data['name_ar']}): {n} communes")

        # Rate limiting (don't delay after last request)
        if code < max(WILAYA_RANGE):
            time.sleep(REQUEST_DELAY)

    # Build output
    result = {
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "total_wilayas": len(wilayas),
        "total_communes": total_communes,
        "failed_wilayas": failed,
        "wilayas": wilayas,
    }

    # Save output
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    logger.info("=" * 60)
    logger.info(f"Scrape complete: {len(wilayas)} wilayas, {total_communes} communes")
    if failed:
        logger.warning(f"Failed wilayas: {failed}")
    logger.info(f"Output saved to: {OUTPUT_FILE}")
    logger.info("=" * 60)

    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    scrape()
