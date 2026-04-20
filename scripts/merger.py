#!/usr/bin/env python3
"""
Data merger for Algeria administrative divisions.

Combines three data sources:
  1. Legacy data (48 wilayas) — rich contact info, Arabic only
  2. Scraped data (58 wilayas) — commune codes, AR/FR names, fax
  3. Manual data (11 new wilayas) — names only

Priority: scraped → legacy → manual

Handles special cases:
  - 2019 expansion: 10 wilayas split from original 48
  - 2025 expansion: 11 new wilayas with names only
"""

import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from difflib import SequenceMatcher

logger = logging.getLogger(__name__)

# Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
METADATA_DIR = PROJECT_ROOT / "data" / "metadata"

# ---------------------------------------------------------------------------
# 2019 wilaya split mapping: new_code → (parent_code, parent_wilaya_name_ar)
# These wilayas were carved from existing ones. Their communes existed as
# dairas under the parent wilaya in the legacy data.
# ---------------------------------------------------------------------------

WILAYA_SPLITS_2019 = {
    "49": {"parent": "01", "name_ar": "تيميمون", "name_fr": "Timimoun",
           "legacy_dairas": ["تيميمون", "شروين", "أوقروت", "تنركوك"]},
    "50": {"parent": "01", "name_ar": "برج باجي مختار", "name_fr": "Bordj Badji Mokhtar",
           "legacy_dairas": ["برج باجي مختار"]},
    "51": {"parent": "07", "name_ar": "أولاد جلال", "name_fr": "Ouled Djellal",
           "legacy_dairas": ["أولاد جلال", "سيدي خالد"]},
    "52": {"parent": "08", "name_ar": "بني عباس", "name_fr": "Beni Abbes",
           "legacy_dairas": ["بني عباس", "بني ونيف", "القنادسة"]},
    "53": {"parent": "11", "name_ar": "عين صالح", "name_fr": "In Salah",
           "legacy_dairas": ["عين صالح"]},
    "54": {"parent": "11", "name_ar": "عين قزام", "name_fr": "In Guezzam",
           "legacy_dairas": ["عين قزام"]},
    "55": {"parent": "30", "name_ar": "تقرت", "name_fr": "Touggourt",
           "legacy_dairas": ["تقرت", "المقارين", "الطيبات"]},
    "56": {"parent": "33", "name_ar": "جانت", "name_fr": "Djanet",
           "legacy_dairas": ["جانت"]},
    "57": {"parent": "39", "name_ar": "المغير", "name_fr": "El Meghaier",
           "legacy_dairas": ["المغير", "جامعة"]},
    "58": {"parent": "47", "name_ar": "المنيعة", "name_fr": "El Meniaa",
           "legacy_dairas": ["المنيعة"]},
}

# ---------------------------------------------------------------------------
# French names for all 58 wilayas (scraped sidebar only has Arabic)
# ---------------------------------------------------------------------------

WILAYA_FRENCH_NAMES = {
    "01": "Adrar", "02": "Chlef", "03": "Laghouat", "04": "Oum El Bouaghi",
    "05": "Batna", "06": "Bejaia", "07": "Biskra", "08": "Bechar",
    "09": "Blida", "10": "Bouira", "11": "Tamanrasset", "12": "Tebessa",
    "13": "Tlemcen", "14": "Tiaret", "15": "Tizi Ouzou", "16": "Alger",
    "17": "Djelfa", "18": "Jijel", "19": "Setif", "20": "Saida",
    "21": "Skikda", "22": "Sidi Bel Abbes", "23": "Annaba", "24": "Guelma",
    "25": "Constantine", "26": "Medea", "27": "Mostaganem", "28": "M'Sila",
    "29": "Mascara", "30": "Ouargla", "31": "Oran", "32": "El Bayadh",
    "33": "Illizi", "34": "Bordj Bou Arreridj", "35": "Boumerdes",
    "36": "El Tarf", "37": "Tindouf", "38": "Tissemsilt", "39": "El Oued",
    "40": "Khenchela", "41": "Souk Ahras", "42": "Tipaza", "43": "Mila",
    "44": "Ain Defla", "45": "Naama", "46": "Ain Temouchent",
    "47": "Ghardaia", "48": "Relizane",
    "49": "Timimoun", "50": "Bordj Badji Mokhtar", "51": "Ouled Djellal",
    "52": "Beni Abbes", "53": "In Salah", "54": "In Guezzam",
    "55": "Touggourt", "56": "Djanet", "57": "El Meghaier", "58": "El Meniaa",
}


def _normalize_ar(text: str) -> str:
    """Normalize Arabic text for comparison."""
    if not text:
        return ""
    # Remove diacritics (tashkeel)
    text = re.sub(r'[\u064B-\u065F\u0670]', '', text)
    # Normalize alef variants
    text = re.sub(r'[إأآا]', 'ا', text)
    # Normalize taa marbuta and haa
    text = text.replace('ة', 'ه')
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def _fuzzy_match_ar(name1: str, name2: str) -> float:
    """Return similarity ratio between two Arabic names (0-1)."""
    n1 = _normalize_ar(name1)
    n2 = _normalize_ar(name2)
    return SequenceMatcher(None, n1, n2).ratio()


def _load_json(path: Path) -> dict | list:
    """Load a JSON file."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _save_json(data, path: Path):
    """Save data as JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info(f"  Saved: {path}")


def _build_legacy_index(legacy_data: list[dict]) -> dict:
    """
    Build a lookup index from legacy data.

    Returns:
        {
            "wilaya_name_ar": {
                "wilaya_info": { phone, fax, email, ... },
                "dairas": {
                    "daira_name_ar": {
                        "daira_info": { phone, fax, email, ... },
                        "communes": [
                            { commune_name, phone, fax, email, ... },
                            ...
                        ]
                    }
                }
            }
        }
    """
    index = {}

    for record in legacy_data:
        wname = record["wilaya_name"]
        dname = record["daira_name"]
        cname = record["commune_name"]

        if wname not in index:
            index[wname] = {
                "wilaya_info": {
                    "phone": record.get("wilaya_phone", ""),
                    "fax": record.get("wilaya_fax", ""),
                    "email": record.get("wilaya_email", ""),
                    "website": record.get("wilaya_website", ""),
                    "address_ar": record.get("wilaya_address", ""),
                },
                "dairas": {},
            }

        if dname not in index[wname]["dairas"]:
            index[wname]["dairas"][dname] = {
                "daira_info": {
                    "phone": record.get("daira_phone", ""),
                    "fax": record.get("daira_fax", ""),
                    "email": record.get("daira_email", ""),
                    "website": record.get("daira_website", ""),
                    "address_ar": record.get("daira_address", ""),
                },
                "communes": [],
            }

        index[wname]["dairas"][dname]["communes"].append({
            "commune_name": cname,
            "phone": record.get("commune_phone", ""),
            "fax": record.get("commune_fax", ""),
            "email": record.get("commune_email", ""),
            "website": record.get("commune_website", ""),
            "address_ar": record.get("commune_address", ""),
        })

    return index


def _find_legacy_commune(legacy_index: dict, wilaya_ar: str, commune_ar: str) -> dict | None:
    """Find a commune in the legacy index by Arabic name with fuzzy matching."""
    # Try the wilaya directly
    if wilaya_ar in legacy_index:
        wilaya_data = legacy_index[wilaya_ar]
    else:
        # Try fuzzy match on wilaya name
        best_match = None
        best_score = 0
        for wname in legacy_index:
            score = _fuzzy_match_ar(wilaya_ar, wname)
            if score > best_score:
                best_score = score
                best_match = wname
        if best_score < 0.7:
            return None
        wilaya_data = legacy_index[best_match]

    # Search all dairas for the commune
    for daira_name, daira_data in wilaya_data["dairas"].items():
        for legacy_commune in daira_data["communes"]:
            score = _fuzzy_match_ar(commune_ar, legacy_commune["commune_name"])
            if score > 0.8:
                return {
                    **legacy_commune,
                    "daira_name_ar": daira_name,
                }

    return None


def _find_legacy_wilaya_for_split(legacy_index: dict, parent_wilaya_ar: str,
                                   split_dairas: list[str]) -> dict:
    """
    Extract communes belonging to split dairas from the parent wilaya in legacy data.

    Returns:
        {
            "wilaya_info": { ... },
            "dairas": { daira_name: { daira_info, communes } },
            "all_communes": [ ... ]
        }
    """
    result = {"wilaya_info": {}, "dairas": {}, "all_communes": []}

    # Find parent wilaya
    parent = None
    for wname in legacy_index:
        if _fuzzy_match_ar(parent_wilaya_ar, wname) > 0.7:
            parent = legacy_index[wname]
            break

    if not parent:
        logger.warning(f"  ⚠ Could not find parent wilaya '{parent_wilaya_ar}' in legacy data")
        return result

    # Extract matching dairas
    for daira_name, daira_data in parent["dairas"].items():
        for split_daira in split_dairas:
            if _fuzzy_match_ar(daira_name, split_daira) > 0.7:
                result["dairas"][daira_name] = daira_data
                result["all_communes"].extend(daira_data["communes"])
                break

    return result


# ---------------------------------------------------------------------------
# Main merge function
# ---------------------------------------------------------------------------

def merge() -> tuple[list[dict], list[dict], list[dict]]:
    """
    Merge all data sources into unified wilayas, communes, and dairas.

    Returns:
        (wilayas, communes, dairas)
    """
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    METADATA_DIR.mkdir(parents=True, exist_ok=True)

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # --- Load sources ---
    legacy_path = RAW_DIR / "legacy_48.json"
    scraped_path = RAW_DIR / "scraped_58.json"
    manual_path = RAW_DIR / "new_wilayas_11.json"

    # Legacy: use the original file if legacy_48.json doesn't exist yet
    if legacy_path.exists():
        legacy_raw = _load_json(legacy_path)
    else:
        orig_path = PROJECT_ROOT / "algeria-locations.json"
        if orig_path.exists():
            legacy_raw = _load_json(orig_path)
            # Save as legacy_48.json for future use
            _save_json(legacy_raw, legacy_path)
        else:
            logger.warning("No legacy data found!")
            legacy_raw = []

    scraped = _load_json(scraped_path) if scraped_path.exists() else {"wilayas": []}
    manual = _load_json(manual_path) if manual_path.exists() else []

    # Build legacy index
    legacy_index = _build_legacy_index(legacy_raw)
    logger.info(f"Legacy index: {len(legacy_index)} wilayas")

    # Build scraped index
    scraped_index = {w["code"]: w for w in scraped.get("wilayas", [])}
    logger.info(f"Scraped index: {len(scraped_index)} wilayas")

    # --- Output containers ---
    all_wilayas = []
    all_communes = []
    all_dairas = []
    changes_log = []

    # ==================================================================
    # TIER 1: Wilayas 1-48 (legacy + scraped)
    # ==================================================================
    logger.info("--- Processing Tier 1: Wilayas 1-48 ---")

    # Build mapping from legacy wilaya names to codes using scraped data
    # The scraped sidebar gives us the wilaya code → AR name mapping
    scraped_code_to_name = {}
    for sw in scraped.get("wilayas", []):
        scraped_code_to_name[sw["code"]] = sw["name_ar"]

    for code_int in range(1, 49):
        code = f"{code_int:02d}"
        scraped_wilaya = scraped_index.get(code, {})
        wilaya_name_ar = scraped_wilaya.get("name_ar", "")

        # Find in legacy index
        legacy_wilaya = None
        legacy_wilaya_name = None
        for lname in legacy_index:
            if _fuzzy_match_ar(wilaya_name_ar, lname) > 0.7:
                legacy_wilaya = legacy_index[lname]
                legacy_wilaya_name = lname
                break

        # Build wilaya record
        wilaya_info = legacy_wilaya["wilaya_info"] if legacy_wilaya else {}
        wilaya_record = {
            "code": code,
            "name_ar": wilaya_name_ar,
            "name_fr": WILAYA_FRENCH_NAMES.get(code, ""),  # Look up French name
            "name_en": "",  # Will be filled by translator
            "phone": wilaya_info.get("phone", ""),
            "fax": wilaya_info.get("fax", ""),
            "email": wilaya_info.get("email", ""),
            "website": wilaya_info.get("website", ""),
            "address_ar": wilaya_info.get("address_ar", ""),
            "created_year": None,
            "parent_wilaya": None,
            "data_completeness": "full",
            "source": "merged",
            "last_updated": today,
        }
        all_wilayas.append(wilaya_record)

        # Process communes for this wilaya
        scraped_communes = scraped_wilaya.get("communes", [])

        # Track which legacy communes we've matched (to detect splits later)
        matched_legacy = set()

        for sc in scraped_communes:
            commune_record = {
                "code": sc.get("code", ""),
                "name_ar": sc.get("name_ar", ""),
                "name_fr": sc.get("name_fr", ""),
                "name_en": "",
                "wilaya_code": code,
                "wilaya_name_ar": wilaya_name_ar,
                "daira_name_ar": None,
                "phone": "",
                "fax": sc.get("fax", ""),
                "email": "",
                "website": "",
                "address_ar": "",
                "source": "scraped",
            }

            # Try to enrich from legacy
            if legacy_wilaya and legacy_wilaya_name:
                lc = _find_legacy_commune(
                    {legacy_wilaya_name: legacy_wilaya},
                    legacy_wilaya_name,
                    sc.get("name_ar", ""),
                )
                if lc:
                    commune_record["daira_name_ar"] = lc.get("daira_name_ar")
                    commune_record["phone"] = lc.get("phone", "")
                    commune_record["email"] = lc.get("email", "")
                    commune_record["website"] = lc.get("website", "")
                    commune_record["address_ar"] = lc.get("address_ar", "")
                    commune_record["source"] = "merged"
                    matched_legacy.add(lc["commune_name"])

            all_communes.append(commune_record)

        # Build dairas from legacy data for this wilaya
        if legacy_wilaya:
            for daira_name, daira_data in legacy_wilaya["dairas"].items():
                # Check if this daira was split off to a 2019 wilaya
                is_split = False
                for split_code, split_info in WILAYA_SPLITS_2019.items():
                    if split_info["parent"] == code:
                        for sd in split_info["legacy_dairas"]:
                            if _fuzzy_match_ar(daira_name, sd) > 0.7:
                                is_split = True
                                break
                    if is_split:
                        break

                if is_split:
                    continue  # This daira now belongs to a newer wilaya

                # Find commune codes for this daira
                daira_commune_codes = []
                for dc in daira_data["communes"]:
                    # Match to scraped commune by name
                    for sc in scraped_communes:
                        if _fuzzy_match_ar(dc["commune_name"], sc.get("name_ar", "")) > 0.8:
                            daira_commune_codes.append(sc["code"])
                            break

                daira_record = {
                    "name_ar": daira_name,
                    "name_fr": "",  # Will need to be filled
                    "name_en": "",
                    "wilaya_code": code,
                    "phone": daira_data["daira_info"].get("phone", ""),
                    "fax": daira_data["daira_info"].get("fax", ""),
                    "email": daira_data["daira_info"].get("email", ""),
                    "communes": daira_commune_codes,
                    "source": "legacy",
                }
                all_dairas.append(daira_record)

    # ==================================================================
    # TIER 2: Wilayas 49-58 (2019 expansion — scraped + legacy splits)
    # ==================================================================
    logger.info("--- Processing Tier 2: Wilayas 49-58 ---")

    for code_int in range(49, 59):
        code = f"{code_int:02d}"
        scraped_wilaya = scraped_index.get(code, {})
        split_info = WILAYA_SPLITS_2019.get(code, {})

        wilaya_name_ar = scraped_wilaya.get("name_ar", split_info.get("name_ar", ""))
        parent_code = split_info.get("parent", "")

        # Find parent wilaya name
        parent_name_ar = ""
        for lname in legacy_index:
            # Check if the parent code matches (by comparing against scraped sidebar)
            if parent_code in scraped_code_to_name:
                if _fuzzy_match_ar(scraped_code_to_name[parent_code], lname) > 0.7:
                    parent_name_ar = lname
                    break

        # Get legacy data from parent's split dairas
        legacy_split = _find_legacy_wilaya_for_split(
            legacy_index, parent_name_ar, split_info.get("legacy_dairas", [])
        )

        # Build wilaya record
        wilaya_record = {
            "code": code,
            "name_ar": wilaya_name_ar,
            "name_fr": split_info.get("name_fr", ""),
            "name_en": "",
            "phone": "",
            "fax": "",
            "email": "",
            "website": "",
            "address_ar": "",
            "created_year": 2019,
            "parent_wilaya": parent_code,
            "data_completeness": "partial",
            "source": "merged",
            "last_updated": today,
        }
        all_wilayas.append(wilaya_record)

        # Process scraped communes
        scraped_communes = scraped_wilaya.get("communes", [])
        for sc in scraped_communes:
            commune_record = {
                "code": sc.get("code", ""),
                "name_ar": sc.get("name_ar", ""),
                "name_fr": sc.get("name_fr", ""),
                "name_en": "",
                "wilaya_code": code,
                "wilaya_name_ar": wilaya_name_ar,
                "daira_name_ar": None,
                "phone": "",
                "fax": sc.get("fax", ""),
                "email": "",
                "website": "",
                "address_ar": "",
                "source": "scraped",
            }

            # Try to enrich from legacy split data
            for lc in legacy_split["all_communes"]:
                if _fuzzy_match_ar(sc.get("name_ar", ""), lc["commune_name"]) > 0.8:
                    commune_record["phone"] = lc.get("phone", "")
                    commune_record["email"] = lc.get("email", "")
                    commune_record["website"] = lc.get("website", "")
                    commune_record["address_ar"] = lc.get("address_ar", "")
                    commune_record["source"] = "merged"

                    # Find which daira this commune was in
                    for dname, ddata in legacy_split["dairas"].items():
                        for dc in ddata["communes"]:
                            if _fuzzy_match_ar(sc.get("name_ar", ""), dc["commune_name"]) > 0.8:
                                commune_record["daira_name_ar"] = dname
                                break
                    break

            all_communes.append(commune_record)

        # Build dairas from legacy split data
        for daira_name, daira_data in legacy_split["dairas"].items():
            daira_commune_codes = []
            for dc in daira_data["communes"]:
                for sc in scraped_communes:
                    if _fuzzy_match_ar(dc["commune_name"], sc.get("name_ar", "")) > 0.8:
                        daira_commune_codes.append(sc["code"])
                        break

            daira_record = {
                "name_ar": daira_name,
                "name_fr": "",
                "name_en": "",
                "wilaya_code": code,
                "phone": daira_data["daira_info"].get("phone", ""),
                "fax": daira_data["daira_info"].get("fax", ""),
                "email": daira_data["daira_info"].get("email", ""),
                "communes": daira_commune_codes,
                "source": "legacy",
            }
            all_dairas.append(daira_record)

        # Log the split
        changes_log.append({
            "type": "wilaya_split",
            "new_wilaya_code": code,
            "new_wilaya_name_ar": wilaya_name_ar,
            "parent_wilaya_code": parent_code,
            "parent_wilaya_name_ar": parent_name_ar,
            "dairas_moved": split_info.get("legacy_dairas", []),
            "communes_count": len(scraped_communes),
            "effective_year": 2019,
        })

    # ==================================================================
    # TIER 3: Wilayas 59-69 (2025 expansion — manual only)
    # ==================================================================
    logger.info("--- Processing Tier 3: Wilayas 59-69 ---")

    for mw in manual:
        wilaya_record = {
            "code": mw["code"],
            "name_ar": mw["name_ar"],
            "name_fr": mw["name_fr"],
            "name_en": "",
            "phone": "",
            "fax": "",
            "email": "",
            "website": "",
            "address_ar": "",
            "created_year": mw.get("created_year", 2025),
            "parent_wilaya": mw.get("parent_wilaya_code"),
            "data_completeness": "names_only",
            "source": "manual",
            "last_updated": today,
        }
        all_wilayas.append(wilaya_record)

        changes_log.append({
            "type": "wilaya_created",
            "new_wilaya_code": mw["code"],
            "new_wilaya_name_ar": mw["name_ar"],
            "new_wilaya_name_fr": mw["name_fr"],
            "parent_wilaya_code": mw.get("parent_wilaya_code"),
            "parent_wilaya_name_ar": mw.get("parent_wilaya_name_ar", ""),
            "effective_year": 2025,
        })

    # Sort everything
    all_wilayas.sort(key=lambda w: w["code"])
    all_communes.sort(key=lambda c: c["code"])
    all_dairas.sort(key=lambda d: (d["wilaya_code"], d["name_ar"]))

    # --- Save processed data ---
    _save_json(all_wilayas, PROCESSED_DIR / "wilayas.json")
    _save_json(all_communes, PROCESSED_DIR / "communes.json")
    _save_json(all_dairas, PROCESSED_DIR / "dairas.json")

    # --- Save metadata ---
    _save_json(changes_log, METADATA_DIR / "changes.json")

    sources = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sources": {
            "legacy_48": {
                "file": "data/raw/legacy_48.json",
                "description": "Original 48 wilayas dataset (Arabic only, full contact info)",
                "records": len(legacy_raw),
            },
            "scraped_58": {
                "file": "data/raw/scraped_58.json",
                "description": "Scraped from apcsali-adrar.dz (58 wilayas, AR+FR, commune codes)",
                "wilayas": len(scraped.get("wilayas", [])),
                "communes": scraped.get("total_communes", 0),
            },
            "new_wilayas_11": {
                "file": "data/raw/new_wilayas_11.json",
                "description": "Manually entered 11 new wilayas (November 2025, names only)",
                "records": len(manual),
            },
        },
        "merge_strategy": "scraped → legacy → manual",
    }
    _save_json(sources, METADATA_DIR / "sources.json")

    logger.info(f"\n{'='*60}")
    logger.info(f"Merge complete:")
    logger.info(f"  Wilayas:  {len(all_wilayas)}")
    logger.info(f"  Communes: {len(all_communes)}")
    logger.info(f"  Dairas:   {len(all_dairas)}")
    logger.info(f"  Changes:  {len(changes_log)}")
    logger.info(f"{'='*60}")

    return all_wilayas, all_communes, all_dairas


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    merge()
