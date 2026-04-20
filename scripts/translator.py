#!/usr/bin/env python3
"""
Translator module for Algeria administrative division names.

Handles 3-language translation (Arabic, French, English).
- Arabic & French come from source data.
- English uses a curated mapping for well-known cities,
  falling back to French names for the rest.
"""

import logging

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Curated English translations for well-known Algerian places
# Key: French name (lowercase for matching), Value: English name
# ---------------------------------------------------------------------------

ENGLISH_NAMES = {
    # Major cities with distinct English spellings
    "alger": "Algiers",
    "constantine": "Constantine",
    "oran": "Oran",
    "annaba": "Annaba",
    "tlemcen": "Tlemcen",
    "setif": "Setif",
    "batna": "Batna",
    "biskra": "Biskra",
    "bejaia": "Bejaia",
    "blida": "Blida",
    "tizi ouzou": "Tizi Ouzou",
    "djelfa": "Djelfa",
    "skikda": "Skikda",
    "jijel": "Jijel",
    "ghardaia": "Ghardaia",
    "ouargla": "Ouargla",
    "bechar": "Bechar",
    "tamanrasset": "Tamanrasset",
    "tebessa": "Tebessa",
    "tiaret": "Tiaret",
    "medea": "Medea",
    "m'sila": "M'Sila",
    "mascara": "Mascara",
    "saida": "Saida",
    "mostaganem": "Mostaganem",
    "chlef": "Chlef",
    "bouira": "Bouira",
    "el oued": "El Oued",
    "khenchela": "Khenchela",
    "souk ahras": "Souk Ahras",
    "tipaza": "Tipaza",
    "mila": "Mila",
    "ain defla": "Ain Defla",
    "ain temouchent": "Ain Temouchent",
    "relizane": "Relizane",
    "el bayadh": "El Bayadh",
    "naama": "Naama",
    "illizi": "Illizi",
    "bordj bou arreridj": "Bordj Bou Arreridj",
    "boumerdes": "Boumerdes",
    "el tarf": "El Tarf",
    "tindouf": "Tindouf",
    "tissemsilt": "Tissemsilt",
    "sidi bel abbes": "Sidi Bel Abbes",
    "guelma": "Guelma",
    "adrar": "Adrar",
    "oum el bouaghi": "Oum El Bouaghi",
    "laghouat": "Laghouat",
    # 2019 expansion wilayas
    "timimoun": "Timimoun",
    "bordj badji mokhtar": "Bordj Badji Mokhtar",
    "ouled djellal": "Ouled Djellal",
    "beni abbes": "Beni Abbes",
    "in salah": "In Salah",
    "in guezzam": "In Guezzam",
    "touggourt": "Touggourt",
    "djanet": "Djanet",
    "el meghaier": "El Meghaier",
    "el meniaa": "El Meniaa",
    # 2025 expansion wilayas
    "aflou": "Aflou",
    "barika": "Barika",
    "el kantara": "El Kantara",
    "bir el ater": "Bir El Ater",
    "el aricha": "El Aricha",
    "ksar chellala": "Ksar Chellala",
    "ain oussara": "Ain Oussara",
    "messaad": "Messaad",
    "ksar el boukhari": "Ksar El Boukhari",
    "bou saada": "Bou Saada",
    "el abiodh sidi cheikh": "El Abiodh Sidi Cheikh",
}


def get_english_name(name_fr: str | None, name_ar: str | None = None) -> str:
    """
    Get the English name for a place.

    Strategy:
    1. Look up French name in curated dictionary
    2. If not found, use French name as-is (title-cased)
    3. If no French name, use Arabic name

    Args:
        name_fr: French name of the place
        name_ar: Arabic name (fallback)

    Returns:
        English name string
    """
    if name_fr:
        # Try exact match (case-insensitive)
        key = name_fr.lower().strip()
        if key in ENGLISH_NAMES:
            return ENGLISH_NAMES[key]
        # Fallback: use French name, title-cased
        return name_fr.strip().title()

    # No French name available
    if name_ar:
        return name_ar
    return ""


def translate_wilayas(wilayas: list[dict]) -> list[dict]:
    """
    Add English names to a list of wilaya dicts.

    Each wilaya dict should have 'name_fr' and 'name_ar' keys.
    Adds 'name_en' key.
    """
    translated = 0
    for w in wilayas:
        w["name_en"] = get_english_name(w.get("name_fr"), w.get("name_ar"))
        translated += 1

    logger.info(f"Translated {translated} wilaya names to English")
    return wilayas


def translate_communes(communes: list[dict]) -> list[dict]:
    """
    Add English names to a list of commune dicts.
    """
    translated = 0
    for c in communes:
        c["name_en"] = get_english_name(c.get("name_fr"), c.get("name_ar"))
        translated += 1

    logger.info(f"Translated {translated} commune names to English")
    return communes


def translate_dairas(dairas: list[dict]) -> list[dict]:
    """
    Add English names to a list of daira dicts.
    """
    translated = 0
    for d in dairas:
        d["name_en"] = get_english_name(d.get("name_fr"), d.get("name_ar"))
        translated += 1

    logger.info(f"Translated {translated} daira names to English")
    return dairas


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Quick test
    test_names = [
        ("Alger", "الجزائر"),
        ("Constantine", "قسنطينة"),
        ("Oran", "وهران"),
        ("ADRAR", "أدرار"),
        ("TAMEST", "تامست"),
        ("Ain Oussara", "عين وسارة"),
        (None, "بلدية مجهولة"),
    ]
    for fr, ar in test_names:
        en = get_english_name(fr, ar)
        print(f"  {fr or '(none)':30s} → {en}")
