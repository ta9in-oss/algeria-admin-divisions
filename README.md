<div align="center">

# 🇩🇿 Algeria Administrative Divisions Database

### قاعدة بيانات التقسيم الإداري للجزائر

### Base de données des divisions administratives de l'Algérie

[![Maintained by Ta9in](https://img.shields.io/badge/Maintained%20by-Ta9in-blueviolet)](https://ta9in.com)
[![Update Data](https://github.com/ta9in-oss/algeria-admin-divisions/actions/workflows/update-data.yml/badge.svg)](https://github.com/ta9in-oss/algeria-admin-divisions/actions/workflows/update-data.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Data Quality](https://img.shields.io/badge/Data%20Quality-69%2F69%20Wilayas-brightgreen)](data/processed/)


</div>

---

## 📋 Overview | نظرة عامة

A comprehensive, multilingual (Arabic, French, English) database of Algeria's **69 wilayas** (provinces), their **dairas** (districts), and **communes** (municipalities).

قاعدة بيانات شاملة ومتعددة اللغات (العربية، الفرنسية، الإنجليزية) لـ **69 ولاية** جزائرية، دوائرها وبلدياتها.

### Historical Evolution | التطور التاريخي

| Period | Wilayas | Event |
|--------|---------|-------|
| Original | 48 | Historical provinces since independence |
| November 2019 | 58 | +10 southern wilayas promoted from administrative districts |
| November 2025 | 69 | +11 new wilayas split from existing ones |

---

## 📊 Data Completeness Status | حالة اكتمال البيانات

| Wilayas | Communes | Dairas | Contact Info | Status |
|---------|----------|--------|--------------|--------|
| 1-48 | ✅ Full | ✅ Full | ✅ Full | Complete |
| 49-58 | ✅ Full | ⚠️ Partial | ⚠️ Partial | Scraped + Legacy |
| 59-69 | ⚠️ Names only | ❌ Missing | ❌ Missing | Manual |

<!-- STATS_START -->
| Metric | Count |
|--------|-------|
| Wilayas | 69 |
| Communes | 1541 |
| Dairas | 533 |
| Full data | 48 wilayas |
| Partial data | 10 wilayas |
| Names only | 11 wilayas |

> Last updated: 2026-04-20 11:36 UTC
<!-- STATS_END -->

---

## 📁 Project Structure | هيكل المشروع

```
algeria-admin-divisions/
├── data/
│   ├── raw/                     # Original raw data sources
│   │   ├── legacy_48.json       # Old dataset (48 wilayas, Arabic, full contact)
│   │   ├── scraped_58.json      # Scraped from apcsali-adrar.dz (58 wilayas)
│   │   ├── new_wilayas_11.json  # Manual input for 11 new wilayas
│   │   └── cache/               # Cached HTML pages from scraper
│   ├── processed/               # Final production data
│   │   ├── wilayas.json         # All 69 wilayas
│   │   ├── communes.json        # All communes
│   │   └── dairas.json          # All dairas
│   ├── exports/                 # Alternative formats
│   │   ├── wilayas.csv
│   │   ├── communes.csv
│   │   ├── dairas.csv
│   │   └── algeria_divisions.sql
│   └── metadata/
│       ├── changes.json         # Migration/split change log
│       └── sources.json         # Data source documentation
├── scripts/
│   ├── scraper.py               # Web scraper for commune data
│   ├── translator.py            # AR/FR/EN translation engine
│   ├── merger.py                # 3-source data fusion
│   ├── validator.py             # Data integrity checks
│   ├── exporter.py              # CSV + SQL export
│   └── main.py                  # Pipeline orchestrator
├── .github/workflows/
│   ├── update-data.yml          # Weekly automated pipeline
│   └── validate-pr.yml          # PR validation
├── requirements.txt
└── README.md
```

---

## 🚀 Usage | الاستخدام

### Python

```python
import json

# Load wilayas
with open("data/processed/wilayas.json", encoding="utf-8") as f:
    wilayas = json.load(f)

# Find a wilaya by code
adrar = next(w for w in wilayas if w["code"] == "01")
print(f"{adrar['name_ar']} — {adrar['name_fr']} — {adrar['name_en']}")
# أدرار — Adrar — Adrar

# List all 2025 expansion wilayas
new_2025 = [w for w in wilayas if w.get("created_year") == 2025]
for w in new_2025:
    print(f"  {w['code']}: {w['name_ar']} ({w['name_fr']})")
```

### JavaScript

```javascript
const wilayas = require("./data/processed/wilayas.json");
const communes = require("./data/processed/communes.json");

// Get communes for a specific wilaya
const oran_communes = communes.filter(c => c.wilaya_code === "31");
console.log(`Oran has ${oran_communes.length} communes`);
```

### SQL (SQLite)

```bash
# Import the database
sqlite3 algeria.db < data/exports/algeria_divisions.sql

# Query examples
sqlite3 algeria.db "SELECT code, name_ar, name_fr FROM wilayas ORDER BY code;"
sqlite3 algeria.db "SELECT COUNT(*) FROM communes WHERE wilaya_code = '16';"
```

---

## 🔄 Data Pipeline | خط إنتاج البيانات

The pipeline runs automatically every Sunday via GitHub Actions, or can be triggered manually:

```bash
# Install dependencies
pip install -r requirements.txt

# Run the full pipeline
cd scripts
python main.py

# Skip web scraping (use cached data)
python main.py --skip-scrape

# Verbose output
python main.py --verbose
```

### Pipeline Steps

1. **Scrape** — Fetch commune data from `apcsali-adrar.dz` (wilayas 1-58)
2. **Merge** — Combine scraped, legacy, and manual data sources
3. **Translate** — Generate English names
4. **Validate** — Check data integrity (codes, duplicates, references)
5. **Export** — Generate CSV and SQL formats
6. **Update README** — Refresh data statistics

---

## 📍 Data Sources | مصادر البيانات

| Source | Coverage | Data | Languages |
|--------|----------|------|-----------|
| `algeria-locations.json` | 48 wilayas (original) | Full contact info, dairas | Arabic |
| `apcsali-adrar.dz` | 58 wilayas (current) | Commune codes, fax | Arabic, French |
| Manual input | 11 wilayas (2025) | Names only | Arabic, French |

---

## 📐 Data Schema | هيكل البيانات

### Wilaya

| Field | Type | Description |
|-------|------|-------------|
| `code` | string | 2-digit code (01-69) |
| `name_ar` | string | Arabic name |
| `name_fr` | string | French name |
| `name_en` | string | English name |
| `phone` | string | Phone number |
| `fax` | string | Fax number |
| `email` | string | Email address |
| `website` | string | Website URL |
| `address_ar` | string | Address in Arabic |
| `created_year` | int/null | Year created (2019/2025 for new) |
| `parent_wilaya` | string/null | Parent wilaya code (for splits) |
| `data_completeness` | string | `full`, `partial`, or `names_only` |
| `source` | string | Data source: `merged`, `scraped`, `manual` |

### Commune

| Field | Type | Description |
|-------|------|-------------|
| `code` | string | 4-digit code (XXYY) |
| `name_ar` | string | Arabic name |
| `name_fr` | string | French name |
| `name_en` | string | English name |
| `wilaya_code` | string | Parent wilaya code |
| `daira_name_ar` | string/null | Parent daira name |
| `fax` | string | Fax number |

### Daira

| Field | Type | Description |
|-------|------|-------------|
| `name_ar` | string | Arabic name |
| `name_fr` | string | French name |
| `name_en` | string | English name |
| `wilaya_code` | string | Parent wilaya code |
| `communes` | array | List of commune codes |

---

## 🤝 Contributing | المساهمة

Contributions are welcome! Especially for:

- **Wilayas 59-69**: Commune and daira data when officially released
- **Contact info updates**: Phone numbers, emails that have changed
- **English translations**: Better English spellings for places

### How to contribute

1. Fork the repository
2. Edit data files in `data/raw/`
3. Run `python scripts/main.py --skip-scrape` to validate
4. Submit a Pull Request — the CI will automatically validate your changes

---

## 📜 License | الرخصة

MIT License — see [LICENSE](LICENSE) for details.


<div align="center">

**Made with ❤️ by [Ta9in](https://ta9in.com) for Algeria 🇩🇿**

</div>
