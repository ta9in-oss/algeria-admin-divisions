#!/usr/bin/env python3
"""Quick verification script to spot-check pipeline output."""
import json, sys

sys.stdout.reconfigure(encoding="utf-8")

ROOT = "../data/processed"

w = json.load(open(f"{ROOT}/wilayas.json", encoding="utf-8"))
c = json.load(open(f"{ROOT}/communes.json", encoding="utf-8"))
d = json.load(open(f"{ROOT}/dairas.json", encoding="utf-8"))

print("=" * 70)
print(f"TOTALS: {len(w)} wilayas | {len(c)} communes | {len(d)} dairas")
print("=" * 70)

# Sample wilayas: first 3, tier 2, tier 3
print("\n--- Sample Wilayas (Tier 1: 1-48) ---")
for x in w[:3]:
    print(f"  {x['code']}: {x['name_ar']} | {x['name_fr']} | {x['name_en']} | completeness={x['data_completeness']}")

print("\n--- Sample Wilayas (Tier 2: 49-58) ---")
for x in w[48:52]:
    print(f"  {x['code']}: {x['name_ar']} | {x['name_fr']} | {x['name_en']} | parent={x.get('parent_wilaya')} | completeness={x['data_completeness']}")

print("\n--- Sample Wilayas (Tier 3: 59-69) ---")
for x in w[58:]:
    print(f"  {x['code']}: {x['name_ar']} | {x['name_fr']} | {x['name_en']} | parent={x.get('parent_wilaya')} | completeness={x['data_completeness']}")

# Djanet special case
print("\n--- Djanet (56) - Split from Illizi (33) ---")
djanet_communes = [x for x in c if x["wilaya_code"] == "56"]
print(f"  Communes: {len(djanet_communes)}")
for x in djanet_communes:
    print(f"    {x['code']}: {x['name_ar']} / {x['name_fr']} | daira={x.get('daira_name_ar', 'N/A')}")

djanet_dairas = [x for x in d if x["wilaya_code"] == "56"]
print(f"  Dairas: {len(djanet_dairas)}")
for x in djanet_dairas:
    print(f"    {x['name_ar']} | communes={x['communes']}")

# Adrar dairas (should exclude Timimoun & BBM splits)
print("\n--- Adrar (01) Dairas (should exclude Timimoun/BBM) ---")
adrar_dairas = [x for x in d if x["wilaya_code"] == "01"]
print(f"  Count: {len(adrar_dairas)}")
for x in adrar_dairas:
    print(f"    {x['name_ar']} ({len(x['communes'])} communes)")

# Timimoun dairas (should have dairas from Adrar split)
print("\n--- Timimoun (49) Dairas ---")
tim_dairas = [x for x in d if x["wilaya_code"] == "49"]
print(f"  Count: {len(tim_dairas)}")
for x in tim_dairas:
    print(f"    {x['name_ar']} ({len(x['communes'])} communes)")

# Check Algiers commune sample
print("\n--- Algiers (16) Sample Communes ---")
algiers = [x for x in c if x["wilaya_code"] == "16"]
print(f"  Total communes: {len(algiers)}")
for x in algiers[:3]:
    print(f"    {x['code']}: {x['name_ar']} / {x['name_fr']} / {x['name_en']} | daira={x.get('daira_name_ar', 'N/A')}")

# Completeness distribution
print("\n--- Completeness Distribution ---")
for level in ["full", "partial", "names_only"]:
    count = sum(1 for x in w if x.get("data_completeness") == level)
    codes = [x["code"] for x in w if x.get("data_completeness") == level]
    print(f"  {level}: {count} wilayas ({codes[0]}-{codes[-1]})")

# Check exports exist
from pathlib import Path
exports = Path("../data/exports")
print("\n--- Export Files ---")
for f in sorted(exports.iterdir()):
    print(f"  {f.name}: {f.stat().st_size:,} bytes")

print("\n" + "=" * 70)
print("Verification complete!")
