#!/usr/bin/env python3
"""
Data validator for Algeria administrative divisions.

Checks data integrity rules:
  1. All wilaya codes 01-69 exist
  2. No duplicate commune codes
  3. Commune codes follow XXYY format (XX = wilaya code)
  4. Required fields present
  5. Fax format validation
  6. Referential integrity (communes → wilayas, dairas → wilayas)
  7. No orphan communes
  8. data_completeness field is set correctly
"""

import json
import re
import logging
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

# ---------------------------------------------------------------------------
# Validation result container
# ---------------------------------------------------------------------------

class ValidationReport:
    """Collects validation errors and warnings."""

    def __init__(self):
        self.errors: list[dict] = []
        self.warnings: list[dict] = []
        self.stats: dict = {}

    def error(self, rule: str, message: str, details: dict | None = None):
        self.errors.append({"rule": rule, "message": message, "details": details or {}})

    def warn(self, rule: str, message: str, details: dict | None = None):
        self.warnings.append({"rule": rule, "message": message, "details": details or {}})

    @property
    def is_valid(self) -> bool:
        return len(self.errors) == 0

    def summary(self) -> str:
        lines = [
            "=" * 60,
            "VALIDATION REPORT",
            "=" * 60,
            f"Status: {'✅ PASSED' if self.is_valid else '❌ FAILED'}",
            f"Errors: {len(self.errors)}",
            f"Warnings: {len(self.warnings)}",
            "-" * 60,
        ]

        if self.stats:
            lines.append("Stats:")
            for k, v in self.stats.items():
                lines.append(f"  {k}: {v}")
            lines.append("-" * 60)

        if self.errors:
            lines.append("ERRORS:")
            for e in self.errors:
                lines.append(f"  ❌ [{e['rule']}] {e['message']}")
                if e["details"]:
                    for dk, dv in e["details"].items():
                        lines.append(f"      {dk}: {dv}")

        if self.warnings:
            lines.append("WARNINGS:")
            for w in self.warnings:
                lines.append(f"  ⚠️  [{w['rule']}] {w['message']}")

        lines.append("=" * 60)
        return "\n".join(lines)

    def to_dict(self) -> dict:
        return {
            "is_valid": self.is_valid,
            "error_count": len(self.errors),
            "warning_count": len(self.warnings),
            "stats": self.stats,
            "errors": self.errors,
            "warnings": self.warnings,
        }


# ---------------------------------------------------------------------------
# Validation rules
# ---------------------------------------------------------------------------

def _validate_wilaya_coverage(wilayas: list[dict], report: ValidationReport):
    """Rule 1: All wilaya codes 01-69 must exist."""
    codes = {w["code"] for w in wilayas}
    expected = {f"{i:02d}" for i in range(1, 70)}
    missing = expected - codes
    extra = codes - expected

    if missing:
        report.error("WILAYA_COVERAGE", f"Missing wilaya codes: {sorted(missing)}")
    if extra:
        report.warn("WILAYA_COVERAGE", f"Unexpected wilaya codes: {sorted(extra)}")

    report.stats["total_wilayas"] = len(wilayas)


def _validate_no_duplicate_communes(communes: list[dict], report: ValidationReport):
    """Rule 2: No duplicate commune codes."""
    seen = {}
    for c in communes:
        code = c.get("code", "")
        if code in seen:
            report.error(
                "DUPLICATE_COMMUNE",
                f"Duplicate commune code: {code}",
                {"first": seen[code], "second": c.get("name_ar", "")},
            )
        else:
            seen[code] = c.get("name_ar", "")

    report.stats["total_communes"] = len(communes)
    report.stats["unique_commune_codes"] = len(seen)


def _validate_commune_code_format(communes: list[dict], report: ValidationReport):
    """Rule 3: Commune codes follow XXYY format where XX = wilaya code."""
    for c in communes:
        code = c.get("code", "")
        wilaya_code = c.get("wilaya_code", "")

        if not re.match(r'^\d{4}$', code):
            report.error(
                "COMMUNE_CODE_FORMAT",
                f"Invalid commune code format: '{code}' (expected 4 digits)",
                {"commune": c.get("name_ar", ""), "wilaya": wilaya_code},
            )
            continue

        # Check prefix matches wilaya code
        prefix = code[:2]
        if prefix != wilaya_code:
            report.warn(
                "COMMUNE_CODE_PREFIX",
                f"Commune code {code} prefix '{prefix}' doesn't match wilaya '{wilaya_code}'",
                {"commune": c.get("name_ar", "")},
            )


def _validate_required_fields(wilayas: list[dict], communes: list[dict], report: ValidationReport):
    """Rule 4: Required fields must be present and non-empty."""
    # Wilayas: code, name_ar required always; name_fr optional for manual entries
    for w in wilayas:
        if not w.get("code"):
            report.error("REQUIRED_FIELD", "Wilaya missing 'code'", {"data": str(w)[:100]})
        if not w.get("name_ar"):
            report.error("REQUIRED_FIELD", f"Wilaya {w.get('code', '?')} missing 'name_ar'")

    # Communes: code, name_ar, name_fr required
    missing_fr = 0
    for c in communes:
        if not c.get("code"):
            report.error("REQUIRED_FIELD", "Commune missing 'code'", {"data": str(c)[:100]})
        if not c.get("name_ar"):
            report.error("REQUIRED_FIELD", f"Commune {c.get('code', '?')} missing 'name_ar'")
        if not c.get("name_fr"):
            missing_fr += 1

    if missing_fr > 0:
        report.warn("REQUIRED_FIELD", f"{missing_fr} communes missing 'name_fr'")


def _validate_fax_format(communes: list[dict], report: ValidationReport):
    """Rule 5: Fax numbers (when present) should match expected format."""
    # Accept multiple formats: (XXX) XX - XX - XX or XXX.XX.XX.XX or XXX XX XX XX
    fax_pattern = re.compile(
        r'^\(?\d{3}\)?\s*[\.\- ]*\d{2}\s*[\.\- ]*\d{2}\s*[\.\- ]*\d{2}$'
    )
    invalid_fax = 0
    for c in communes:
        fax = c.get("fax", "")
        if fax and fax.strip():
            if not fax_pattern.match(fax.strip()):
                invalid_fax += 1

    if invalid_fax > 0:
        report.warn("FAX_FORMAT", f"{invalid_fax} communes have non-standard fax format")


def _validate_referential_integrity(wilayas: list[dict], communes: list[dict],
                                     dairas: list[dict], report: ValidationReport):
    """Rules 6-7: Referential integrity checks."""
    wilaya_codes = {w["code"] for w in wilayas}

    # Communes reference valid wilayas
    for c in communes:
        wc = c.get("wilaya_code", "")
        if wc not in wilaya_codes:
            report.error(
                "INVALID_WILAYA_REF",
                f"Commune {c.get('code', '?')} references non-existent wilaya '{wc}'",
            )

    # Dairas reference valid wilayas
    for d in dairas:
        wc = d.get("wilaya_code", "")
        if wc not in wilaya_codes:
            report.error(
                "INVALID_WILAYA_REF",
                f"Daira '{d.get('name_ar', '?')}' references non-existent wilaya '{wc}'",
            )

    # Check daira commune references
    commune_codes = {c["code"] for c in communes}
    orphan_refs = 0
    for d in dairas:
        for cc in d.get("communes", []):
            if cc not in commune_codes:
                orphan_refs += 1

    if orphan_refs > 0:
        report.warn(
            "ORPHAN_DAIRA_REF",
            f"{orphan_refs} daira commune references point to non-existent communes",
        )

    report.stats["total_dairas"] = len(dairas)


def _validate_completeness_field(wilayas: list[dict], report: ValidationReport):
    """Rule 8: data_completeness is set correctly."""
    valid_values = {"full", "partial", "names_only"}
    for w in wilayas:
        comp = w.get("data_completeness", "")
        if comp not in valid_values:
            report.error(
                "COMPLETENESS_FIELD",
                f"Wilaya {w['code']} has invalid data_completeness: '{comp}'",
            )

    # Count by completeness level
    counts = {}
    for w in wilayas:
        c = w.get("data_completeness", "unknown")
        counts[c] = counts.get(c, 0) + 1
    report.stats["completeness"] = counts


# ---------------------------------------------------------------------------
# Main validation function
# ---------------------------------------------------------------------------

def validate(wilayas: list[dict] | None = None,
             communes: list[dict] | None = None,
             dairas: list[dict] | None = None) -> ValidationReport:
    """
    Run all validation rules on the processed data.

    If no data is passed, loads from data/processed/*.json.

    Returns:
        ValidationReport with all errors and warnings.
    """
    # Load from files if not provided
    if wilayas is None:
        wilayas_path = PROCESSED_DIR / "wilayas.json"
        if not wilayas_path.exists():
            report = ValidationReport()
            report.error("FILE_MISSING", f"File not found: {wilayas_path}")
            return report
        with open(wilayas_path, "r", encoding="utf-8") as f:
            wilayas = json.load(f)

    if communes is None:
        communes_path = PROCESSED_DIR / "communes.json"
        if communes_path.exists():
            with open(communes_path, "r", encoding="utf-8") as f:
                communes = json.load(f)
        else:
            communes = []

    if dairas is None:
        dairas_path = PROCESSED_DIR / "dairas.json"
        if dairas_path.exists():
            with open(dairas_path, "r", encoding="utf-8") as f:
                dairas = json.load(f)
        else:
            dairas = []

    logger.info("Running validation...")
    report = ValidationReport()

    _validate_wilaya_coverage(wilayas, report)
    _validate_no_duplicate_communes(communes, report)
    _validate_commune_code_format(communes, report)
    _validate_required_fields(wilayas, communes, report)
    _validate_fax_format(communes, report)
    _validate_referential_integrity(wilayas, communes, dairas, report)
    _validate_completeness_field(wilayas, report)

    return report


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    report = validate()
    print(report.summary())

    # Save report
    report_path = PROCESSED_DIR / "validation_report.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report.to_dict(), f, ensure_ascii=False, indent=2)
    logger.info(f"Report saved to: {report_path}")

    sys.exit(0 if report.is_valid else 1)
