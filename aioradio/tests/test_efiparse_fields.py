"""Tests for EFIParse field_to_max_widths and filed_date_min_max completeness."""

import pytest
from aioradio.file_ingestion import EFIParse

FIELDS_2024 = [
    "PREFERREDFIRSTNAME", "HISPANIC", "SAI", "CAMPUSVISITDATE",
    "ORIENTREGDATE", "ORIENTATTENDDATE", "HOUSINGDATE", "APPLICATIONTYPE",
    "COUNSELORNAME", "PARENT1FIRSTNAME", "PARENT1LASTNAME", "PARENT1EMAIL",
    "PARENT2FIRSTNAME", "PARENT2LASTNAME", "PARENT2EMAIL", "INDEXSCORE",
]

FIELDS_2025 = ["HOUSINGTYPE"]

EN5537_FIELDS = [
    "ALTERNATEID", "TESTOPTIONAL", "TRANSFERCOLLEGE", "COLLEGEGPA",
    "DEFERRED", "DEFERREDFLAG", "DENIED", "WAITLISTED", "WAITLISTFLAG",
    "CAMPUSVISITTYPE", "ADMITPLAN", "APPLIEDPROGRAM", "ENROLLEDPROGRAM",
    "CUSTOMFILTER6", "CUSTOMFILTER7", "CUSTOMFILTER8", "CUSTOMFILTER9",
    "CUSTOMFILTER10",
]

SCORE_FIELDS = ["AWARD_PLUS_SCORE", "INQUIRY_PLUS_SCORE"]

DATE_FIELDS_ADDED = [
    "CAMPUSVISITDATE", "ORIENTREGDATE", "ORIENTATTENDDATE", "HOUSINGDATE",
    "DEFERRED", "DENIED", "WAITLISTED",
]

ALL_NEW_WIDTH_FIELDS = FIELDS_2024 + FIELDS_2025 + EN5537_FIELDS + SCORE_FIELDS


@pytest.fixture
def efi():
    obj = EFIParse(filename="test.csv")
    obj.__post_init__()
    return obj


class TestFieldToMaxWidths:
    def test_all_new_fields_present(self, efi):
        for field in ALL_NEW_WIDTH_FIELDS:
            assert field in efi.field_to_max_widths, f"{field} missing from field_to_max_widths"

    def test_score_field_widths(self, efi):
        assert efi.field_to_max_widths["AWARD_PLUS_SCORE"] == 20
        assert efi.field_to_max_widths["INQUIRY_PLUS_SCORE"] == 20


class TestFiledDateMinMax:
    def test_all_new_date_fields_present(self, efi):
        for field in DATE_FIELDS_ADDED:
            assert field in efi.filed_date_min_max, f"{field} missing from filed_date_min_max"
            min_date, max_date = efi.filed_date_min_max[field]
            assert min_date < max_date

    def test_score_fields_not_in_date_ranges(self, efi):
        for field in SCORE_FIELDS:
            assert field not in efi.filed_date_min_max
