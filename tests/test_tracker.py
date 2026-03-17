"""
Tests for cka-ckad-cks-release-tracker.

Intent 1: Quickly see when the next change in exam release is to be expected.
Intent 2: Low maintenance — detect failures, archive after 30 days.
"""

import importlib.util
import re
import sys
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest

# Import the script as a module despite the hyphens in the filename
_spec = importlib.util.spec_from_file_location(
    "tracker",
    Path(__file__).resolve().parent.parent / "cka-ckad-cks-release-tracker.py",
)
tracker = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(tracker)


# --- Fixtures ---

SAMPLE_ENDOFLIFE = [
    {"cycle": "1.35", "releaseDate": "2025-12-17", "eol": "2027-02-28", "latest": "1.35.2"},
    {"cycle": "1.34", "releaseDate": "2025-08-27", "eol": "2026-10-27", "latest": "1.34.5"},
    {"cycle": "1.33", "releaseDate": "2025-04-23", "eol": "2026-06-28", "latest": "1.33.9"},
    {"cycle": "1.32", "releaseDate": "2024-12-11", "eol": "2026-02-28", "latest": "1.32.13"},
    {"cycle": "1.31", "releaseDate": "2024-08-13", "eol": "2025-10-28", "latest": "1.31.14"},
    {"cycle": "1.30", "releaseDate": "2024-04-17", "eol": "2025-06-28", "latest": "1.30.14"},
    {"cycle": "1.29", "releaseDate": "2023-12-13", "eol": "2025-02-28", "latest": "1.29.14"},
]

SAMPLE_SIG_README = """
| **v1.36.0 released**  | Branch Manager | Wednesday 22nd April 2026  | week 15  |
"""


# --- Intent 1: Output shows predictions ---

class TestIntent1Predictions:
    """Quickly see when the next change in exam release is to be expected."""

    def test_prediction_row_present(self):
        """Each cert table must contain at least one predicted (~) row."""
        rows = [
            ("1.36", date(2026, 4, 22), date(2026, 6, 30), True, True, True),
            ("1.35", date(2025, 12, 17), date(2026, 3, 3), True, False, False),
        ]
        lines = tracker.format_table("CKA", rows, 69, "Tue", date(2026, 3, 17))
        text = "\n".join(lines)
        assert "~2026-06-30" in text, "Missing predicted switch date"
        assert "~Tue" in text, "Missing predicted day-of-week"

    def test_overdue_column_appears_when_predicted_date_passed(self):
        """If a predicted switch date is in the past, show an Overdue column."""
        today = date(2026, 3, 17)
        rows = [
            ("1.35", date(2025, 12, 17), date(2026, 2, 24), True, False, True),
            ("1.34", date(2025, 8, 27), date(2025, 10, 30), True, False, False),
        ]
        lines = tracker.format_table("CKS", rows, 68, "Tue", today)
        text = "\n".join(lines)
        assert "Overdue" in text
        assert "~21" in text, "Expected ~21 days overdue"

    def test_no_overdue_column_when_not_needed(self):
        """Overdue column must not appear when all predicted dates are in the future."""
        today = date(2026, 3, 17)
        rows = [
            ("1.36", date(2026, 4, 22), date(2026, 6, 30), True, True, True),
            ("1.35", date(2025, 12, 17), date(2026, 3, 3), True, False, False),
        ]
        lines = tracker.format_table("CKA", rows, 69, "Tue", today)
        text = "\n".join(lines)
        assert "Overdue" not in text


# --- Intent 1: Output structure ---

class TestIntent1Structure:
    """Output is valid markdown that renders correctly."""

    def test_table_has_header_separator_and_data(self):
        rows = [
            ("1.35", date(2025, 12, 17), date(2026, 3, 3), True, False, False),
        ]
        lines = tracker.format_table("CKA", rows, 69, "Tue", date(2026, 3, 17))
        # Find table lines (starting with |)
        table_lines = [l for l in lines if l.startswith("|")]
        assert len(table_lines) >= 3, "Need header + separator + at least 1 data row"
        assert table_lines[1].startswith("|:"), "Second line must be alignment separator"

    def test_columns_present(self):
        rows = [
            ("1.35", date(2025, 12, 17), date(2026, 3, 3), True, False, False),
        ]
        lines = tracker.format_table("CKA", rows, 69, "Tue", date(2026, 3, 17))
        header = [l for l in lines if l.startswith("|")][0]
        assert "K8s GA" in header
        assert "CKA Switch" in header
        assert "Day" in header
        assert "Days" in header

    def test_legend_present(self):
        rows = [
            ("1.35", date(2025, 12, 17), date(2026, 3, 3), True, False, False),
        ]
        lines = tracker.format_table("CKA", rows, 69, "Tue", date(2026, 3, 17))
        text = "\n".join(lines)
        assert "~ Predicted:" in text
        assert "EOL" in text


# --- Intent 2: Schema validation catches bad data ---

class TestIntent2Validation:
    """Schema validation detects API changes early."""

    def test_valid_endoflife_response(self):
        tracker.validate_endoflife(SAMPLE_ENDOFLIFE)  # should not raise

    def test_endoflife_rejects_empty(self):
        with pytest.raises(ValueError, match="4\\+ items"):
            tracker.validate_endoflife([])

    def test_endoflife_rejects_missing_keys(self):
        bad = [{"cycle": "1.35"}, {"cycle": "1.34"}, {"cycle": "1.33"}, {"cycle": "1.32"}]
        with pytest.raises(ValueError, match="missing keys"):
            tracker.validate_endoflife(bad)

    def test_endoflife_rejects_non_list(self):
        with pytest.raises(ValueError, match="Expected list"):
            tracker.validate_endoflife({"error": "not found"})

    def test_valid_commits_response(self):
        sample = [{"commit": {"committer": {"date": "2026-03-03T18:12:56Z"}}}]
        tracker.validate_commits(sample)  # should not raise

    def test_commits_rejects_missing_date(self):
        bad = [{"commit": {"author": {}}}]
        with pytest.raises(ValueError, match="committer date"):
            tracker.validate_commits(bad)

    def test_commits_rejects_non_list(self):
        with pytest.raises(ValueError, match="Expected list"):
            tracker.validate_commits("not a list")


# --- Intent 2: Exit codes for workflow ---

class TestIntent2ExitCodes:
    """Script exits with correct codes so the workflow knows what to do."""

    def test_exit_2_when_no_versions(self):
        """Critical failure when K8s versions can't be fetched."""
        tracker._errors.clear()
        with patch.object(tracker, "released_versions", return_value=None):
            output, code = tracker.generate(today=date(2026, 3, 17))
        assert code == 2
        assert output is None

    def test_exit_0_when_all_ok(self):
        """Success when all data is available."""
        tracker._errors.clear()

        def mock_cert_switch(cert, minor):
            # Return plausible dates for each cert+version
            base = {"1.35": date(2026, 3, 3), "1.34": date(2025, 10, 28)}.get(minor)
            return base

        with patch.object(tracker, "released_versions", return_value=SAMPLE_ENDOFLIFE), \
             patch.object(tracker, "cert_switch_date", side_effect=mock_cert_switch), \
             patch.object(tracker, "next_release_date", return_value=date(2026, 4, 22)):
            output, code = tracker.generate(today=date(2026, 3, 17))

        assert code == 0
        assert output is not None
        assert "### CKA" in output
        assert "### CKAD" in output
        assert "### CKS" in output


# --- Pure function tests ---

class TestPureFunctions:
    def test_nearest_weekday_same_day(self):
        # 2026-06-30 is a Tuesday (weekday=1)
        assert tracker.nearest_weekday(date(2026, 6, 30), 1) == date(2026, 6, 30)

    def test_nearest_weekday_snap_forward(self):
        # 2026-06-29 (Mon) → nearest Tue = 2026-06-30
        assert tracker.nearest_weekday(date(2026, 6, 29), 1) == date(2026, 6, 30)

    def test_nearest_weekday_snap_backward(self):
        # 2026-07-01 (Wed) → nearest Tue = 2026-06-30
        assert tracker.nearest_weekday(date(2026, 7, 1), 1) == date(2026, 6, 30)

    def test_predict_switch(self):
        deltas = [76, 62, 71, 68]
        switch_dates = [date(2026, 3, 3), date(2025, 10, 28), date(2025, 7, 3), date(2025, 2, 17)]
        ga = date(2026, 4, 22)
        result, avg_lag, _ = tracker.predict_switch(ga, deltas, switch_dates)
        assert avg_lag == 69
        # 2026-04-22 + 69d = 2026-06-30 (Tue), already nearest Tue
        assert result == date(2026, 6, 30)

    def test_parse_ordinal_date(self):
        assert tracker._parse_ordinal_date("22nd April 2026") == date(2026, 4, 22)
        assert tracker._parse_ordinal_date("1st January 2025") == date(2025, 1, 1)
        assert tracker._parse_ordinal_date("3rd March 2026") == date(2026, 3, 3)


# --- Tactic D: Fallback parsers ---

class TestTacticDFallbacks:
    """Multiple fallback parsers reduce chance of silent failure."""

    def test_next_release_date_table_format(self):
        """Primary: parse table row format."""
        with patch.object(tracker, "fetch_raw", return_value=SAMPLE_SIG_README):
            result = tracker.next_release_date("1.36")
        assert result == date(2026, 4, 22)

    def test_next_release_date_bullet_format(self):
        """Fallback: parse bullet point format."""
        bullet = "- **Wednesday 22nd April 2026**: Week 15 — Kubernetes v1.36.0 released"
        with patch.object(tracker, "fetch_raw", return_value=bullet):
            result = tracker.next_release_date("1.36")
        assert result == date(2026, 4, 22)

    def test_cks_filename_patterns(self):
        """CKS has 3 filename patterns tried in order."""
        assert len(tracker.CERT_FILE_PATTERNS["CKS"]) >= 3

    def test_cert_switch_falls_back_to_contents_listing(self):
        """When pattern-based lookup fails, contents listing is tried."""
        contents = [{"name": "CKA_Curriculum_v1.35.pdf"}]
        commits = [{"commit": {"committer": {"date": "2026-03-03T18:12:56Z"}}}]

        def mock_fetch(url):
            if "contents" in url:
                return contents
            return commits

        with patch.object(tracker, "_cert_switch_from_patterns", return_value=None), \
             patch.object(tracker, "fetch_json", side_effect=mock_fetch):
            result = tracker.cert_switch_date("CKA", "1.35")

        assert result == date(2026, 3, 3)
