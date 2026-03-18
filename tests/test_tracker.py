"""
Tests for cka-ckad-cks-release-tracker.

Intent 1: Quickly see when the next change in exam release is to be expected.
Intent 2: Warn for changes in the topics.
Intent 3: Low maintenance — detect failures, archive after 30 days.
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

    def test_marker_on_switch_date(self):
        """Superscript marker appears next to the switch date with a space."""
        rows = [
            ("1.32", date(2024, 12, 11), date(2025, 2, 17), False, False, False),
        ]
        markers = {"1.32": "¹"}
        lines = tracker.format_table("CKA", rows, 69, "Tue", date(2026, 3, 17), markers)
        text = "\n".join(lines)
        assert "2025-02-17 ¹" in text

    def test_no_marker_when_unchanged(self):
        """No superscript on switch dates without topic changes."""
        rows = [
            ("1.33", date(2025, 4, 23), date(2025, 7, 3), True, False, False),
        ]
        lines = tracker.format_table("CKA", rows, 69, "Tue", date(2026, 3, 17))
        text = "\n".join(lines)
        assert "2025-07-03" in text
        assert "¹" not in text


# --- Intent 2: Warn for changes in the topics ---

class TestIntent2TopicChanges:
    """Warn for changes in the topics."""

    def test_match_cert_version_cka(self):
        assert tracker._match_cert_version("CKA", "CKA_Curriculum_v1.35.pdf") == "1.35"

    def test_match_cert_version_cks_space(self):
        assert tracker._match_cert_version("CKS", "CKS_Curriculum v1.31.pdf") == "1.31"

    def test_match_cert_version_no_match(self):
        assert tracker._match_cert_version("CKA", "CKAD_Curriculum_v1.35.pdf") is None
        assert tracker._match_cert_version("CKA", "readme.txt") is None

    def test_identical_shas_skips_download(self):
        """Method 6: identical blob SHAs → 'identical' without downloading PDFs."""
        shas = {
            "1.32": ("abc123", "CKA_Curriculum_v1.32.pdf"),
            "1.33": ("abc123", "CKA_Curriculum_v1.33.pdf"),
            "1.34": ("abc123", "CKA_Curriculum_v1.34.pdf"),
        }
        with patch.object(tracker, "get_curriculum_shas", return_value=shas), \
             patch.object(tracker, "download_pdf") as mock_dl:
            results, _ = tracker.diff_curricula("CKA", ["1.32", "1.33", "1.34"])
        mock_dl.assert_not_called()
        assert len(results) == 2
        assert results[0] == ("1.32", "1.33", "identical", [])
        assert results[1] == ("1.33", "1.34", "identical", [])

    def test_different_shas_without_pymupdf(self):
        """When SHAs differ but PyMuPDF is missing, status is 'changed-no-detail'."""
        shas = {
            "1.31": ("aaa", "old-versions/CKA_Curriculum_v1.31.pdf"),
            "1.32": ("bbb", "CKA_Curriculum_v1.32.pdf"),
        }
        with patch.object(tracker, "get_curriculum_shas", return_value=shas), \
             patch.object(tracker, "HAS_FITZ", False):
            results, _ = tracker.diff_curricula("CKA", ["1.31", "1.32"])
        assert results[0][2] == "changed-no-detail"

    def test_missing_sha_is_unavailable(self):
        """If a version's SHA can't be fetched, report 'unavailable'."""
        shas = {"1.31": ("aaa", "old-versions/CKA_Curriculum_v1.31.pdf")}
        with patch.object(tracker, "get_curriculum_shas", return_value=shas):
            results, _ = tracker.diff_curricula("CKA", ["1.31", "1.32"])
        assert results[0][2] == "unavailable"

    def test_count_changes(self):
        diff_lines = ["--- a", "+++ b", "@@ -1 +1 @@", "-old", "+new", " context"]
        assert tracker._count_changes(diff_lines) == 2

    def test_extract_topic_changes(self):
        diff_lines = [
            "--- v1.32", "+++ v1.33", "@@ -1 +1 @@",
            "-• Kuztomize", "+• Kustomize",
        ]
        added, removed = tracker._extract_topic_changes(diff_lines)
        assert removed == ["Kuztomize"]
        assert added == ["Kustomize"]

    def test_footnotes_markers_in_table_order(self):
        """Footnote numbers follow table row order (top to bottom)."""
        diffs = [
            ("1.31", "1.32", "changed-no-detail", []),
            ("1.32", "1.33", "changed-no-detail", []),
        ]
        file_info = {
            "1.31": ("a", "old-versions/CKA_Curriculum_v1.31.pdf"),
            "1.32": ("b", "CKA_Curriculum_v1.32.pdf"),
            "1.33": ("c", "CKA_Curriculum_v1.33.pdf"),
        }
        row_order = ["1.36", "1.35", "1.34", "1.33", "1.32", "1.31", "1.30", "1.29"]
        markers, footnotes, n = tracker.build_topic_footnotes("CKA", diffs, file_info, row_order)
        assert markers["1.33"] == "¹"  # appears first in table
        assert markers["1.32"] == "²"  # appears second
        assert len(footnotes) == 2
        assert footnotes[0].startswith("¹")
        assert footnotes[1].startswith("²")
        assert n == 2

    def test_footnotes_global_numbering(self):
        """Footnote numbers continue from start parameter for global numbering."""
        diffs = [("1.31", "1.32", "changed-no-detail", [])]
        file_info = {
            "1.31": ("a", "old-versions/CKAD_Curriculum_v1.31.pdf"),
            "1.32": ("b", "CKAD_Curriculum_v1.32.pdf"),
        }
        row_order = ["1.32"]
        markers, footnotes, n = tracker.build_topic_footnotes("CKAD", diffs, file_info, row_order, start=2)
        assert markers["1.32"] == "³"  # third superscript (0-indexed: ¹²³)
        assert footnotes[0].startswith("³")
        assert n == 3

    def test_footnotes_major_shows_links(self):
        """Major changes show PDF links in footnotes."""
        diffs = [("1.31", "1.32", "changed", [f"+line{i}" for i in range(20)])]
        file_info = {
            "1.31": ("a", "old-versions/CKA_Curriculum_v1.31.pdf"),
            "1.32": ("b", "CKA_Curriculum_v1.32.pdf"),
        }
        row_order = ["1.32"]
        markers, footnotes, _ = tracker.build_topic_footnotes("CKA", diffs, file_info, row_order)
        assert "1.32" in markers
        assert "github.com" in footnotes[0]
        assert "v1.31 curriculum" in footnotes[0]

    def test_footnotes_small_inline(self):
        """Small changes show compact inline text in a single footnote line."""
        diffs = [("1.32", "1.33", "changed", [
            "--- v1.32", "+++ v1.33", "@@ -1 +1 @@",
            "-• Kuztomize", "+• Kustomize",
        ])]
        file_info = {
            "1.32": ("a", "CKAD_Curriculum_v1.32.pdf"),
            "1.33": ("b", "CKAD_Curriculum_v1.33.pdf"),
        }
        row_order = ["1.33"]
        markers, footnotes, _ = tracker.build_topic_footnotes("CKAD", diffs, file_info, row_order)
        assert "1.33" in markers
        assert "Removed: Kuztomize" in footnotes[0]
        assert "Added: Kustomize" in footnotes[0]
        assert len(footnotes) == 1

    def test_footnotes_skip_identical(self):
        """Identical pairs get no markers."""
        diffs = [("1.33", "1.34", "identical", [])]
        row_order = ["1.34"]
        markers, footnotes, n = tracker.build_topic_footnotes("CKA", diffs, {}, row_order)
        assert markers == {}
        assert footnotes == []
        assert n == 0

    def test_footnotes_skip_unavailable(self):
        """Unavailable pairs get no markers."""
        diffs = [("1.29", "1.30", "unavailable", [])]
        row_order = ["1.30"]
        markers, footnotes, n = tracker.build_topic_footnotes("CKA", diffs, {}, row_order)
        assert markers == {}
        assert footnotes == []
        assert n == 0

    def test_footnotes_changed_no_detail_shows_links(self):
        """changed-no-detail (no PyMuPDF) still shows PDF links."""
        diffs = [("1.31", "1.32", "changed-no-detail", [])]
        file_info = {
            "1.31": ("aaa", "old-versions/CKA_Curriculum_v1.31.pdf"),
            "1.32": ("bbb", "CKA_Curriculum_v1.32.pdf"),
        }
        row_order = ["1.32"]
        markers, footnotes, _ = tracker.build_topic_footnotes("CKA", diffs, file_info, row_order)
        assert "github.com" in footnotes[0]

    def test_format_diff_output_detailed(self):
        """--diff format shows full diff in code blocks."""
        diff_lines = ["--- v1.31", "+++ v1.32", "@@ -1 +1 @@", "-old", "+new"]
        diffs = [("1.31", "1.32", "changed", diff_lines)]
        lines = tracker.format_diff_output("CKA", diffs)
        text = "\n".join(lines)
        assert "```diff" in text
        assert "-old" in text
        assert "+new" in text

    def test_format_diff_output_changed_no_detail(self):
        """--diff format renders changed-no-detail like unavailable."""
        diffs = [("1.31", "1.32", "changed-no-detail", [])]
        lines = tracker.format_diff_output("CKA", diffs)
        text = "\n".join(lines)
        assert "*PDF not available*" in text

    def test_br_after_legend_when_footnotes_exist(self):
        """Legend line gets <br> when footnotes follow."""
        tracker._errors.clear()

        def mock_cert_switch(cert, minor):
            return {"1.35": date(2026, 3, 3), "1.34": date(2025, 10, 28)}.get(minor)

        def mock_diff(cert, versions):
            if cert == "CKA":
                return [("1.31", "1.32", "changed-no-detail", [])], {
                    "1.31": ("a", "old-versions/CKA_Curriculum_v1.31.pdf"),
                    "1.32": ("b", "CKA_Curriculum_v1.32.pdf"),
                }
            return [], {}

        with patch.object(tracker, "released_versions", return_value=SAMPLE_ENDOFLIFE), \
             patch.object(tracker, "cert_switch_date", side_effect=mock_cert_switch), \
             patch.object(tracker, "next_release_date", return_value=date(2026, 4, 22)), \
             patch.object(tracker, "diff_curricula", side_effect=mock_diff):
            output, code = tracker.generate(today=date(2026, 3, 17))

        # CKA has a footnote → legend should end with <br>
        lines = output.split("\n")
        for i, line in enumerate(lines):
            if line.startswith("~ Predicted:") and line.endswith("<br>"):
                # Next line should be a footnote
                assert lines[i + 1].startswith("¹"), f"Expected footnote after legend<br>, got: {lines[i + 1]}"
                break
        else:
            pytest.fail("No legend line with <br> found in output")

    def test_no_br_when_no_footnotes(self):
        """Legend line has no <br> when there are no footnotes."""
        tracker._errors.clear()

        def mock_cert_switch(cert, minor):
            return {"1.35": date(2026, 3, 3), "1.34": date(2025, 10, 28)}.get(minor)

        with patch.object(tracker, "released_versions", return_value=SAMPLE_ENDOFLIFE), \
             patch.object(tracker, "cert_switch_date", side_effect=mock_cert_switch), \
             patch.object(tracker, "next_release_date", return_value=date(2026, 4, 22)), \
             patch.object(tracker, "diff_curricula", return_value=([], {})):
            output, code = tracker.generate(today=date(2026, 3, 17))

        for line in output.split("\n"):
            if line.startswith("~ Predicted:"):
                assert not line.endswith("<br>"), f"Legend should not have <br> without footnotes: {line}"

    def test_br_between_multiple_footnotes(self):
        """Non-final footnotes get <br>, last footnote does not."""
        tracker._errors.clear()

        def mock_cert_switch(cert, minor):
            return {"1.35": date(2026, 3, 3), "1.34": date(2025, 10, 28)}.get(minor)

        def mock_diff(cert, versions):
            if cert == "CKA":
                return [
                    ("1.31", "1.32", "changed-no-detail", []),
                    ("1.32", "1.33", "changed-no-detail", []),
                ], {
                    "1.31": ("a", "old-versions/CKA_Curriculum_v1.31.pdf"),
                    "1.32": ("b", "CKA_Curriculum_v1.32.pdf"),
                    "1.33": ("c", "CKA_Curriculum_v1.33.pdf"),
                }
            return [], {}

        with patch.object(tracker, "released_versions", return_value=SAMPLE_ENDOFLIFE), \
             patch.object(tracker, "cert_switch_date", side_effect=mock_cert_switch), \
             patch.object(tracker, "next_release_date", return_value=date(2026, 4, 22)), \
             patch.object(tracker, "diff_curricula", side_effect=mock_diff):
            output, code = tracker.generate(today=date(2026, 3, 17))

        lines = output.split("\n")
        # Find the CKA footnotes (¹ and ²)
        fn_lines = [l for l in lines if l.startswith("¹") or l.startswith("²")]
        assert len(fn_lines) == 2, f"Expected 2 CKA footnotes, got {fn_lines}"
        assert fn_lines[0].endswith("<br>"), "First footnote should end with <br>"
        assert not fn_lines[1].endswith("<br>"), "Last footnote should not end with <br>"


# --- Intent 3: Schema validation catches bad data ---

class TestIntent3Validation:
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


# --- Intent 3: Exit codes for workflow ---

class TestIntent3ExitCodes:
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
            base = {"1.35": date(2026, 3, 3), "1.34": date(2025, 10, 28)}.get(minor)
            return base

        with patch.object(tracker, "released_versions", return_value=SAMPLE_ENDOFLIFE), \
             patch.object(tracker, "cert_switch_date", side_effect=mock_cert_switch), \
             patch.object(tracker, "next_release_date", return_value=date(2026, 4, 22)), \
             patch.object(tracker, "diff_curricula", return_value=([], {})):
            output, code = tracker.generate(today=date(2026, 3, 17))

        assert code == 0
        assert output is not None
        assert "### CKA" in output
        assert "### CKAD" in output
        assert "### CKS" in output
        assert "EOL" in output  # global footer


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

    def test_filter_outliers_normal_data(self):
        """Normal data passes through unchanged."""
        recent = [64, 71, 76, 63]
        reference = [64, 71, 76, 63, 55, 62]
        assert tracker.filter_outliers(recent, reference) == [64, 71, 76, 63]

    def test_filter_outliers_removes_extreme(self):
        """A value beyond μ+2σ of the reference set is excluded."""
        # reference: μ=65.2, σ=6.7, upper=78.6
        reference = [64, 71, 76, 63, 55, 62]
        recent = [95, 64, 71, 76]
        filtered = tracker.filter_outliers(recent, reference)
        assert 95 not in filtered
        assert filtered == [64, 71, 76]

    def test_filter_outliers_too_few_reference(self):
        """With < 3 reference points, no filtering occurs."""
        assert tracker.filter_outliers([90, 60], [90, 60]) == [90, 60]

    def test_filter_outliers_all_identical(self):
        """Zero std → no filtering."""
        assert tracker.filter_outliers([50, 50], [50, 50, 50]) == [50, 50]

    def test_filter_outliers_fallback_if_all_removed(self):
        """If filtering would remove everything, return original."""
        # All values are outliers relative to a very different reference
        recent = [200, 210]
        reference = [50, 51, 52, 53, 54, 55]  # upper bound ~59
        result = tracker.filter_outliers(recent, reference)
        assert result == [200, 210]


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
