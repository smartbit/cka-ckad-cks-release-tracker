# Plan: Detect Mid-Version Curriculum Updates

## Context

The tracker detects topic changes **between** K8s versions (e.g., v1.32 → v1.33) but misses **intra-version** curriculum updates — where CNCF silently updates a PDF while the exam still runs on that version.

**Real example:** CKS v1.32 curriculum was initially committed 2025-02-25. On 2025-04-08, the PDF was updated to add Istio alongside Cilium. For ~3 months (until CKS 1.33 switch on 2025-07-03), candidates faced a modified curriculum the tracker never flagged.

This is an Intent 2 gap: "Warn for changes in the topics."

## Key insight

`_cert_switch_from_patterns()` (line 188) already fetches the full commit list per file via the GitHub API. It uses only `commits[-1]` (oldest) and discards `commits[0]` (newest) and `len(commits)`. **Detection is free — zero additional API calls.**

## Decisions

- ✅ Include `revision_date` in tracker.json alongside `curriculum_revised`
- ✅ Flag current version only in tracker.json (README footnotes show all historical)
- ✅ False positives from cosmetic renames acceptable in Phase 1 (PyMuPDF diffing in CI mitigates)

## Approach: Solution D (Hybrid)

Detect mid-version revisions from already-fetched commit data (free). Diff the initial vs latest PDF when PyMuPDF is available (0-2 extra downloads, only for revised versions).

### Changes

**1. Refactor `_cert_switch_from_patterns()` return type** (line 188)

Currently returns `date | None`. Change to return a dict:
```python
{"switch_date": date, "last_updated": date, "commit_count": int}
```
- `switch_date` = `commits[-1]` date (unchanged)
- `last_updated` = `commits[0]` date (currently discarded)
- `commit_count` = `len(commits)`

Same change for `_cert_switch_from_contents()` (line 204).

Update `cert_switch_date()` (line 172) to return same structure.

**2. Propagate revision info through `build_cert_data()`** (line 329)

Collect `{version: {"last_updated": date, "commit_count": int}}` in a parallel dict alongside the row tuples. Return it as a third element from `build_cert_data()`.

**3. Add `curriculum_revised` + `revision_date` to tracker.json**

For the **current** version only:
```json
"CKS": {
  "version": "1.34",
  "topics_changed": false,
  "curriculum_revised": true,
  "revision_date": "2025-04-08",
  "overdue": true,
  ...
}
```

When not revised: `"curriculum_revised": false, "revision_date": null`.

`curriculum_revised` is separate from `topics_changed` — different semantics:
- `topics_changed`: curriculum changed at version switch boundary (expected)
- `curriculum_revised`: curriculum silently updated mid-cycle (unexpected, warrants warning)

**4. Surface in README footnotes**

Reuse the existing superscript footnote system. For revised versions:
```
⁵ v1.32 curriculum revised 2025-04-08 (initially published 2025-02-25)
```

With PyMuPDF available (CI), also show what changed:
```
⁵ v1.32 curriculum revised 2025-04-08: Added: Istio alongside Cilium
```

This requires downloading the PDF at the initial commit SHA and at the latest commit SHA, then diffing. URL pattern:
```
https://raw.githubusercontent.com/cncf/curriculum/{commit_sha}/{filename}
```

**5. Update tests**

- All tests mocking `cert_switch_date` need updating for dict return type
- New test class `TestMidVersionRevision`:
  - Detection from commit count
  - `curriculum_revised` + `revision_date` in tracker_data
  - Footnote generation for revised versions
  - Graceful handling when commit_count == 1 (no revision)

### Files to modify

- `cka-ckad-cks-release-tracker.py` — `_cert_switch_from_patterns()`, `_cert_switch_from_contents()`, `cert_switch_date()`, `build_cert_data()`, `build_topic_footnotes()`, `generate()`
- `tests/test_tracker.py` — update mocks, add new test class

### What does NOT change

- `.github/workflows/daily.yml` — already installs PyMuPDF
- `tracker.json` schema is additive (new fields, no breaking changes)
- Cross-version diff logic (`diff_curricula()`) is untouched

## Pros & Cons

**Pros:**
- ⬜ Zero additional API calls for detection
- ⬜ Fully stateless — no new persistence
- ⬜ Follows existing patterns (superscript footnotes, HAS_FITZ graceful degradation)
- ⬜ Separate `curriculum_revised` field won't confuse existing `topics_changed` consumers
- ⬜ With PyMuPDF in CI, candidates see exactly what changed

**Cons:**
- ⬜ Changing `cert_switch_date()` return type ripples to callers and test mocks
- ⬜ `commit_count > 1` may include cosmetic changes (renames, metadata) — PyMuPDF diffing mitigates
- ⬜ Downloading PDFs at specific commit SHAs is a new URL pattern to maintain

## Verification

1. `python3 -m pytest tests/test_tracker.py -v` — all tests pass
2. `python3 cka-ckad-cks-release-tracker.py` — CKS v1.32 shows revision footnote
3. `cat tracker.json` — `curriculum_revised` + `revision_date` fields present per cert
4. `gh workflow run daily.yml` — CI run succeeds with updated output
