#!/usr/bin/env python3
"""
Generates markdown tracking Kubernetes cert exam release adoption.

Intent 1: Quickly see when the next change in exam release is to be expected.
Intent 2: Warn for changes in the topics.
Intent 3: Machine-readable output for downstream automation.
Intent 4: Low maintenance — detect failures, archive after 30 days.

Data sources (with fallbacks — Tactic D):
  - endoflife.date/api/kubernetes.json     → fallback: GitHub kubernetes/kubernetes releases API
  - GitHub API (cncf/curriculum) by path   → fallback: list repo contents + regex match
  - GitHub API (kubernetes/sig-release)    → fallback: bullet format, then estimation

Cross-validation (Tactic E):
  - Linux Foundation FAQ page              → validates current cert versions in tracker.json

Schema validation (Tactic C):
  - Validates API responses before use
  - Collects errors, reports to stderr
  - Exit codes: 0=ok, 1=degraded, 2=critical failure

Caching:
  - diff-cache.json stores PDF diff results keyed by immutable git blob/commit
    SHAs, so entries never go stale; committed by CI alongside tracker.json

Requires: python 3.9+, gh CLI (optional, used for authenticated GitHub API).
Optional: pymupdf (for detailed topic change extraction).
"""

import base64
import json
import re
import shutil
import subprocess
import sys
from collections import Counter
from datetime import date, datetime, timedelta
from urllib.error import URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

ENDOFLIFE_URL = "https://endoflife.date/api/kubernetes.json"
CURRICULUM_REPO = "https://api.github.com/repos/cncf/curriculum"
CURRICULUM_COMMITS = f"{CURRICULUM_REPO}/commits"
CURRICULUM_CONTENTS = f"{CURRICULUM_REPO}/contents/"
K8S_RELEASES = "https://api.github.com/repos/kubernetes/kubernetes/releases?per_page=100"
SIG_RELEASE_README = "https://api.github.com/repos/kubernetes/sig-release/contents/releases/release-{}/README.md"
LF_FAQ_URL = "https://docs.linuxfoundation.org/tc-docs/certification/faq-cka-ckad-cks"
UA = "cka-ckad-cks-release-tracker/1.0"

CERTS = ("CKA", "CKAD", "CKS")
HISTORICAL = 7  # released versions to show (supported + recent unsupported)
PREDICTION_WINDOW = 4  # last N releases used for average lag
OUTLIER_SIGMA = 2.0  # exclude deltas beyond μ ± 2σ from prediction


# Filename patterns per cert (tried in order — Tactic D)
CERT_FILE_PATTERNS = {
    "CKA": ["CKA_Curriculum_v{v}.pdf"],
    "CKAD": [
        "CKAD_Curriculum_v{v}.pdf",
        "CKAD_Curriculum_ v{v}.pdf",
    ],
    "CKS": [
        "CKS_Curriculum v{v}.pdf",       # 1.31+: space before version
        "CKS_Curriculum_ v{v}.pdf",      # 1.28–1.30: underscore + space
        "CKS_Curriculum_v{v}.pdf",       # older: underscore only
    ],
}

# --- Error tracking ---

_errors = []


def log_error(source, msg):
    _errors.append({"source": source, "message": str(msg)})
    print(f"WARNING: [{source}] {msg}", file=sys.stderr)


# --- Schema validation (Tactic C) ---

def validate_endoflife(data):
    """Validate endoflife.date response structure."""
    if not isinstance(data, list) or len(data) < 4:
        raise ValueError(f"Expected list with 4+ items, got {type(data).__name__} len={len(data) if isinstance(data, list) else 'N/A'}")
    required = {"cycle", "releaseDate", "eol"}
    for i, item in enumerate(data[:8]):
        missing = required - set(item.keys())
        if missing:
            raise ValueError(f"Item {i} (cycle={item.get('cycle','?')}) missing keys: {missing}")


def validate_commits(data):
    """Validate GitHub commits response structure."""
    if not isinstance(data, list):
        raise ValueError(f"Expected list, got {type(data).__name__}")
    for item in data[:3]:
        try:
            _ = item["commit"]["committer"]["date"]
        except (KeyError, TypeError) as e:
            raise ValueError(f"Commit missing committer date: {e}")


# --- HTTP helpers ---

def fetch_json(url):
    """Fetch JSON, preferring gh CLI for GitHub URLs (authenticated, higher rate limit)."""
    if "api.github.com" in url and shutil.which("gh"):
        path = url.replace("https://api.github.com/", "")
        r = subprocess.run(["gh", "api", path], capture_output=True, text=True, timeout=30)
        if r.returncode == 0:
            return json.loads(r.stdout)
    req = Request(url, headers={"User-Agent": UA})
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def fetch_raw(url):
    """Fetch raw text content via GitHub contents API."""
    if "api.github.com" in url and shutil.which("gh"):
        path = url.replace("https://api.github.com/", "")
        r = subprocess.run(["gh", "api", path], capture_output=True, text=True, timeout=30)
        if r.returncode == 0:
            data = json.loads(r.stdout)
            return base64.b64decode(data["content"]).decode()
    req = Request(url, headers={"User-Agent": UA})
    with urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read())
        return base64.b64decode(data["content"]).decode()


# --- Data fetchers with fallbacks (Tactic D) ---

def released_versions():
    """Get K8s versions. Primary: endoflife.date, fallback: GitHub releases API."""
    # Primary
    try:
        data = fetch_json(ENDOFLIFE_URL)
        validate_endoflife(data)
        return data
    except Exception as e:
        log_error("endoflife.date", e)

    # Fallback: derive from GitHub releases
    try:
        return _released_versions_from_github()
    except Exception as e:
        log_error("github-k8s-releases-fallback", e)

    return None


def _released_versions_from_github():
    """Derive K8s version info from GitHub releases API (fallback)."""
    releases = fetch_json(K8S_RELEASES)
    versions = {}
    for r in releases:
        if r.get("prerelease") or r.get("draft"):
            continue
        m = re.match(r"v(\d+\.\d+)\.0$", r["tag_name"])
        if m:
            minor = m.group(1)
            rel_date = r["published_at"][:10]
            # Estimate EOL: ~14 months after .0 release
            rd = date.fromisoformat(rel_date)
            eol = (rd + timedelta(days=426)).isoformat()  # 14 months ≈ 426 days
            versions[minor] = {"cycle": minor, "releaseDate": rel_date, "eol": eol}
    result = sorted(versions.values(),
                    key=lambda v: [int(x) for x in v["cycle"].split(".")], reverse=True)
    validate_endoflife(result)
    return result


def _parse_commit_dates(commits):
    """Extract dates and SHAs from a commits list (newest first)."""
    dates = []
    shas = []
    for c in commits:
        ts = c["commit"]["committer"]["date"]
        dates.append(datetime.fromisoformat(ts.replace("Z", "+00:00")).date())
        shas.append(c["sha"])
    return dates, shas


def cert_switch_date(cert, minor):
    """Get cert switch info. Primary: commits by filename. Fallback: list contents + regex.

    Returns dict with switch_date, last_updated, commit_count, filename,
    first_sha, latest_sha, commit_dates, commit_shas — or None.
    """
    # Primary: try known filename patterns
    result = _cert_switch_from_patterns(cert, minor)
    if result:
        return result

    # Fallback: list repo contents, regex match, then get commits
    try:
        return _cert_switch_from_contents(cert, minor)
    except Exception as e:
        log_error(f"cert-switch-{cert}-{minor}-fallback", e)

    return None


def _cert_switch_from_patterns(cert, minor):
    """Try known filename patterns to find the cert switch commit.

    Returns dict with switch_date, last_updated, commit_count, filename,
    first_sha, latest_sha, commit_dates, commit_shas — or None.
    """
    for pattern in CERT_FILE_PATTERNS[cert]:
        filename = pattern.format(v=minor)
        url = f"{CURRICULUM_COMMITS}?path={quote(filename)}"
        try:
            commits = fetch_json(url)
            if commits:
                validate_commits(commits)
                dates, shas = _parse_commit_dates(commits)
                return {
                    "switch_date": dates[-1],
                    "last_updated": dates[0],
                    "commit_count": len(commits),
                    "filename": filename,
                    "first_sha": shas[-1],
                    "latest_sha": shas[0],
                    "commit_dates": dates,
                    "commit_shas": shas,
                }
        except (URLError, KeyError, IndexError, ValueError):
            continue
    return None


def _cert_switch_from_contents(cert, minor):
    """Fallback: list repo contents, find file by regex, get its first commit.

    Returns dict with switch_date, last_updated, commit_count, filename,
    first_sha, latest_sha, commit_dates, commit_shas — or None.
    """
    contents = fetch_json(CURRICULUM_CONTENTS)
    if not isinstance(contents, list):
        return None
    # Match any file with cert name and version
    pattern = re.compile(rf"{cert}.*?{re.escape(minor)}\.pdf", re.I)
    for f in contents:
        name = f.get("name", "")
        if pattern.search(name):
            url = f"{CURRICULUM_COMMITS}?path={quote(name)}"
            commits = fetch_json(url)
            if commits:
                validate_commits(commits)
                dates, shas = _parse_commit_dates(commits)
                return {
                    "switch_date": dates[-1],
                    "last_updated": dates[0],
                    "commit_count": len(commits),
                    "filename": name,
                    "first_sha": shas[-1],
                    "latest_sha": shas[0],
                    "commit_dates": dates,
                    "commit_shas": shas,
                }
    return None


def next_release_date(minor):
    """Get next K8s GA date. Primary: table format. Fallback: bullet format, estimation."""
    url = SIG_RELEASE_README.format(minor)
    try:
        readme = fetch_raw(url)
    except (URLError, KeyError) as e:
        log_error(f"sig-release-{minor}", e)
        return None

    # Primary: table row — "**v1.36.0 released** | ... | Wednesday 22nd April 2026"
    table_pattern = rf"\*\*v{re.escape(minor)}\.0 released\*\*.*?(\d+(?:st|nd|rd|th)\s+\w+\s+\d{{4}})"
    match = re.search(table_pattern, readme)
    if match:
        return _parse_ordinal_date(match.group(1))

    # Fallback: bullet — "**Wednesday 22nd April 2026**: ... v1.36.0 released"
    bullet_pattern = rf"\*\*\w+\s+(\d+(?:st|nd|rd|th)\s+\w+\s+\d{{4}})\*\*.*?v{re.escape(minor)}\.0 released"
    match = re.search(bullet_pattern, readme)
    if match:
        return _parse_ordinal_date(match.group(1))

    log_error(f"sig-release-{minor}-parse", "No date pattern matched in README")
    return None


def _parse_ordinal_date(raw):
    """Parse '22nd April 2026' into a date object."""
    # Anchor to the digit so month names keep their letters ("August" contains "st")
    cleaned = re.sub(r"(?<=\d)(st|nd|rd|th)\b", "", raw)
    try:
        return datetime.strptime(cleaned, "%d %B %Y").date()
    except ValueError as e:
        log_error("ordinal-date-parse", e)
        return None


# --- Cross-validation (Tactic E) ---

def _version_key(v):
    """Convert version string to sortable tuple for comparison."""
    return tuple(int(x) for x in v.split("."))


def fetch_faq_versions():
    """Fetch current exam versions from Linux Foundation FAQ page (Tactic E).

    Parses the FAQ page for patterns like:
        "CKA exam environment is currently running Kubernetes v1.35"

    Returns {cert: version_str} or None on failure.
    """
    try:
        req = Request(LF_FAQ_URL, headers={"User-Agent": UA})
        with urlopen(req, timeout=30) as resp:
            html = resp.read().decode()
    except Exception:
        return None

    text = re.sub(r'<[^>]+>', ' ', html)
    versions = {}
    for cert in CERTS:
        m = re.search(
            rf'The\s+{cert}\s+exam\s+environment\s+is\s+currently\s+'
            rf'running\s+Kubernetes\s+v(\d+\.\d+)', text)
        if m:
            versions[cert] = m.group(1)
    return versions if versions else None


# --- Prediction ---

def nearest_weekday(target, weekday):
    """Return the date nearest to target that falls on the given weekday (0=Mon..6=Sun)."""
    diff = (weekday - target.weekday()) % 7
    if diff > 3:
        diff -= 7
    return target + timedelta(days=diff)


def predict_switch(ga_date, deltas, switch_dates):
    """Predict switch date: GA + avg lag, snapped to most frequent weekday."""
    avg_lag = round(sum(deltas) / len(deltas))
    weekdays = Counter(d.weekday() for d in switch_dates)
    most_common_day = weekdays.most_common(1)[0][0]
    raw = ga_date + timedelta(days=avg_lag)
    return nearest_weekday(raw, most_common_day), avg_lag, most_common_day


def filter_outliers(recent, reference, sigma=OUTLIER_SIGMA):
    """Filter recent deltas using Gaussian bounds from reference deltas.

    With n=4 (PREDICTION_WINDOW), the max z-score of any point in its own
    sample is only √3 ≈ 1.73, so a 2σ threshold against the same 4 values
    would never trigger. Bounds are computed from the larger HISTORICAL
    reference set (n=6–7) where 2σ is meaningful.
    """
    if len(reference) < 3:
        return recent
    mu = sum(reference) / len(reference)
    var = sum((x - mu) ** 2 for x in reference) / len(reference)
    std = var ** 0.5
    if std == 0:
        return recent
    lo = mu - sigma * std
    hi = mu + sigma * std
    filtered = [d for d in recent if lo <= d <= hi]
    return filtered if filtered else recent


# --- Table building ---

def _filter_revision_info(revision_info, switch_dates_set):
    """Remove false-positive revisions caused by file moves.

    When CNCF publishes a new version, the old file is moved to
    old-versions/, creating an extra commit. These commits have dates
    matching another version's switch_date and should be excluded.

    Returns filtered revision_info with updated last_updated/latest_sha
    pointing to the most recent genuine revision commit.
    """
    filtered = {}
    for ver, info in revision_info.items():
        # Intermediate dates = all commit dates except the initial publish
        intermediate = list(zip(info["commit_dates"][:-1],
                                info["commit_shas"][:-1]))
        # Keep only dates that don't match any version's switch date
        real = [(d, s) for d, s in intermediate if d not in switch_dates_set]
        if real:
            info = dict(info)
            info["last_updated"] = real[0][0]
            info["latest_sha"] = real[0][1]
            filtered[ver] = info
    return filtered


def build_cert_data(cert, all_versions, next_minor, next_ga, today):
    """Collect switch dates and compute predictions for one cert.

    Returns (rows, avg_lag, day_name, revision_info) where each row is:
        (minor, ga, switch, supported, ga_predicted, sw_predicted)
    and revision_info is {version: switch_info_dict} for versions with
    genuine mid-version curriculum revisions (file moves excluded).
    """
    hist = []
    raw_revision_info = {}
    for v in all_versions[:HISTORICAL]:
        minor = v["cycle"]
        ga = date.fromisoformat(v["releaseDate"])
        info = cert_switch_date(cert, minor)
        switch = info["switch_date"] if info else None
        if info and info["commit_count"] > 1:
            raw_revision_info[minor] = info
        eol = date.fromisoformat(v["eol"]) if isinstance(v["eol"], str) else None
        supported = eol is None or eol > today
        hist.append((minor, ga, switch, supported))

    pairs_with_data = [(ga, sw) for _, ga, sw, _ in hist if sw]
    all_deltas = [(sw - ga).days for ga, sw in pairs_with_data]
    deltas = filter_outliers(all_deltas[:PREDICTION_WINDOW], all_deltas)
    # Weekday vote uses ALL historical switches
    all_switch_dates = [sw for _, sw in pairs_with_data]

    # Next version
    next_info = cert_switch_date(cert, next_minor)
    next_switch = next_info["switch_date"] if next_info else None
    if next_info and next_info["commit_count"] > 1:
        raw_revision_info[next_minor] = next_info
    rows = []
    sw_predicted = False
    if not next_switch and next_ga and deltas:
        next_switch, _, _ = predict_switch(next_ga, deltas, all_switch_dates)
        sw_predicted = True
    rows.append((next_minor, next_ga, next_switch, True, True, sw_predicted))

    # Historical — predict for GA'd versions where cert hasn't switched yet
    for minor, ga, switch, supported in hist:
        sw_predicted = False
        if not switch and ga <= today and deltas:
            switch, _, _ = predict_switch(ga, deltas, all_switch_dates)
            sw_predicted = True
        rows.append((minor, ga, switch, supported, False, sw_predicted))

    # Filter out false-positive revisions from file moves
    switch_dates_set = {sw for _, _, sw, _ in hist if sw}
    if next_info:
        switch_dates_set.add(next_info["switch_date"])
    revision_info = _filter_revision_info(raw_revision_info, switch_dates_set)

    avg_lag = round(sum(deltas) / len(deltas)) if deltas else 0
    day_name = ""
    if all_switch_dates:
        weekdays = Counter(d.weekday() for d in all_switch_dates)
        common_day = weekdays.most_common(1)[0][0]
        day_name = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][common_day]

    return rows, avg_lag, day_name, revision_info


def format_table(cert, rows, avg_lag, day_name, today, markers=None):
    """Format one cert's markdown table. Returns list of lines.
    markers: {version: superscript_char} for topic change footnotes.
    """
    lines = []

    has_overdue = any(
        sw_pred and switch and switch < today
        for _, _, switch, _, _, sw_pred in rows
    )

    lines.append(f"### {cert}")
    lines.append("")

    cert_hdr = f"{cert} Switch"
    h = f"| K8s  | K8s GA      | {cert_hdr:<12}| Day  | Days |"
    s = f"|:-----|:------------|:------------|:----:|:----:|"
    if has_overdue:
        h += " Overdue|"
        s += ":------:|"
    lines.append(h)
    lines.append(s)

    for minor, ga, switch, supported, ga_pred, sw_pred in rows:
        gp = "~" if ga_pred else ""
        sp = "~" if sw_pred else ""
        eol = "*" if not supported else ""
        marker = (markers or {}).get(minor, "")
        ga_str = f"{gp}{ga.isoformat()}{eol}" if ga else "TBD"
        if switch:
            if marker:
                sw_str = f"{sp}{switch.isoformat()} {marker}"
            else:
                sw_str = f"{sp}{switch.isoformat()}"
            day_str = f"{sp}{switch.strftime('%a')}"
            delta = (switch - ga).days if ga else None
            delta_str = f"{sp}{delta}" if delta is not None else "—"
        else:
            sw_str = "—"
            day_str = ""
            delta_str = "—"
        row = f"| {minor:<4} | {ga_str:<11} | {sw_str:<12}| {day_str:<4} | {delta_str:>4} |"
        if has_overdue:
            if sw_pred and switch and switch < today:
                overdue = (today - switch).days
                row += f" ~{overdue:<5}|"
            else:
                row += "        |"
        lines.append(row)

    lines.append("")
    lines.append(f"~ Predicted: K8s GA + {avg_lag}d avg (last {PREDICTION_WINDOW}), "
                 f"nearest {day_name}")

    return lines


# --- Curriculum diff (Intent 2: Warn for changes in topics) ---

CURRICULUM_RAW = "https://raw.githubusercontent.com/cncf/curriculum/master"
CURRICULUM_GITHUB = "https://github.com/cncf/curriculum/blob/master"
MAJOR_DIFF_THRESHOLD = 15  # changed lines — above this, link to PDFs instead
SUPERSCRIPTS = "¹²³⁴⁵⁶⁷⁸⁹"

try:
    import fitz  # PyMuPDF — optional dependency
    HAS_FITZ = True
except ImportError:
    HAS_FITZ = False


# --- SHA-keyed diff cache ---
#
# PDF downloads and text diffs are the expensive part of a run, yet their
# inputs are addressed by git blob/commit SHAs and therefore immutable: the
# same SHA pair always diffs to the same result. Caching by SHA pair never
# goes stale and needs no invalidation. Only definitive results are cached;
# transient failures (download errors, missing PyMuPDF) are retried next run.

DIFF_CACHE_FILE = "diff-cache.json"

_diff_cache = None
_diff_cache_dirty = False


def _load_diff_cache():
    """Load the diff cache from disk (lazily, once). Returns the cache dict."""
    global _diff_cache
    if _diff_cache is None:
        try:
            with open(DIFF_CACHE_FILE) as f:
                data = json.load(f)
        except (OSError, ValueError):
            data = {}
        if not isinstance(data, dict):
            data = {}
        pairs = data.get("pairs")
        revisions = data.get("revisions")
        _diff_cache = {
            "pairs": pairs if isinstance(pairs, dict) else {},
            "revisions": revisions if isinstance(revisions, dict) else {},
        }
    return _diff_cache


def _cache_pair(old_sha, new_sha, status, diff_lines):
    """Store a cross-version diff result keyed by its blob SHA pair."""
    global _diff_cache_dirty
    _load_diff_cache()["pairs"][f"{old_sha}:{new_sha}"] = {
        "status": status,
        "diff_lines": diff_lines,
    }
    _diff_cache_dirty = True


def _cache_revision(first_sha, latest_sha, detail):
    """Store a mid-version revision diff summary keyed by its commit SHA pair."""
    global _diff_cache_dirty
    _load_diff_cache()["revisions"][f"{first_sha}:{latest_sha}"] = detail
    _diff_cache_dirty = True


def save_diff_cache():
    """Write the diff cache to disk if it gained entries this run."""
    if _diff_cache_dirty:
        with open(DIFF_CACHE_FILE, "w") as f:
            json.dump(_diff_cache, f, indent=2)
            f.write("\n")


def get_curriculum_shas(cert):
    """Get blob SHAs and repo paths for all curriculum versions of a cert.
    Returns {version: (sha, repo_path)}.
    """
    info = {}
    try:
        root = fetch_json(CURRICULUM_CONTENTS)
        for f in root:
            ver = _match_cert_version(cert, f.get("name", ""))
            if ver:
                info[ver] = (f["sha"], f["name"])
    except Exception as e:
        log_error(f"shas-root-{cert}", e)
    try:
        old = fetch_json(f"{CURRICULUM_REPO}/contents/old-versions")
        for f in old:
            ver = _match_cert_version(cert, f.get("name", ""))
            if ver and ver not in info:
                info[ver] = (f["sha"], f"old-versions/{f['name']}")
    except Exception as e:
        log_error(f"shas-old-{cert}", e)
    return info


def _match_cert_version(cert, filename):
    """Extract version from a curriculum filename if it matches the cert."""
    # Match cert prefix followed by underscore or space (not another letter)
    if not re.match(rf"^{cert}[_ ]", filename.upper()):
        return None
    m = re.search(r"v?(\d+\.\d+)(?:\.\d+)?\.pdf$", filename, re.I)
    return m.group(1) if m else None


def download_pdf(cert, version):
    """Download a curriculum PDF. Returns path to temp file or None."""
    import tempfile
    for pattern in CERT_FILE_PATTERNS.get(cert, []):
        filename = pattern.format(v=version)
        for prefix in ["", "old-versions/"]:
            url = f"{CURRICULUM_RAW}/{prefix}{quote(filename)}"
            try:
                req = Request(url, headers={"User-Agent": UA})
                with urlopen(req, timeout=30) as resp:
                    if resp.status == 200:
                        tmp = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
                        tmp.write(resp.read())
                        tmp.close()
                        return tmp.name
            except Exception:
                continue
    return None


def download_pdf_at_sha(filename, sha):
    """Download a curriculum PDF at a specific commit SHA. Returns path or None."""
    import tempfile
    url = f"https://raw.githubusercontent.com/cncf/curriculum/{sha}/{quote(filename)}"
    try:
        req = Request(url, headers={"User-Agent": UA})
        with urlopen(req, timeout=30) as resp:
            if resp.status == 200:
                tmp = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
                tmp.write(resp.read())
                tmp.close()
                return tmp.name
    except Exception:
        pass
    return None


def extract_pdf_text(path):
    """Extract text from a PDF using PyMuPDF. Returns list of lines."""
    doc = fitz.open(path)
    lines = []
    for page in doc:
        text = page.get_text("text")
        for line in text.splitlines():
            stripped = line.strip()
            if stripped:
                lines.append(stripped)
    doc.close()
    return lines


def diff_curricula(cert, versions):
    """Compare consecutive versions of a cert's curriculum.

    Returns (results, file_info) where:
    - results: list of (old_ver, new_ver, status, diff_lines)
      status: "identical", "changed", "changed-no-detail", "unavailable"
    - file_info: {version: (sha, repo_path)}
    """
    import difflib
    import os

    file_info = get_curriculum_shas(cert)
    results = []

    for i in range(len(versions) - 1):
        old_ver, new_ver = versions[i], versions[i + 1]
        old_entry = file_info.get(old_ver)
        new_entry = file_info.get(new_ver)

        # Method 6: fast binary comparison via blob SHA
        if old_entry and new_entry and old_entry[0] == new_entry[0]:
            results.append((old_ver, new_ver, "identical", []))
            continue

        if not old_entry or not new_entry:
            results.append((old_ver, new_ver, "unavailable", []))
            continue

        # SHAs differ — cached result from an earlier run?
        cached = _load_diff_cache()["pairs"].get(f"{old_entry[0]}:{new_entry[0]}")
        if cached is not None:
            results.append((old_ver, new_ver,
                            cached["status"], cached.get("diff_lines", [])))
            continue

        # Not cached — try Method 2: download + PyMuPDF text diff
        if not HAS_FITZ:
            results.append((old_ver, new_ver, "changed-no-detail", []))
            continue

        old_path = download_pdf(cert, old_ver)
        new_path = download_pdf(cert, new_ver)

        if not old_path or not new_path:
            results.append((old_ver, new_ver, "changed-no-detail", []))
            for p in (old_path, new_path):
                if p:
                    os.unlink(p)
            continue

        try:
            old_lines = extract_pdf_text(old_path)
            new_lines = extract_pdf_text(new_path)
            diff = list(difflib.unified_diff(
                old_lines, new_lines,
                fromfile=f"v{old_ver}", tofile=f"v{new_ver}",
                lineterm="",
            ))
            status = "changed" if diff else "identical"
            _cache_pair(old_entry[0], new_entry[0], status, diff)
            results.append((old_ver, new_ver, status, diff))
        except Exception as e:
            results.append((old_ver, new_ver, "changed-no-detail", [str(e)]))
        finally:
            os.unlink(old_path)
            os.unlink(new_path)

    return results, file_info


def _count_changes(diff_lines):
    """Count changed lines in a unified diff (excluding headers)."""
    return sum(1 for l in diff_lines
               if l.startswith(('+', '-')) and not l.startswith(('+++', '---')))


def _extract_topic_changes(diff_lines):
    """Extract added/removed topic items from a small diff."""
    added, removed = [], []
    for line in diff_lines:
        if line.startswith(('+++', '---', '@@')):
            continue
        if not line.startswith(('+', '-')):
            continue
        if '•' not in line:
            continue
        topic = line[1:].strip().lstrip('•\t ')
        if topic:
            (added if line[0] == '+' else removed).append(topic)
    return added, removed


_BOILERPLATE_RE = re.compile(
    r'Cloud native computing|https?://|©|[Cc]opyright|'
    r'Important Instructions|Linux Foundation|'
    r'These Certification Exam|Curriculum|'
    r'\d+\s*%\s*-|Certified Kubernetes',
    re.I
)


def _fix_ligatures(text):
    """Replace common PDF ligature characters with plain ASCII."""
    return (text
            .replace('\ufb01', 'fi')
            .replace('\ufb02', 'fl')
            .replace('\ufb00', 'ff')
            .replace('\ufb03', 'ffi')
            .replace('\ufb04', 'ffl'))


def _normalize_topics(lines):
    """Extract bullet-point topics from PDF text lines as a set.

    Joins continuation lines, fixes ligatures, and stops at boilerplate.
    """
    topics = set()
    current = None
    for line in lines:
        line = _fix_ligatures(line.strip())
        if not line:
            continue
        if _BOILERPLATE_RE.search(line):
            if current is not None:
                topics.add(current)
                current = None
            continue
        if '•' in line:
            if current is not None:
                topics.add(current)
            current = line.split('•', 1)[1].strip()
        elif current is not None:
            current = current + ' ' + line
    if current is not None:
        topics.add(current)
    return topics


def _diff_revision(info):
    """Diff initial vs latest PDF of a mid-version revision.

    Uses set-based topic comparison to ignore reformatting noise.
    Downloads PDFs at the first and latest commit SHAs, extracts topics,
    and compares as sets. Returns a change summary string or None.
    """
    key = f"{info['first_sha']}:{info['latest_sha']}"
    revisions = _load_diff_cache()["revisions"]
    if key in revisions:
        return revisions[key]

    if not HAS_FITZ:
        return None

    import os

    old_path = download_pdf_at_sha(info["filename"], info["first_sha"])
    new_path = download_pdf_at_sha(info["filename"], info["latest_sha"])

    if not old_path or not new_path:
        for p in (old_path, new_path):
            if p:
                os.unlink(p)
        return None

    try:
        old_topics = _normalize_topics(extract_pdf_text(old_path))
        new_topics = _normalize_topics(extract_pdf_text(new_path))

        added = new_topics - old_topics
        removed = old_topics - new_topics

        if not added and not removed:
            detail = None
        else:
            parts = ([f"Removed: {t}" for t in sorted(removed)]
                     + [f"Added: {t}" for t in sorted(added)])
            detail = " · ".join(parts)
        _cache_revision(info["first_sha"], info["latest_sha"], detail)
        return detail
    except Exception:
        return None
    finally:
        os.unlink(old_path)
        os.unlink(new_path)


def _pdf_link(file_info, version):
    """Construct GitHub URL for a curriculum PDF."""
    entry = file_info.get(version)
    if not entry:
        return None
    return f"{CURRICULUM_GITHUB}/{quote(entry[1])}"


def build_topic_footnotes(cert, diffs, file_info, row_order, start=0, revision_info=None):
    """Build superscript footnotes for topic changes and mid-version revisions.

    Returns (markers, footnotes, n) where:
    - markers: {version: superscript_char} for versions with changes
    - footnotes: list of footnote lines in table order
    - n: next available footnote number (for global numbering across certs)
    """
    # Map new_ver to its diff entry (only changed ones)
    change_map = {}
    for old_ver, new_ver, status, diff_lines in diffs:
        if status not in ("identical", "unavailable"):
            change_map[new_ver] = (old_ver, new_ver, status, diff_lines)

    markers = {}
    footnotes = []
    n = start

    for ver in row_order:
        if ver not in change_map:
            continue
        old_ver, new_ver, status, diff_lines = change_map[ver]
        sup = SUPERSCRIPTS[n] if n < len(SUPERSCRIPTS) else f"[{n + 1}]"
        markers[ver] = sup

        old_url = _pdf_link(file_info, old_ver)
        new_url = _pdf_link(file_info, new_ver)

        is_major = (status == "changed-no-detail"
                    or _count_changes(diff_lines) > MAJOR_DIFF_THRESHOLD)

        if is_major:
            if old_url and new_url:
                footnotes.append(
                    f"{sup} v{old_ver} → v{new_ver} topics changed: "
                    f"[v{old_ver} curriculum]({old_url}) · "
                    f"[v{new_ver} curriculum]({new_url})")
            else:
                footnotes.append(f"{sup} v{old_ver} → v{new_ver}: topics changed")
        else:
            added, removed = _extract_topic_changes(diff_lines)
            if added or removed:
                parts = ([f"Removed: {t}" for t in removed]
                         + [f"Added: {t}" for t in added])
                footnotes.append(
                    f"{sup} v{old_ver} → v{new_ver} topics changed: "
                    + " · ".join(parts))
            else:
                footnotes.append(
                    f"{sup} v{old_ver} → v{new_ver}: minor formatting changes")

        n += 1

    # Mid-version revision footnotes
    if revision_info:
        for ver in row_order:
            if ver not in revision_info:
                continue
            info = revision_info[ver]
            sup = SUPERSCRIPTS[n] if n < len(SUPERSCRIPTS) else f"[{n + 1}]"
            if ver in markers:
                markers[ver] += sup
            else:
                markers[ver] = sup

            detail = _diff_revision(info)
            base = (f"{sup} v{ver} curriculum revised "
                    f"{info['last_updated'].isoformat()}")
            if detail:
                footnotes.append(f"{base}: {detail}")
            else:
                initial_url = (f"https://github.com/cncf/curriculum/blob/"
                               f"{info['first_sha']}/{quote(info['filename'])}")
                revised_url = (f"https://github.com/cncf/curriculum/blob/"
                               f"{info['latest_sha']}/{quote(info['filename'])}")
                footnotes.append(
                    f"{base}: "
                    f"[initial]({initial_url}) · "
                    f"[revised]({revised_url})")

            n += 1

    return markers, footnotes, n


def format_diff_output(cert, diffs):
    """Format detailed curriculum diffs as markdown (for --diff flag)."""
    lines = [f"## {cert} Curriculum Changes", ""]
    for old_ver, new_ver, status, diff_lines in diffs:
        lines.append(f"### v{old_ver} → v{new_ver}")
        lines.append("")
        if status == "identical":
            lines.append("No topic changes (identical curriculum)")
        elif status in ("unavailable", "changed-no-detail"):
            msg = diff_lines[0] if diff_lines else "PDF not available"
            lines.append(f"*{msg}*")
        elif status == "changed":
            lines.append("```diff")
            lines.extend(diff_lines)
            lines.append("```")
        lines.append("")
    return lines


def generate_diff(cert, versions):
    """Generate detailed curriculum diff markdown for a cert. Returns string."""
    diffs, _ = diff_curricula(cert, versions)
    lines = format_diff_output(cert, diffs)
    return "\n".join(lines) + "\n"


# --- Main ---

def generate(today=None):
    """Generate the markdown output. Returns (output_string, exit_code, tracker_data)."""
    if today is None:
        today = date.today()

    all_versions = released_versions()
    if not all_versions:
        return None, 2, None

    latest = all_versions[0]
    latest_minor = int(latest["cycle"].split(".")[1])
    next_minor = f"1.{latest_minor + 1}"
    next_ga = next_release_date(next_minor)

    # Versions for curriculum diff (oldest first)
    diff_versions = [v["cycle"] for v in all_versions[:HISTORICAL]]
    diff_versions.reverse()

    lines = []
    certs_with_data = 0
    footnote_num = 0
    tracker_data = {"updated": today.isoformat()}
    for cert in CERTS:
        rows, avg_lag, day_name, revision_info = build_cert_data(cert, all_versions, next_minor, next_ga, today)
        # Check if we got any actual switch data
        actual_switches = sum(1 for _, _, sw, _, _, sp in rows if sw and not sp)
        if actual_switches > 0:
            certs_with_data += 1

        # Intent 2: topic changes
        diffs, file_info = diff_curricula(cert, diff_versions)
        row_order = [r[0] for r in rows]
        markers, footnotes, footnote_num = build_topic_footnotes(
            cert, diffs, file_info, row_order, footnote_num, revision_info)

        # Intent 3: machine-readable data for downstream automation
        current_version = None
        overdue = False
        for minor, ga, switch, supported, ga_pred, sw_pred in rows:
            if switch and not sw_pred:
                current_version = minor
                break
            if sw_pred and switch and switch < today:
                overdue = True

        # topics_changed: cross-version diff (separate from mid-version revision)
        topics_changed = False
        if current_version:
            for old_ver, new_ver, status, _ in diffs:
                if new_ver == current_version and status not in ("identical", "unavailable"):
                    topics_changed = True
                    break

        # curriculum_revised: mid-version update (current version only)
        current_revised = False
        current_revision_date = None
        if current_version and current_version in revision_info:
            current_revised = True
            current_revision_date = revision_info[current_version]["last_updated"].isoformat()

        cert_entry = {
            "version": current_version,
            "topics_changed": topics_changed,
            "curriculum_revised": current_revised,
            "revision_date": current_revision_date,
            "overdue": overdue,
        }

        for key, days in (("version_in_1w", 7), ("version_in_2w", 14), ("version_in_1m", 30)):
            horizon = today + timedelta(days=days)
            predicted_ver = current_version
            for minor, ga, switch, supported, ga_pred, sw_pred in rows:
                if switch and switch <= horizon:
                    predicted_ver = minor
                    break
            cert_entry[key] = predicted_ver

        tracker_data[cert] = cert_entry

        lines.extend(format_table(cert, rows, avg_lag, day_name, today, markers))
        if footnotes:
            lines[-1] += "<br>"
            for fn in footnotes[:-1]:
                lines.append(fn + "<br>")
            lines.append(footnotes[-1])
        lines.append("")

    if certs_with_data == 0:
        log_error("output", "No actual cert switch data found for any cert")
        return None, 2, None

    # Tactic E: validate tracker_data against Linux Foundation FAQ
    faq_versions = fetch_faq_versions()
    if faq_versions:
        for cert in CERTS:
            if cert not in faq_versions or cert not in tracker_data:
                continue
            faq_ver = faq_versions[cert]
            tracker_ver = tracker_data[cert].get("version")
            if not tracker_ver or faq_ver == tracker_ver:
                continue
            if _version_key(faq_ver) > _version_key(tracker_ver):
                log_error(f"faq-override-{cert}",
                          f"FAQ says v{faq_ver} but commits show v{tracker_ver}; "
                          f"using FAQ version with today as switch date")
                tracker_data[cert]["version"] = faq_ver
                tracker_data[cert]["topics_changed"] = False
                tracker_data[cert]["curriculum_revised"] = False
                tracker_data[cert]["revision_date"] = None
                tracker_data[cert]["overdue"] = False
                for key in ("version_in_1w", "version_in_2w", "version_in_1m"):
                    if _version_key(tracker_data[cert].get(key, "0.0")) < _version_key(faq_ver):
                        tracker_data[cert][key] = faq_ver
            else:
                log_error(f"faq-mismatch-{cert}",
                          f"Commits show v{tracker_ver} but FAQ says v{faq_ver}; "
                          f"possible pre-staging")

    lines.append("\\* EOL (end of life)")
    lines.append("")

    output = "\n".join(lines) + "\n"

    # Exit code: 0=ok, 1=degraded (some errors but output is usable)
    exit_code = 1 if _errors else 0
    return output, exit_code, tracker_data


def main():
    if "--diff" in sys.argv:
        certs = [a for a in sys.argv[1:] if a != "--diff" and a in CERTS]
        if not certs:
            certs = list(CERTS)

        all_versions = released_versions()
        if not all_versions:
            print("FATAL: Could not fetch K8s versions", file=sys.stderr)
            sys.exit(2)

        versions = [v["cycle"] for v in all_versions[:HISTORICAL]]
        versions.reverse()

        lines = []
        for cert in certs:
            diffs, _ = diff_curricula(cert, versions)
            lines.extend(format_diff_output(cert, diffs))

        print("\n".join(lines), end="")
        save_diff_cache()
        sys.exit(1 if _errors else 0)

    output, exit_code, tracker_data = generate()

    if _errors:
        print(f"\n--- Errors ({len(_errors)}) ---", file=sys.stderr)
        for e in _errors:
            print(f"  [{e['source']}] {e['message']}", file=sys.stderr)

    if output:
        print(output, end="")
    else:
        print("FATAL: Could not generate output", file=sys.stderr)

    if tracker_data:
        with open("tracker.json", "w") as f:
            json.dump(tracker_data, f, indent=2)
            f.write("\n")

    save_diff_cache()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
