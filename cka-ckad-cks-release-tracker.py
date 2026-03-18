#!/usr/bin/env python3
"""
Generates markdown tracking Kubernetes cert exam release adoption.

Intent 1: Quickly see when the next change in exam release is to be expected.
Intent 2: Warn for changes in the topics.
Intent 3: Low maintenance — detect failures, archive after 30 days.

Data sources (with fallbacks — Tactic D):
  - endoflife.date/api/kubernetes.json     → fallback: GitHub kubernetes/kubernetes releases API
  - GitHub API (cncf/curriculum) by path   → fallback: list repo contents + regex match
  - GitHub API (kubernetes/sig-release)    → fallback: bullet format, then estimation

Schema validation (Tactic C):
  - Validates API responses before use
  - Collects errors, reports to stderr
  - Exit codes: 0=ok, 1=degraded, 2=critical failure

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


def cert_switch_date(cert, minor):
    """Get cert switch date. Primary: commits by filename. Fallback: list contents + regex."""
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
    """Try known filename patterns to find the cert switch commit."""
    for pattern in CERT_FILE_PATTERNS[cert]:
        filename = pattern.format(v=minor)
        url = f"{CURRICULUM_COMMITS}?path={quote(filename)}"
        try:
            commits = fetch_json(url)
            if commits:
                validate_commits(commits)
                ts = commits[-1]["commit"]["committer"]["date"]
                return datetime.fromisoformat(ts.replace("Z", "+00:00")).date()
        except (URLError, KeyError, IndexError, ValueError):
            continue
    return None


def _cert_switch_from_contents(cert, minor):
    """Fallback: list repo contents, find file by regex, get its first commit."""
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
                ts = commits[-1]["commit"]["committer"]["date"]
                return datetime.fromisoformat(ts.replace("Z", "+00:00")).date()
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
    cleaned = re.sub(r"(st|nd|rd|th)", "", raw)
    return datetime.strptime(cleaned, "%d %B %Y").date()


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

def build_cert_data(cert, all_versions, next_minor, next_ga, today):
    """Collect switch dates and compute predictions for one cert.

    Returns (rows, avg_lag, day_name) where each row is:
        (minor, ga, switch, supported, ga_predicted, sw_predicted)
    """
    hist = []
    for v in all_versions[:HISTORICAL]:
        minor = v["cycle"]
        ga = date.fromisoformat(v["releaseDate"])
        switch = cert_switch_date(cert, minor)
        eol = date.fromisoformat(v["eol"]) if isinstance(v["eol"], str) else None
        supported = eol is None or eol > today
        hist.append((minor, ga, switch, supported))

    pairs_with_data = [(ga, sw) for _, ga, sw, _ in hist if sw]
    all_deltas = [(sw - ga).days for ga, sw in pairs_with_data]
    deltas = filter_outliers(all_deltas[:PREDICTION_WINDOW], all_deltas)
    # Weekday vote uses ALL historical switches
    all_switch_dates = [sw for _, sw in pairs_with_data]

    # Next version
    next_switch = cert_switch_date(cert, next_minor)
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

    avg_lag = round(sum(deltas) / len(deltas)) if deltas else 0
    day_name = ""
    if all_switch_dates:
        weekdays = Counter(d.weekday() for d in all_switch_dates)
        common_day = weekdays.most_common(1)[0][0]
        day_name = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][common_day]

    return rows, avg_lag, day_name


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

        # SHAs differ — try Method 2: download + PyMuPDF text diff
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


def _pdf_link(file_info, version):
    """Construct GitHub URL for a curriculum PDF."""
    entry = file_info.get(version)
    if not entry:
        return None
    return f"{CURRICULUM_GITHUB}/{quote(entry[1])}"


def build_topic_footnotes(cert, diffs, file_info, row_order, start=0):
    """Build superscript footnotes for topic changes, keyed to table row order.

    Returns (markers, footnotes, n) where:
    - markers: {version: superscript_char} for versions with topic changes
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
    """Generate the markdown output. Returns (output_string, exit_code)."""
    if today is None:
        today = date.today()

    all_versions = released_versions()
    if not all_versions:
        return None, 2

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
    for cert in CERTS:
        rows, avg_lag, day_name = build_cert_data(cert, all_versions, next_minor, next_ga, today)
        # Check if we got any actual switch data
        actual_switches = sum(1 for _, _, sw, _, _, sp in rows if sw and not sp)
        if actual_switches > 0:
            certs_with_data += 1

        # Intent 2: topic changes
        diffs, file_info = diff_curricula(cert, diff_versions)
        row_order = [r[0] for r in rows]
        markers, footnotes, footnote_num = build_topic_footnotes(cert, diffs, file_info, row_order, footnote_num)

        lines.extend(format_table(cert, rows, avg_lag, day_name, today, markers))
        if footnotes:
            lines[-1] += "<br>"
            for fn in footnotes[:-1]:
                lines.append(fn + "<br>")
            lines.append(footnotes[-1])
        lines.append("")

    if certs_with_data == 0:
        log_error("output", "No actual cert switch data found for any cert")
        return None, 2

    lines.append("\\* EOL (end of life)")
    lines.append("")

    output = "\n".join(lines) + "\n"

    # Exit code: 0=ok, 1=degraded (some errors but output is usable)
    exit_code = 1 if _errors else 0
    return output, exit_code


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
        sys.exit(1 if _errors else 0)

    output, exit_code = generate()

    if _errors:
        print(f"\n--- Errors ({len(_errors)}) ---", file=sys.stderr)
        for e in _errors:
            print(f"  [{e['source']}] {e['message']}", file=sys.stderr)

    if output:
        print(output, end="")
    else:
        print("FATAL: Could not generate output", file=sys.stderr)

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
