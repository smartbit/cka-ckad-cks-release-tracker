#!/usr/bin/env python3
"""
Generates markdown tables of Kubernetes releases vs CKA/CKAD/CKS exam adoption.

Data sources (with fallbacks — Tactic D):
  - endoflife.date/api/kubernetes.json     → fallback: GitHub kubernetes/kubernetes releases API
  - GitHub API (cncf/curriculum) by path   → fallback: list repo contents + regex match
  - GitHub API (kubernetes/sig-release)    → fallback: bullet format, then estimation

Schema validation (Tactic C):
  - Validates API responses before use
  - Collects errors, reports to stderr
  - Exit codes: 0=ok, 1=degraded, 2=critical failure

Requires: python 3.9+, gh CLI (optional, used for authenticated GitHub API).
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
SEP = "│"  # U+2502 BOX DRAWINGS LIGHT VERTICAL — visual separator column

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
    recent = pairs_with_data[:PREDICTION_WINDOW]
    deltas = [(sw - ga).days for ga, sw in recent]
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


def format_table(cert, rows, avg_lag, day_name, today):
    """Format one cert's markdown table. Returns list of lines."""
    lines = []

    has_overdue = any(
        sw_pred and switch and switch < today
        for _, _, switch, _, _, sw_pred in rows
    )

    lines.append(f"### {cert}")
    lines.append("")

    h = f"| K8s  | K8s GA      | {SEP} | {cert} Switch | Day  | Days |"
    s = f"|:-----|:------------|:---:|:------------|:----:|:----:|"
    if has_overdue:
        h += " Overdue |"
        s += ":------:|"
    lines.append(h)
    lines.append(s)

    for minor, ga, switch, supported, ga_pred, sw_pred in rows:
        gp = "~" if ga_pred else ""
        sp = "~" if sw_pred else ""
        eol = "*" if not supported else ""
        ga_str = f"{gp}{ga.isoformat()}{eol}" if ga else "TBD"
        if switch:
            sw_str = f"{sp}{switch.isoformat()}"
            day_str = f"{sp}{switch.strftime('%a')}"
            delta = (switch - ga).days if ga else None
            delta_str = f"{sp}{delta}" if delta is not None else "—"
        else:
            sw_str = "—"
            day_str = ""
            delta_str = "—"
        row = f"| {minor:<4} | {ga_str:<11} | {SEP} | {sw_str:<11} | {day_str:<4} | {delta_str:>4} |"
        if has_overdue:
            if sw_pred and switch and switch < today:
                overdue = (today - switch).days
                row += f" ~{overdue:<5} |"
            else:
                row += "        |"
        lines.append(row)

    lines.append("")
    lines.append(f"~ Predicted: K8s GA + {avg_lag}d avg (last {PREDICTION_WINDOW}), "
                 f"nearest {day_name}<br/>\\* EOL (end of life)")
    lines.append("")

    return lines


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

    lines = []
    certs_with_data = 0
    for cert in CERTS:
        rows, avg_lag, day_name = build_cert_data(cert, all_versions, next_minor, next_ga, today)
        # Check if we got any actual switch data
        actual_switches = sum(1 for _, _, sw, _, _, sp in rows if sw and not sp)
        if actual_switches > 0:
            certs_with_data += 1
        lines.extend(format_table(cert, rows, avg_lag, day_name, today))

    if certs_with_data == 0:
        log_error("output", "No actual cert switch data found for any cert")
        return None, 2

    output = "\n".join(lines) + "\n"

    # Exit code: 0=ok, 1=degraded (some errors but output is usable)
    exit_code = 1 if _errors else 0
    return output, exit_code


def main():
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
