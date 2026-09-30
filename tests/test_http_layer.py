"""Tests for the HTTP layer: token auth, in-memory caching, fallbacks.

Locks in the resource behaviour of fetch_json/_get_github_token: one network
request per unique URL per run, 404s cached, gh CLI consulted once and only
as fallback, and an unauthenticated retry when the token is rejected (401).
"""

import importlib.util
from datetime import date
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from urllib.error import HTTPError

# Import the script as a module despite the hyphens in the filename
_spec = importlib.util.spec_from_file_location(
    "tracker_http",
    Path(__file__).resolve().parent.parent / "cka-ckad-cks-release-tracker.py",
)
tracker = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(tracker)


class _FakeResponse:
    """Minimal urlopen() stand-in: context manager returning fixed bytes."""

    def __init__(self, payload=b"[]"):
        self.status = 200
        self._payload = payload

    def read(self):
        return self._payload

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


@pytest.fixture(autouse=True)
def _reset_http_globals():
    """Isolate token lookup and in-memory JSON cache between tests."""
    tracker._json_cache.clear()
    tracker._token = None
    tracker._token_checked = False
    yield
    tracker._json_cache.clear()
    tracker._token = None
    tracker._token_checked = False


def _http_error(code, url="https://api.github.com/x"):
    return HTTPError(url, code, {401: "Unauthorized", 403: "Forbidden",
                                 404: "Not Found"}[code], {}, None)


# --- Token precedence (one lookup per run) ---

class TestGetGithubToken:
    def test_env_gh_token_wins(self, monkeypatch):
        monkeypatch.setenv("GH_TOKEN", "env-token")
        monkeypatch.setenv("GITHUB_TOKEN", "other-token")
        with patch("shutil.which", return_value=False):
            assert tracker._get_github_token() == "env-token"

    def test_env_github_token_used_without_gh_token(self, monkeypatch):
        monkeypatch.delenv("GH_TOKEN", raising=False)
        monkeypatch.setenv("GITHUB_TOKEN", "env-token")
        with patch("shutil.which", return_value=False):
            assert tracker._get_github_token() == "env-token"

    def test_falls_back_to_gh_cli_token(self, monkeypatch):
        monkeypatch.delenv("GH_TOKEN", raising=False)
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)
        fake = Mock(returncode=0, stdout="cli-token\n")
        with patch("shutil.which", return_value="/usr/bin/gh"), \
             patch("subprocess.run", return_value=fake) as mock_run:
            assert tracker._get_github_token() == "cli-token"
        mock_run.assert_called_once_with(
            ["gh", "auth", "token"], capture_output=True, text=True, timeout=5)

    def test_none_when_no_env_and_no_gh(self, monkeypatch):
        monkeypatch.delenv("GH_TOKEN", raising=False)
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)
        with patch("shutil.which", return_value=False):
            assert tracker._get_github_token() is None

    def test_gh_cli_consulted_once_per_run(self, monkeypatch):
        """The token is cached — no repeated gh subprocess spawns."""
        monkeypatch.delenv("GH_TOKEN", raising=False)
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)
        fake = Mock(returncode=0, stdout="cli-token\n")
        with patch("shutil.which", return_value="/usr/bin/gh"), \
             patch("subprocess.run", return_value=fake) as mock_run:
            tracker._get_github_token()
            tracker._get_github_token()
        mock_run.assert_called_once()


# --- fetch_json: in-memory cache (one network request per unique URL) ---

class TestFetchJsonCache:
    def test_repeated_url_fetched_once(self):
        with patch.object(tracker, "urlopen",
                          return_value=_FakeResponse(b"[1, 2]")) as mock_dl:
            first = tracker.fetch_json("https://api.github.com/x")
            second = tracker.fetch_json("https://api.github.com/x")
        assert first == second == [1, 2]
        mock_dl.assert_called_once()

    def test_404_cached_without_network_retry(self):
        """A cached 404 re-raises locally — the URL is not fetched twice."""
        with patch.object(tracker, "urlopen",
                          side_effect=_http_error(404)) as mock_dl:
            with pytest.raises(HTTPError):
                tracker.fetch_json("https://api.github.com/x")
            with pytest.raises(HTTPError):
                tracker.fetch_json("https://api.github.com/x")
        mock_dl.assert_called_once()

    def test_404_cache_does_not_poison_other_urls(self):
        responses = [_http_error(404), _FakeResponse(b'{"ok": 1}')]
        with patch.object(tracker, "urlopen", side_effect=responses):
            with pytest.raises(HTTPError):
                tracker.fetch_json("https://api.github.com/a")
            assert tracker.fetch_json("https://api.github.com/b") == {"ok": 1}

    def test_non_github_urls_skip_token_lookup(self):
        with patch.object(tracker, "urlopen",
                          return_value=_FakeResponse(b'{"eol": true}')), \
             patch.object(tracker, "_get_github_token") as mock_token:
            assert tracker.fetch_json("https://endoflife.date/api/x.json") == {"eol": True}
        mock_token.assert_not_called()


# --- fetch_json: fallbacks and failure handling ---

class TestFetchJsonFallbacks:
    def test_falls_back_to_gh_api_on_403_without_token(self):
        """Without a token, a 403 (rate limit) retries via `gh api`."""
        fake = Mock(returncode=0, stdout='{"via": "gh"}')
        with patch.object(tracker, "urlopen", side_effect=_http_error(403)), \
             patch.object(tracker, "_get_github_token", return_value=None), \
             patch("shutil.which", return_value="/usr/bin/gh"), \
             patch("subprocess.run", return_value=fake) as mock_run:
            assert tracker.fetch_json("https://api.github.com/x") == {"via": "gh"}
        mock_run.assert_called_once_with(
            ["gh", "api", "x"], capture_output=True, text=True, timeout=30)

    def test_no_gh_fallback_when_token_present(self):
        """With a token, a 403 propagates — gh api would fail identically."""
        with patch.object(tracker, "urlopen", side_effect=_http_error(403)), \
             patch.object(tracker, "_get_github_token", return_value="tok"), \
             patch("shutil.which", return_value="/usr/bin/gh"), \
             patch("subprocess.run") as mock_run, \
             pytest.raises(HTTPError):
            tracker.fetch_json("https://api.github.com/x")
        mock_run.assert_not_called()

    def test_rejected_token_retries_unauthenticated(self):
        """A 401 must degrade (warn) and retry without the token, not die."""
        responses = [_http_error(401), _FakeResponse(b'{"ok": 1}')]
        with patch.object(tracker, "urlopen", side_effect=responses) as mock_dl, \
             patch.object(tracker, "_get_github_token",
                          return_value="stale-token"):
            assert tracker.fetch_json("https://api.github.com/x") == {"ok": 1}
        assert mock_dl.call_count == 2
        first_req, retry_req = (call.args[0] for call in mock_dl.call_args_list)
        assert "Authorization" in first_req.headers
        assert "Authorization" not in retry_req.headers

    def test_rejected_token_401_without_token_raises_once(self):
        """No token sent → a 401 is not retried unauthenticated."""
        with patch.object(tracker, "urlopen",
                          side_effect=_http_error(401)) as mock_dl, \
             patch.object(tracker, "_get_github_token", return_value=None), \
             patch("shutil.which", return_value=False), \
             pytest.raises(HTTPError):
            tracker.fetch_json("https://api.github.com/x")
        assert mock_dl.call_count == 1


# --- Pre-fetch dedupe (resource regression guard) ---

class TestPrefetchDedupe:
    def test_prefetch_and_loop_hit_network_once_per_url(self):
        """Pool pre-fetch + sequential loop must not double network requests.

        Guards the coupling the request reduction relies on: build_cert_data
        requests every version twice (pre-fetch, then the loop), and only the
        in-memory cache makes that free.
        """
        urls = []

        def fake_urlopen(req, timeout=None):
            urls.append(req.full_url)
            return _FakeResponse(b"[]")

        versions = [
            {"cycle": "1.35", "releaseDate": "2025-12-17", "eol": "2027-02-28"},
            {"cycle": "1.36", "releaseDate": "2026-04-22", "eol": "2027-06-28"},
        ]
        with patch.object(tracker, "urlopen", side_effect=fake_urlopen), \
             patch.object(tracker, "_get_github_token", return_value=None):
            tracker.build_cert_data("CKA", versions, "1.37", None, date(2026, 9, 30))

        # 3 commits URLs + 1 contents listing (the pattern-miss fallback),
        # and the contents URL fetched once — not once per missing version
        assert len(urls) == 4, f"unexpected fetches: {urls}"
        assert len(urls) == len(set(urls)), "duplicate network request for same URL"
