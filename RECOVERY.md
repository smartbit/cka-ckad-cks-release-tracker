# Recovery

## Failure workflow

1. Script fails → GitHub Issue opened (label: `tracker-failure`)
2. Failures persist 30+ days → repo auto-archived via `gh repo archive`
3. Actions stop (archived repos are read-only)
4. Manual recovery required (see below)

## Restoring an archived repo

```bash
# 1. Un-archive
gh repo unarchive smartbit/cka-ckad-cks-release-tracker --yes

# 2. Check what broke (read the issue)
gh issue list --label tracker-failure --state open

# 3. Fix the script, push, then trigger manually
gh workflow run daily.yml

# 4. The workflow auto-closes the issue on success
```

## Common failure modes

| Source | Symptom | Fix |
|--------|---------|-----|
| endoflife.date | API down or schema changed | Fallback to GitHub releases API kicks in automatically. If both fail, check `ENDOFLIFE_URL` and `K8S_RELEASES` in the script |
| cncf/curriculum | Filename pattern changed | Add the new pattern to `CERT_FILE_PATTERNS`. The contents-listing fallback may already handle it |
| kubernetes/sig-release | README format changed | Update regex patterns in `next_release_date()`. Two formats are already supported |
| GitHub API | Rate-limited | The script prefers `gh` CLI (5000/hr). Unauthenticated falls back to 60/hr. In Actions, `GITHUB_TOKEN` provides 1000/hr |
