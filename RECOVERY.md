# Recovery

## Failure workflow

1. Daily run fails (tracker exit code other than 0/1, **or** any workflow step fails) → GitHub Issue opened (label: `tracker-failure`) and the run goes red
2. Each further failing day → comment added to that issue
3. Issue open 30+ days → repo made **private** (`gh repo edit --visibility private`, authenticated via the `ARCHIVE_PAT` secret)
4. Actions keep running on the private repo, but the page and badges disappear for visitors
5. Manual recovery required (see below)

## Restoring a repo that was made private

```bash
# 1. Check what broke (read the issue)
gh issue list --label tracker-failure --state open

# 2. Fix the script, push, then trigger manually
gh workflow run daily.yml

# 3. The workflow auto-closes the issue on success

# 4. Make the repo public again
gh repo edit smartbit/cka-ckad-cks-release-tracker \
  --visibility public --accept-visibility-change-consequences
```

> **Warning:** going private permanently removes stars and watchers and detaches
> forks. Restoring public visibility does **not** bring them back.

## Common failure modes

| Source | Symptom | Fix |
|--------|---------|-----|
| endoflife.date | API down or schema changed | Fallback to GitHub releases API kicks in automatically. If both fail, check `ENDOFLIFE_URL` and `K8S_RELEASES` in the script |
| cncf/curriculum | Filename pattern changed | Add the new pattern to `CERT_FILE_PATTERNS`. The contents-listing fallback may already handle it |
| kubernetes/sig-release | README format changed | Update regex patterns in `next_release_date()`. Two formats are already supported |
| GitHub API | Rate-limited | The script prefers `gh` CLI (5000/hr). Unauthenticated falls back to 60/hr. In Actions, `GITHUB_TOKEN` provides 1000/hr |
