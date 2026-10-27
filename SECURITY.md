# Keeping credentials out of GitHub

Keep real credentials in the ignored local `.env` or a secret manager. Commit
only the blank/template `.env.example`. Never put Dhan keys, tokens, PINs,
TOTP seeds, database passwords, wallet downloads, or private keys into source,
screenshots, issues, pull requests, or logs. `symbols.csv` contains symbols only.

Before staging/publishing, run from the repository root:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/check-publish.ps1
```

The guard requires Git, Node.js, and Docker. It checks publication candidates,
all reachable Git history, sensitive file paths, and exact credential values
read privately from your local `.env`. Gitleaks adds provider/private-key
detection plus Dhan-specific literal checks. Its image is pinned by digest;
scanning containers use no network and read-only repository mounts. Reports
contain locations/rules only, never secret values or matching source lines.

After explicitly staging the intended files, check the actual index too:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/check-publish.ps1 -Staged
git diff --cached --stat
git diff --cached --name-only
```

Do not force-add ignored files, blindly stage a directory containing exports,
or bypass a failed check. `.gitignore` does not untrack existing files and does
not remove old secrets from commits. Generated `transformation/dbt_packages/`,
`transformation/target/`, and local dbt identity settings should stay local.

The GitHub workflow scans pushes/PRs, but runs **after** an upload. Run the
local guard first, and enable GitHub push protection as an additional safeguard.
No scanner can guarantee detection of every possible credential. In CI there
is normally no local `.env`, so exact local-value comparisons are unavailable;
pattern and sensitive-path checks still run. Do not add production credentials
to CI just to scan for them.

The documented local MinIO development username is treated as a public
identifier only in the two access-key ID settings. Passwords/secret keys and
all other access-key values are still checked; there is no source/test-wide
credential exemption.

To enable the included local pre-commit/pre-push guards after reviewing them:

```powershell
git config --local core.hooksPath .githooks
```

The hooks run the same checks and fail closed if prerequisites or local `.env`
are missing. CI explicitly uses `-AllowMissingEnv` because hosted runners must
not receive production credentials. Do not use that switch to skip local checks.

## If a credential was exposed

Revoke/rotate it at the provider first, even if the repository is private or
the file was later removed. Dhan credentials previously shared in screenshots
must be treated as exposed. Update local configuration privately; do not paste
replacement secrets into chat. History rewriting is a separate, coordinated
operation and must not be done with an unreviewed force push.

References: [GitHub's removal guidance](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/removing-sensitive-data-from-a-repository),
[Gitleaks](https://github.com/gitleaks/gitleaks).
