## Static Secret Scan Prompt (analyze_static.py)

Goal: find **raw credentials** in Airflow DAG sources (passwords, tokens, API keys, private keys, client secrets). Treat anything literal in code as a risk unless it clearly comes from env/Variables/Secret Manager/Connections.

Output rules
- Always return a response: if risky hardcoded credentials are found, list the findings; if none are found, respond exactly with `Nie znaleziono zagrozen.`.
- Do **not** add any other prose beyond the findings or the exact no-findings message.
- Final chat reply must be only the findings (each in its own fenced block) or exactly `Nie znaleziono zagrozen.` as plain text—no headings, summaries, or extra sentences.
- After each analysis, provide only the findings (using the format below) or the exact `Nie znaleziono zagrozen.` message when no risks are detected.
- If the user asks about any secret/key/password/token, first run the analysis below and answer only with the findings or the exact `Nie znaleziono zagrozen.` message.
- Never stay silent: every run must return either the findings (formatted below) or exactly `Nie znaleziono zagrozen.`.
- After running the scan, always read `output.txt` and immediately send one of the two allowed responses (findings or `Nie znaleziono zagrozen.`) back to the user; do not omit the reply.

How to run (and what to do)
- Detection-only: **do not modify any files/config**. Just report findings; remediation is handled by others.
- Always run the scanner with context 5 and capture output to `output.txt` in the same directory as the script:
  - Full scan: `cd airflow && python3 analyze_static.py -C 5 > output.txt`
  - Specific file: `cd airflow && python3 analyze_static.py -C 5 code/path/to/file.py > output.txt`
- Always base responses on the script output: read and interpret `output.txt` (it already includes 5 lines of context before/after each hit). Only grep/open source files for extra context if the output is ambiguous.
- After it runs, **manually review the response** from `output.txt` (do not paste it blindly). Validate each finding by opening the file/snippet (e.g., `grep -n "token" code/DAG_X/source.py`) and confirm whether it is a hardcoded secret before reporting.

What to search for (already covered by analyzer)
- `password`, `token`, `secret`, `api_key`, `access_token`, `refresh_token`
- Client IDs/secrets, Authorization/Bearer headers, JWTs
- AWS keys, Slack/GitHub/GitLab/Stripe/SendGrid/Google/Twilio/Telegram patterns
- URIs with `user:pass@`
- Private keys (PEM/SSH/PGP/RSA/DSA/EC, OpenSSH), PGP blocks
- GitHub/GitLab PATs, Stripe/SendGrid/Google API keys, Twilio, Telegram bot tokens

Interpret results
- Output: `file:line: [type] snippet`
- Flag/remove literal secrets (examples: `password = "..."`, `token = "..."`, embedded URIs with creds, PEM blocks).
- OK/low risk if the value is pulled from env/Variables/Secret Manager/Connections; verify it’s not hardcoded.

Reporting format (use this so findings are clickable in the IDE)
- For each confirmed risky literal, output a fenced block using `startLine:endLine:filepath` and include the snippet:

```
82:85:airflow/code/swine__orum-fan-mig/source.py
        username = "sj_influx"
        password = "asdf1234"
        database = 'sj_TSDB'
```

- Keep one fenced block per finding; avoid extra prose beyond the list of findings.
- If there are multiple findings in one file, repeat separate blocks with the relevant line ranges.

