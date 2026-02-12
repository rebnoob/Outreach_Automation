# Robotics Outreach Toolkit

A crawler + enrichment + lead scoring + outreach automation toolkit for U.S. manufacturing robotics sales.

## Capabilities

- Discover manufacturing companies from search queries
- Crawl websites and extract contact channels (email, phone, form, LinkedIn)
- Score lead fit and recommend best outreach channel
- Build outreach sequences and queue actions in SQLite
- Send queued email actions (dry-run or live SMTP)
- Run everything from CLI or browser dashboard

## Run the Web Dashboard (recommended)

From the parent directory of this package:

```bash
cd /Users/rebnoob/Desktop
python3 -m pip install -r robotics_outreach_toolkit/requirements.txt
python3 -m robotics_outreach_toolkit.webapp
```

Then open:

- [http://localhost:8000](http://localhost:8000)

The dashboard lets you:

- Run `discover`, `enrich`, `score`, `plan`, `send`
- Run the full pipeline in one action
- Filter and inspect leads
- Download `CSV`

### If discover says `0 new`

- It usually means those domains are already in your database.
- The UI now shows `new`, `already in DB`, and `total hits` so you can tell the difference.
- Use **Clear All Lead Data** (type `DELETE`) if you want to restart from scratch.

## CLI Quick Start

```bash
cd /Users/rebnoob/Desktop
python3 -m robotics_outreach_toolkit.cli init
python3 -m robotics_outreach_toolkit.cli run-all --queries-file /Users/rebnoob/Desktop/robotics_outreach_toolkit/queries_manufacturing.txt --max-results 20 --limit 150 --plan-limit 100
python3 -m robotics_outreach_toolkit.cli export --out-csv /Users/rebnoob/Desktop/robotics_outreach_toolkit/data/leads_export.csv
```

## Core CLI Commands

```bash
python3 -m robotics_outreach_toolkit.cli discover --states california texas --max-results 20
python3 -m robotics_outreach_toolkit.cli enrich --limit 200 --max-pages 4
python3 -m robotics_outreach_toolkit.cli score
python3 -m robotics_outreach_toolkit.cli plan --start-date 2026-02-12 --limit 100
python3 -m robotics_outreach_toolkit.cli send --action-date 2026-02-12
```

## SMTP for Live Send

```bash
export SMTP_HOST="smtp.yourprovider.com"
export SMTP_PORT="587"
export SMTP_USER="you@domain.com"
export SMTP_PASS="app_password"
export SMTP_FROM="you@domain.com"
python3 -m robotics_outreach_toolkit.cli send --action-date 2026-02-12 --live
```

## Docker Deployment

Build and run:

```bash
cd /Users/rebnoob/Desktop/robotics_outreach_toolkit
docker build -t robotics-outreach-toolkit .
docker run --rm -p 8000:8000 -v /Users/rebnoob/Desktop/robotics_outreach_toolkit/data:/app/robotics_outreach_toolkit/data robotics-outreach-toolkit
```

Then open:

- [http://localhost:8000](http://localhost:8000)

## Data Outputs

- SQLite DB: `/Users/rebnoob/Desktop/robotics_outreach_toolkit/data/leads.db`
- CSV export: `/Users/rebnoob/Desktop/robotics_outreach_toolkit/data/leads_export.csv`

## Notes

- Designed for B2B outreach with human review.
- Keep outbound compliant with your local regulations and platform policies.
- Validate top leads before any live send.
