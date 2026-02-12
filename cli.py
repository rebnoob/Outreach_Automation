from __future__ import annotations

import argparse
import csv
from datetime import date
from pathlib import Path
from typing import List

from .config import load_config
from .crawler import enrich_company
from .database import (
    export_leads,
    get_companies_for_enrichment,
    get_connection,
    get_scored_companies,
    init_db,
)
from .discovery import default_manufacturing_queries, discover_companies, load_queries_from_file
from .outreach import DEFAULT_VALUE_PROP, plan_outreach
from .scoring import score_companies
from .sender import send_due_emails


def _default_db_path() -> str:
    root = Path(__file__).resolve().parent
    return str(root / "data" / "leads.db")


def cmd_init(args: argparse.Namespace) -> None:
    init_db(args.db_path)
    print(f"Initialized database: {args.db_path}")


def cmd_discover(args: argparse.Namespace) -> None:
    init_db(args.db_path)
    config = load_config()
    if args.queries_file:
        queries = load_queries_from_file(args.queries_file)
    elif args.query:
        queries = args.query
    else:
        queries = default_manufacturing_queries(states=args.states)

    with get_connection(args.db_path) as conn:
        inserted = discover_companies(
            conn=conn,
            queries=queries,
            max_results_per_query=args.max_results,
            segment=args.segment,
            config=config,
        )
        conn.commit()

    print(f"Discovery complete. New companies inserted: {inserted}")


def cmd_enrich(args: argparse.Namespace) -> None:
    init_db(args.db_path)
    config = load_config()

    with get_connection(args.db_path) as conn:
        targets = get_companies_for_enrichment(conn, args.limit)
        updated = 0
        for row in targets:
            result = enrich_company(conn, row, config=config, max_pages=args.max_pages)
            if result.get("updated"):
                updated += 1
        conn.commit()

    print(f"Enrichment complete. Companies enriched: {updated}/{len(targets)}")


def cmd_score(args: argparse.Namespace) -> None:
    init_db(args.db_path)
    with get_connection(args.db_path) as conn:
        rows = conn.execute("SELECT * FROM companies WHERE status IN ('enriched', 'scored')").fetchall()
        scored = score_companies(conn, rows)
        conn.commit()
    print(f"Scoring complete. Companies scored: {scored}")


def cmd_plan(args: argparse.Namespace) -> None:
    init_db(args.db_path)
    start = date.fromisoformat(args.start_date)

    with get_connection(args.db_path) as conn:
        companies = get_scored_companies(conn, args.limit)
        planned = plan_outreach(
            conn,
            companies,
            start_date=start,
            value_prop=args.value_prop,
        )
        conn.commit()

    print(f"Outreach planning complete. Actions planned: {planned}")


def cmd_send(args: argparse.Namespace) -> None:
    init_db(args.db_path)
    with get_connection(args.db_path) as conn:
        result = send_due_emails(
            conn,
            action_date=args.action_date,
            limit=args.limit,
            live=args.live,
        )
        conn.commit()

    mode = "LIVE" if args.live else "DRY-RUN"
    print(f"Send mode: {mode}")
    print(result)


def cmd_export(args: argparse.Namespace) -> None:
    init_db(args.db_path)
    with get_connection(args.db_path) as conn:
        rows = list(export_leads(conn))

    out_path = Path(args.out_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    fields = [
        "domain",
        "name",
        "url",
        "fit_score",
        "contact_score",
        "outreach_score",
        "best_channel",
        "channel_reason",
        "primary_email",
        "phone",
        "contact_form_url",
        "linkedin_url",
        "source_queries",
        "status",
    ]

    with out_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row[k] for k in fields})

    print(f"Exported {len(rows)} leads to {out_path}")


def cmd_run_all(args: argparse.Namespace) -> None:
    init_db(args.db_path)
    config = load_config()

    if args.queries_file:
        queries = load_queries_from_file(args.queries_file)
    elif args.query:
        queries = args.query
    else:
        queries = default_manufacturing_queries(states=args.states)

    with get_connection(args.db_path) as conn:
        inserted = discover_companies(
            conn=conn,
            queries=queries,
            max_results_per_query=args.max_results,
            segment=args.segment,
            config=config,
        )

        targets = get_companies_for_enrichment(conn, args.limit)
        enriched = 0
        for row in targets:
            result = enrich_company(conn, row, config=config, max_pages=args.max_pages)
            if result.get("updated"):
                enriched += 1

        scored_rows = conn.execute("SELECT * FROM companies WHERE status IN ('enriched', 'scored')").fetchall()
        scored = score_companies(conn, scored_rows)

        companies = get_scored_companies(conn, args.plan_limit)
        planned = plan_outreach(
            conn,
            companies,
            start_date=date.fromisoformat(args.start_date),
            value_prop=args.value_prop,
        )

        conn.commit()

    print(
        "Pipeline complete. "
        f"Inserted={inserted}, Enriched={enriched}, Scored={scored}, PlannedActions={planned}"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Crawler + lead scoring + outreach automation for manufacturing robotics outreach."
    )
    parser.set_defaults(func=None)

    parser.add_argument("--db-path", default=_default_db_path(), help="Path to SQLite database")

    sub = parser.add_subparsers(dest="command")

    p_init = sub.add_parser("init", help="Initialize database")
    p_init.set_defaults(func=cmd_init)

    p_discover = sub.add_parser("discover", help="Discover candidate companies via search")
    p_discover.add_argument("--queries-file", help="File with one query per line")
    p_discover.add_argument("--query", action="append", help="Custom query (can be repeated)")
    p_discover.add_argument("--states", nargs="*", help="State list for default query generator")
    p_discover.add_argument("--max-results", type=int, default=20)
    p_discover.add_argument("--segment", default="manufacturing")
    p_discover.set_defaults(func=cmd_discover)

    p_enrich = sub.add_parser("enrich", help="Crawl discovered websites and extract contacts")
    p_enrich.add_argument("--limit", type=int, default=200)
    p_enrich.add_argument("--max-pages", type=int, default=4)
    p_enrich.set_defaults(func=cmd_enrich)

    p_score = sub.add_parser("score", help="Score lead fit and best outreach channel")
    p_score.set_defaults(func=cmd_score)

    p_plan = sub.add_parser("plan", help="Create outreach action sequence")
    p_plan.add_argument("--start-date", default=date.today().isoformat())
    p_plan.add_argument("--limit", type=int, default=100)
    p_plan.add_argument("--value-prop", default=DEFAULT_VALUE_PROP)
    p_plan.set_defaults(func=cmd_plan)

    p_send = sub.add_parser("send", help="Send due email actions")
    p_send.add_argument("--action-date", default=date.today().isoformat())
    p_send.add_argument("--limit", type=int, default=50)
    p_send.add_argument("--live", action="store_true", help="Actually send via SMTP")
    p_send.set_defaults(func=cmd_send)

    p_export = sub.add_parser("export", help="Export lead table to CSV")
    p_export.add_argument("--out-csv", default=str(Path(__file__).resolve().parent / "data" / "leads_export.csv"))
    p_export.set_defaults(func=cmd_export)

    p_all = sub.add_parser("run-all", help="Run discovery -> enrich -> score -> plan")
    p_all.add_argument("--queries-file", help="File with one query per line")
    p_all.add_argument("--query", action="append", help="Custom query (can be repeated)")
    p_all.add_argument("--states", nargs="*", help="State list for default query generator")
    p_all.add_argument("--max-results", type=int, default=20)
    p_all.add_argument("--segment", default="manufacturing")
    p_all.add_argument("--limit", type=int, default=150, help="Companies to enrich")
    p_all.add_argument("--max-pages", type=int, default=4)
    p_all.add_argument("--plan-limit", type=int, default=100)
    p_all.add_argument("--start-date", default=date.today().isoformat())
    p_all.add_argument("--value-prop", default=DEFAULT_VALUE_PROP)
    p_all.set_defaults(func=cmd_run_all)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        return
    args.func(args)


if __name__ == "__main__":
    main()
