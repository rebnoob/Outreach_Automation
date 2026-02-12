from __future__ import annotations

import csv
import io
import os
from datetime import date
from pathlib import Path
from typing import Dict, List

from flask import Flask, Response, flash, redirect, render_template, request, url_for

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


def _get_db_path() -> str:
    return os.getenv("OUTREACH_DB_PATH", _default_db_path())


def _parse_int(value: str, default: int, min_value: int, max_value: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(min_value, min(max_value, parsed))


def _parse_float(value: str, default: float, min_value: float, max_value: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return max(min_value, min(max_value, parsed))


def _split_lines(raw_text: str) -> List[str]:
    return [line.strip() for line in (raw_text or "").splitlines() if line.strip()]


def _split_csv_values(raw_text: str) -> List[str]:
    return [part.strip() for part in (raw_text or "").split(",") if part.strip()]


def _fetch_dashboard_data(
    db_path: str,
    search: str,
    status_filter: str,
    channel_filter: str,
    min_score: float,
    limit: int,
) -> Dict[str, object]:
    with get_connection(db_path) as conn:
        totals = conn.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM companies) AS companies,
                (SELECT COUNT(*) FROM contacts) AS contacts,
                (SELECT COUNT(*) FROM pages) AS pages,
                (SELECT COUNT(*) FROM outreach_actions WHERE status = 'pending') AS pending_actions,
                (SELECT COUNT(*) FROM outreach_actions WHERE status = 'sent') AS sent_actions
            """
        ).fetchone()

        status_rows = conn.execute(
            "SELECT status, COUNT(*) AS count FROM companies GROUP BY status ORDER BY count DESC"
        ).fetchall()

        channel_rows = conn.execute(
            """
            SELECT best_channel, COUNT(*) AS count
            FROM companies
            WHERE best_channel IS NOT NULL AND best_channel != ''
            GROUP BY best_channel
            ORDER BY count DESC
            """
        ).fetchall()

        base_query = """
            SELECT
                id,
                domain,
                COALESCE(name, '') AS name,
                COALESCE(segment, '') AS segment,
                COALESCE(status, '') AS status,
                ROUND(COALESCE(fit_score, 0), 1) AS fit_score,
                ROUND(COALESCE(contact_score, 0), 1) AS contact_score,
                ROUND(COALESCE(outreach_score, 0), 1) AS outreach_score,
                COALESCE(best_channel, '') AS best_channel,
                COALESCE(primary_email, '') AS primary_email,
                COALESCE(phone, '') AS phone,
                COALESCE(contact_form_url, '') AS contact_form_url,
                COALESCE(linkedin_url, '') AS linkedin_url,
                COALESCE(notes, '') AS notes,
                COALESCE(updated_at, '') AS updated_at
            FROM companies
        """

        conditions: List[str] = []
        params: List[object] = []

        if search:
            wildcard = f"%{search}%"
            conditions.append("(domain LIKE ? OR name LIKE ? OR notes LIKE ?)")
            params.extend([wildcard, wildcard, wildcard])

        if status_filter:
            conditions.append("status = ?")
            params.append(status_filter)

        if channel_filter:
            conditions.append("best_channel = ?")
            params.append(channel_filter)

        if min_score > 0:
            conditions.append("outreach_score >= ?")
            params.append(min_score)

        if conditions:
            base_query += " WHERE " + " AND ".join(conditions)

        base_query += " ORDER BY outreach_score DESC, fit_score DESC, updated_at DESC LIMIT ?"
        params.append(limit)

        leads = conn.execute(base_query, params).fetchall()

        action_rows = conn.execute(
            """
            SELECT
                oa.id,
                oa.step_name,
                oa.channel,
                oa.status,
                oa.scheduled_for,
                COALESCE(c.name, c.domain) AS company_name,
                c.domain
            FROM outreach_actions oa
            JOIN companies c ON c.id = oa.company_id
            ORDER BY oa.created_at DESC, oa.id DESC
            LIMIT 40
            """
        ).fetchall()

    return {
        "totals": totals,
        "status_rows": status_rows,
        "channel_rows": channel_rows,
        "leads": leads,
        "actions": action_rows,
    }


def create_app() -> Flask:
    app = Flask(__name__)
    app.secret_key = os.getenv("OUTREACH_WEB_SECRET", "robotics-outreach-local-dev")

    @app.get("/")
    def index() -> str:
        db_path = _get_db_path()
        init_db(db_path)

        search = request.args.get("q", "").strip()
        status_filter = request.args.get("status", "").strip()
        channel_filter = request.args.get("channel", "").strip()
        min_score = _parse_float(request.args.get("min_score", "0"), 0.0, 0.0, 100.0)
        limit = _parse_int(request.args.get("limit", "100"), 100, 10, 500)

        data = _fetch_dashboard_data(
            db_path=db_path,
            search=search,
            status_filter=status_filter,
            channel_filter=channel_filter,
            min_score=min_score,
            limit=limit,
        )

        return render_template(
            "index.html",
            db_path=db_path,
            today=date.today().isoformat(),
            default_value_prop=DEFAULT_VALUE_PROP,
            search=search,
            status_filter=status_filter,
            channel_filter=channel_filter,
            min_score=min_score,
            limit=limit,
            **data,
        )

    @app.post("/run")
    def run_action():
        db_path = _get_db_path()
        init_db(db_path)
        action = request.form.get("action", "").strip()

        if not action:
            flash("Missing action", "error")
            return redirect(url_for("index"))

        try:
            if action == "init":
                init_db(db_path)
                flash(f"Database initialized at {db_path}", "success")

            elif action == "discover":
                config = load_config()
                query_text = request.form.get("queries", "")
                queries_file = request.form.get("queries_file", "").strip()
                states_raw = request.form.get("states", "")
                max_results = _parse_int(request.form.get("max_results", "20"), 20, 1, 100)
                segment = request.form.get("segment", "manufacturing").strip() or "manufacturing"

                if _split_lines(query_text):
                    queries = _split_lines(query_text)
                elif queries_file:
                    queries = load_queries_from_file(queries_file)
                else:
                    states = _split_csv_values(states_raw)
                    queries = default_manufacturing_queries(states=states or None)

                with get_connection(db_path) as conn:
                    inserted = discover_companies(
                        conn=conn,
                        queries=queries,
                        max_results_per_query=max_results,
                        segment=segment,
                        config=config,
                    )
                    conn.commit()
                flash(f"Discovery complete. New companies inserted: {inserted}", "success")

            elif action == "enrich":
                config = load_config()
                limit = _parse_int(request.form.get("limit", "200"), 200, 1, 2000)
                max_pages = _parse_int(request.form.get("max_pages", "4"), 4, 1, 20)

                with get_connection(db_path) as conn:
                    targets = get_companies_for_enrichment(conn, limit)
                    updated = 0
                    for row in targets:
                        result = enrich_company(conn, row, config=config, max_pages=max_pages)
                        if result.get("updated"):
                            updated += 1
                    conn.commit()
                flash(f"Enrichment complete. Companies enriched: {updated}/{len(targets)}", "success")

            elif action == "score":
                with get_connection(db_path) as conn:
                    rows = conn.execute(
                        "SELECT * FROM companies WHERE status IN ('enriched', 'scored')"
                    ).fetchall()
                    scored = score_companies(conn, rows)
                    conn.commit()
                flash(f"Scoring complete. Companies scored: {scored}", "success")

            elif action == "plan":
                limit = _parse_int(request.form.get("limit", "100"), 100, 1, 2000)
                start_date = request.form.get("start_date", date.today().isoformat())
                value_prop = request.form.get("value_prop", DEFAULT_VALUE_PROP).strip() or DEFAULT_VALUE_PROP

                with get_connection(db_path) as conn:
                    companies = get_scored_companies(conn, limit)
                    planned = plan_outreach(
                        conn,
                        companies,
                        start_date=date.fromisoformat(start_date),
                        value_prop=value_prop,
                    )
                    conn.commit()
                flash(f"Outreach planning complete. Actions planned: {planned}", "success")

            elif action == "run_all":
                config = load_config()
                query_text = request.form.get("queries", "")
                queries_file = request.form.get("queries_file", "").strip()
                states_raw = request.form.get("states", "")
                max_results = _parse_int(request.form.get("max_results", "20"), 20, 1, 100)
                segment = request.form.get("segment", "manufacturing").strip() or "manufacturing"
                enrich_limit = _parse_int(request.form.get("enrich_limit", "150"), 150, 1, 3000)
                max_pages = _parse_int(request.form.get("max_pages", "4"), 4, 1, 20)
                plan_limit = _parse_int(request.form.get("plan_limit", "100"), 100, 1, 3000)
                start_date = request.form.get("start_date", date.today().isoformat())
                value_prop = request.form.get("value_prop", DEFAULT_VALUE_PROP).strip() or DEFAULT_VALUE_PROP

                if _split_lines(query_text):
                    queries = _split_lines(query_text)
                elif queries_file:
                    queries = load_queries_from_file(queries_file)
                else:
                    states = _split_csv_values(states_raw)
                    queries = default_manufacturing_queries(states=states or None)

                with get_connection(db_path) as conn:
                    inserted = discover_companies(
                        conn=conn,
                        queries=queries,
                        max_results_per_query=max_results,
                        segment=segment,
                        config=config,
                    )

                    targets = get_companies_for_enrichment(conn, enrich_limit)
                    enriched = 0
                    for row in targets:
                        result = enrich_company(conn, row, config=config, max_pages=max_pages)
                        if result.get("updated"):
                            enriched += 1

                    rows = conn.execute(
                        "SELECT * FROM companies WHERE status IN ('enriched', 'scored')"
                    ).fetchall()
                    scored = score_companies(conn, rows)

                    companies = get_scored_companies(conn, plan_limit)
                    planned = plan_outreach(
                        conn,
                        companies,
                        start_date=date.fromisoformat(start_date),
                        value_prop=value_prop,
                    )
                    conn.commit()

                flash(
                    f"Pipeline complete. Inserted={inserted}, Enriched={enriched}, "
                    f"Scored={scored}, PlannedActions={planned}",
                    "success",
                )

            elif action == "send":
                action_date = request.form.get("action_date", date.today().isoformat())
                limit = _parse_int(request.form.get("limit", "50"), 50, 1, 5000)
                live = request.form.get("live") == "on"

                with get_connection(db_path) as conn:
                    result = send_due_emails(conn, action_date=action_date, limit=limit, live=live)
                    conn.commit()

                mode = "LIVE" if live else "DRY-RUN"
                flash(
                    (
                        f"Send mode: {mode}. Processed={result['processed']}, "
                        f"SentOrSimulated={result['sent_or_simulated']}, "
                        f"Failed={result['failed']}, Skipped={result['skipped']}"
                    ),
                    "success",
                )

            else:
                flash(f"Unsupported action: {action}", "error")

        except Exception as exc:
            flash(f"Action '{action}' failed: {exc}", "error")

        return redirect(url_for("index"))

    @app.get("/export.csv")
    def export_csv() -> Response:
        db_path = _get_db_path()
        init_db(db_path)

        with get_connection(db_path) as conn:
            rows = list(export_leads(conn))

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

        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row[key] for key in fields})

        payload = buffer.getvalue()
        return Response(
            payload,
            mimetype="text/csv",
            headers={"Content-Disposition": "attachment; filename=leads_export.csv"},
        )

    @app.get("/health")
    def health() -> Dict[str, str]:
        db_path = _get_db_path()
        init_db(db_path)
        return {"status": "ok", "db_path": db_path}

    return app


app = create_app()


if __name__ == "__main__":
    port = _parse_int(os.getenv("PORT", "8000"), 8000, 1, 65535)
    app.run(host="0.0.0.0", port=port, debug=False)
