from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def get_connection(db_path: str) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: str) -> None:
    with get_connection(db_path) as conn:
        conn.executescript(
            """
            PRAGMA journal_mode=WAL;

            CREATE TABLE IF NOT EXISTS companies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                domain TEXT NOT NULL UNIQUE,
                name TEXT,
                url TEXT,
                segment TEXT,
                source_queries TEXT DEFAULT '[]',
                status TEXT DEFAULT 'new',
                fit_score REAL DEFAULT 0,
                contact_score REAL DEFAULT 0,
                outreach_score REAL DEFAULT 0,
                best_channel TEXT,
                channel_reason TEXT,
                phone TEXT,
                contact_form_url TEXT,
                linkedin_url TEXT,
                primary_email TEXT,
                notes TEXT,
                last_crawled_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS contacts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id INTEGER NOT NULL,
                name TEXT,
                title TEXT,
                email TEXT,
                phone TEXT,
                linkedin_url TEXT,
                source_url TEXT,
                confidence REAL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE CASCADE,
                UNIQUE(company_id, email, phone, linkedin_url)
            );

            CREATE TABLE IF NOT EXISTS pages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id INTEGER NOT NULL,
                url TEXT NOT NULL,
                title TEXT,
                text_excerpt TEXT,
                fetched_at TEXT NOT NULL,
                FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE CASCADE,
                UNIQUE(company_id, url)
            );

            CREATE TABLE IF NOT EXISTS outreach_actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id INTEGER NOT NULL,
                contact_id INTEGER,
                step_name TEXT NOT NULL,
                channel TEXT NOT NULL,
                subject TEXT,
                body TEXT,
                scheduled_for TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                sent_at TEXT,
                error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE CASCADE,
                FOREIGN KEY(contact_id) REFERENCES contacts(id) ON DELETE SET NULL,
                UNIQUE(company_id, step_name, scheduled_for)
            );
            """
        )


def _merge_queries(existing: Optional[str], new_query: str) -> str:
    values = []
    if existing:
        try:
            values = json.loads(existing)
        except json.JSONDecodeError:
            values = []
    if new_query and new_query not in values:
        values.append(new_query)
    return json.dumps(values)


def upsert_company(
    conn: sqlite3.Connection,
    domain: str,
    url: str,
    segment: str,
    source_query: str,
) -> int:
    now = utc_now_iso()
    row = conn.execute(
        "SELECT id, source_queries FROM companies WHERE domain = ?",
        (domain,),
    ).fetchone()
    if row:
        merged = _merge_queries(row["source_queries"], source_query)
        conn.execute(
            """
            UPDATE companies
            SET url = COALESCE(url, ?),
                segment = COALESCE(segment, ?),
                source_queries = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (url, segment, merged, now, row["id"]),
        )
        return int(row["id"])

    conn.execute(
        """
        INSERT INTO companies (
            domain, name, url, segment, source_queries, status,
            created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, 'discovered', ?, ?)
        """,
        (domain, None, url, segment, json.dumps([source_query]), now, now),
    )
    return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])


def get_companies_for_enrichment(conn: sqlite3.Connection, limit: int) -> List[sqlite3.Row]:
    return conn.execute(
        """
        SELECT * FROM companies
        WHERE status IN ('new', 'discovered') OR last_crawled_at IS NULL
        ORDER BY outreach_score DESC, created_at ASC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def upsert_contact(
    conn: sqlite3.Connection,
    company_id: int,
    email: Optional[str],
    phone: Optional[str],
    linkedin_url: Optional[str],
    source_url: str,
    confidence: float,
    name: Optional[str] = None,
    title: Optional[str] = None,
) -> None:
    if not any([email, phone, linkedin_url]):
        return

    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO contacts (
            company_id, name, title, email, phone, linkedin_url, source_url,
            confidence, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(company_id, email, phone, linkedin_url) DO UPDATE SET
            source_url = excluded.source_url,
            confidence = MAX(confidence, excluded.confidence),
            updated_at = excluded.updated_at
        """,
        (
            company_id,
            name,
            title,
            email,
            phone,
            linkedin_url,
            source_url,
            confidence,
            now,
            now,
        ),
    )


def upsert_page(
    conn: sqlite3.Connection,
    company_id: int,
    url: str,
    title: str,
    text_excerpt: str,
) -> None:
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO pages (company_id, url, title, text_excerpt, fetched_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(company_id, url) DO UPDATE SET
            title = excluded.title,
            text_excerpt = excluded.text_excerpt,
            fetched_at = excluded.fetched_at
        """,
        (company_id, url, title, text_excerpt[:5000], now),
    )


def update_company_enrichment(
    conn: sqlite3.Connection,
    company_id: int,
    updates: Dict[str, Optional[str]],
) -> None:
    now = utc_now_iso()
    allowed = {
        "name",
        "status",
        "phone",
        "contact_form_url",
        "linkedin_url",
        "primary_email",
        "last_crawled_at",
        "notes",
    }
    assignments = []
    values: List[object] = []
    for key, value in updates.items():
        if key in allowed:
            assignments.append(f"{key} = ?")
            values.append(value)
    assignments.append("updated_at = ?")
    values.append(now)
    values.append(company_id)

    query = f"UPDATE companies SET {', '.join(assignments)} WHERE id = ?"
    conn.execute(query, values)


def update_company_scoring(
    conn: sqlite3.Connection,
    company_id: int,
    fit_score: float,
    contact_score: float,
    outreach_score: float,
    best_channel: str,
    channel_reason: str,
) -> None:
    now = utc_now_iso()
    conn.execute(
        """
        UPDATE companies
        SET fit_score = ?,
            contact_score = ?,
            outreach_score = ?,
            best_channel = ?,
            channel_reason = ?,
            status = 'scored',
            updated_at = ?
        WHERE id = ?
        """,
        (
            fit_score,
            contact_score,
            outreach_score,
            best_channel,
            channel_reason,
            now,
            company_id,
        ),
    )


def get_scored_companies(conn: sqlite3.Connection, limit: int) -> List[sqlite3.Row]:
    return conn.execute(
        """
        SELECT * FROM companies
        WHERE status IN ('scored', 'enriched')
        ORDER BY outreach_score DESC, fit_score DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def get_company_text(conn: sqlite3.Connection, company_id: int) -> str:
    rows = conn.execute(
        "SELECT text_excerpt FROM pages WHERE company_id = ?",
        (company_id,),
    ).fetchall()
    return "\n".join(r["text_excerpt"] or "" for r in rows)


def get_primary_contact(conn: sqlite3.Connection, company_id: int) -> Optional[sqlite3.Row]:
    return conn.execute(
        """
        SELECT * FROM contacts
        WHERE company_id = ?
        ORDER BY confidence DESC, id ASC
        LIMIT 1
        """,
        (company_id,),
    ).fetchone()


def insert_outreach_action(
    conn: sqlite3.Connection,
    company_id: int,
    contact_id: Optional[int],
    step_name: str,
    channel: str,
    subject: str,
    body: str,
    scheduled_for: str,
) -> None:
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO outreach_actions (
            company_id, contact_id, step_name, channel, subject, body,
            scheduled_for, status, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)
        ON CONFLICT(company_id, step_name, scheduled_for) DO UPDATE SET
            channel = excluded.channel,
            subject = excluded.subject,
            body = excluded.body,
            updated_at = excluded.updated_at
        """,
        (
            company_id,
            contact_id,
            step_name,
            channel,
            subject,
            body,
            scheduled_for,
            now,
            now,
        ),
    )


def get_due_email_actions(conn: sqlite3.Connection, action_date: str, limit: int) -> List[sqlite3.Row]:
    return conn.execute(
        """
        SELECT oa.*, c.name AS company_name, c.domain, c.primary_email, c.contact_form_url,
               c.phone, c.linkedin_url
        FROM outreach_actions oa
        JOIN companies c ON c.id = oa.company_id
        WHERE oa.channel = 'email'
          AND oa.status = 'pending'
          AND oa.scheduled_for <= ?
        ORDER BY oa.scheduled_for ASC, c.outreach_score DESC
        LIMIT ?
        """,
        (action_date, limit),
    ).fetchall()


def mark_action_status(
    conn: sqlite3.Connection,
    action_id: int,
    status: str,
    sent_at: Optional[str] = None,
    error: Optional[str] = None,
) -> None:
    now = utc_now_iso()
    conn.execute(
        """
        UPDATE outreach_actions
        SET status = ?,
            sent_at = COALESCE(?, sent_at),
            error = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (status, sent_at, error, now, action_id),
    )


def export_leads(conn: sqlite3.Connection) -> Iterable[sqlite3.Row]:
    return conn.execute(
        """
        SELECT domain, COALESCE(name, '') AS name, COALESCE(url, '') AS url,
               fit_score, contact_score, outreach_score,
               COALESCE(best_channel, '') AS best_channel,
               COALESCE(channel_reason, '') AS channel_reason,
               COALESCE(primary_email, '') AS primary_email,
               COALESCE(phone, '') AS phone,
               COALESCE(contact_form_url, '') AS contact_form_url,
               COALESCE(linkedin_url, '') AS linkedin_url,
               COALESCE(source_queries, '[]') AS source_queries,
               status
        FROM companies
        ORDER BY outreach_score DESC, fit_score DESC, domain ASC
        """
    )
