from __future__ import annotations

import os
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, flash, redirect, render_template, request, url_for

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.environ.get("CLOUDSHARD_DB", BASE_DIR / "cloudshard_dashboard.db"))

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev-only-change-me")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def connect_db() -> sqlite3.Connection:
    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def init_db() -> None:
    with connect_db() as db:
        db.executescript(
            """
            CREATE TABLE IF NOT EXISTS datasets (
                dataset_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                source_url TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'QUEUED',
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS nodes (
                node_id TEXT PRIMARY KEY,
                endpoint TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'UNKNOWN',
                used_bytes INTEGER NOT NULL DEFAULT 0,
                total_bytes INTEGER NOT NULL DEFAULT 0,
                last_heartbeat TEXT
            );

            CREATE TABLE IF NOT EXISTS shards (
                shard_id TEXT PRIMARY KEY,
                dataset_id TEXT NOT NULL,
                node_id TEXT,
                size_bytes INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'UNKNOWN',
                FOREIGN KEY(dataset_id) REFERENCES datasets(dataset_id),
                FOREIGN KEY(node_id) REFERENCES nodes(node_id)
            );

            CREATE TABLE IF NOT EXISTS jobs (
                job_id TEXT PRIMARY KEY,
                dataset_id TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'QUEUED',
                stage TEXT NOT NULL DEFAULT 'Waiting for ingestion worker',
                message TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(dataset_id) REFERENCES datasets(dataset_id)
            );

            CREATE TABLE IF NOT EXISTS activity (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                level TEXT NOT NULL DEFAULT 'info',
                message TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            """
        )


def add_activity(db: sqlite3.Connection, message: str, level: str = "info") -> None:
    db.execute(
        "INSERT INTO activity(level, message, created_at) VALUES (?, ?, ?)",
        (level, message, utc_now()),
    )


def bytes_label(value: int | None) -> str:
    value = int(value or 0)
    units = ("B", "KB", "MB", "GB", "TB")
    size = float(value)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} B"
        size /= 1024
    return f"{size:.1f} TB"


app.jinja_env.filters["bytes_label"] = bytes_label


@app.route("/", methods=["GET", "POST"])
def dashboard():
    if request.method == "POST":
        action = request.form.get("action", "")

        if action == "queue_dataset":
            name = request.form.get("dataset_name", "").strip()
            source_url = request.form.get("source_url", "").strip()

            if not name or not source_url:
                flash("Dataset name and source URL are required.", "error")
                return redirect(url_for("dashboard"))

            if not source_url.startswith(("https://", "http://")):
                flash("The source URL must start with http:// or https://.", "error")
                return redirect(url_for("dashboard"))

            dataset_id = f"ds_{uuid.uuid4().hex[:12]}"
            job_id = f"job_{uuid.uuid4().hex[:12]}"
            now = utc_now()

            with connect_db() as db:
                db.execute(
                    "INSERT INTO datasets(dataset_id, name, source_url, status, created_at) "
                    "VALUES (?, ?, ?, 'QUEUED', ?)",
                    (dataset_id, name, source_url, now),
                )
                db.execute(
                    "INSERT INTO jobs(job_id, dataset_id, status, stage, message, created_at, updated_at) "
                    "VALUES (?, ?, 'QUEUED', 'Waiting for ingestion worker', "
                    "'Dashboard job created; C++ worker integration is not configured yet.', ?, ?)",
                    (job_id, dataset_id, now, now),
                )
                add_activity(
                    db,
                    f"Queued ingestion for “{name}” ({dataset_id}).",
                    "info",
                )

            flash(
                f"Dataset queued locally as {dataset_id}. Connect the C++ ingestion gRPC worker to start processing.",
                "success",
            )
            return redirect(url_for("dashboard"))

        flash("Unknown dashboard action.", "error")
        return redirect(url_for("dashboard"))

    with connect_db() as db:
        stats = {
            "datasets": db.execute("SELECT COUNT(*) FROM datasets").fetchone()[0],
            "nodes": db.execute("SELECT COUNT(*) FROM nodes").fetchone()[0],
            "online_nodes": db.execute(
                "SELECT COUNT(*) FROM nodes WHERE UPPER(status) IN ('ONLINE', 'HEALTHY', 'ACTIVE')"
            ).fetchone()[0],
            "shards": db.execute("SELECT COUNT(*) FROM shards").fetchone()[0],
            "stored_bytes": db.execute("SELECT COALESCE(SUM(size_bytes), 0) FROM shards").fetchone()[0],
            "active_jobs": db.execute(
                "SELECT COUNT(*) FROM jobs WHERE UPPER(status) IN ('QUEUED', 'RUNNING', 'DOWNLOADING', 'EXTRACTING', 'DISTRIBUTING')"
            ).fetchone()[0],
        }
        datasets = db.execute(
            """
            SELECT d.*, COUNT(DISTINCT s.shard_id) AS shard_count,
                   COALESCE(SUM(s.size_bytes), 0) AS size_bytes
            FROM datasets d
            LEFT JOIN shards s ON s.dataset_id = d.dataset_id
            GROUP BY d.dataset_id
            ORDER BY d.created_at DESC LIMIT 8
            """
        ).fetchall()
        nodes = db.execute(
            """
            SELECT n.*, COUNT(s.shard_id) AS shard_count
            FROM nodes n
            LEFT JOIN shards s ON s.node_id = n.node_id
            GROUP BY n.node_id
            ORDER BY n.node_id LIMIT 8
            """
        ).fetchall()
        jobs = db.execute(
            """
            SELECT j.*, d.name AS dataset_name
            FROM jobs j JOIN datasets d ON d.dataset_id = j.dataset_id
            ORDER BY j.created_at DESC LIMIT 8
            """
        ).fetchall()
        activity = db.execute(
            "SELECT * FROM activity ORDER BY id DESC LIMIT 8"
        ).fetchall()

    return render_template(
        "index.html",
        stats=stats,
        datasets=datasets,
        nodes=nodes,
        jobs=jobs,
        activity=activity,
    )


init_db()

if __name__ == "__main__":
    # For local development only. Put behind a production WSGI server for deployment.
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")), debug=False)
