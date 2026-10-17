from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid
from concurrent import futures
from datetime import datetime, timezone
from pathlib import Path

import grpc
from flask import Flask, flash, jsonify, redirect, render_template, request, url_for

import coordinator_pb2
import coordinator_pb2_grpc
import ingest_pb2
import ingest_pb2_grpc

import socket

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.environ.get("CLOUDSHARD_DB", BASE_DIR / "cloudshard_dashboard.db"))
COORDINATOR_HOST = os.environ.get("COORDINATOR_HOST", "0.0.0.0")
COORDINATOR_PORT = int(os.environ.get("COORDINATOR_PORT", "50051"))
INGESTION_SERVER = os.environ.get("INGESTION_SERVER", "localhost:50060")
HEARTBEAT_TIMEOUT_SECONDS = int(os.environ.get("HEARTBEAT_TIMEOUT_SECONDS", "30"))

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev-only-change-me")

def print_network_info():
    hostname = socket.gethostname()

    print(f"\nHostname: {hostname}")
    print(f"Coordinator bind address: 0.0.0.0:50051")

    try:
        # Find the local IP used for the default network route.
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
        s.close()

        print(f"Detected LAN IPv4: {local_ip}")
        print(f"Other laptops should connect to: {local_ip}:50051")

    except OSError as e:
        print(f"Could not detect LAN IPv4: {e}")

    print()

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def connect_db() -> sqlite3.Connection:
    connection = sqlite3.connect(DB_PATH, timeout=10)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA busy_timeout = 10000")
    return connection


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with connect_db() as db:
        db.execute("PRAGMA journal_mode = WAL")
        db.executescript("""
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
                dataset_id TEXT NOT NULL,
                node_id TEXT NOT NULL,
                shard_id INTEGER NOT NULL,
                dataset_shard_number INTEGER NOT NULL,
                size_bytes INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'UNKNOWN',

                PRIMARY KEY (dataset_id, node_id, shard_id),
                UNIQUE (dataset_id, dataset_shard_number),

                FOREIGN KEY (dataset_id) REFERENCES datasets(dataset_id),
                FOREIGN KEY (node_id) REFERENCES nodes(node_id)
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
        """)


def add_activity(db: sqlite3.Connection, message: str, level: str = "info") -> None:
    db.execute(
        "INSERT INTO activity(level, message, created_at) VALUES (?, ?, ?)",
        (level, message, utc_now()),
    )


def register_shard(
    db: sqlite3.Connection,
    dataset_id: str,
    node_id: str,
    size_bytes: int,
    dataset_shard_number: int,
) -> int:
    row = db.execute(
        """
        SELECT COALESCE(MAX(shard_id), 0) + 1
        FROM shards
        WHERE dataset_id = ? AND node_id = ?
        """,
        (dataset_id, node_id),
    ).fetchone()

    shard_id = row[0]

    db.execute(
        """
        INSERT INTO shards (
            dataset_id,
            node_id,
            shard_id,
            dataset_shard_number,
            size_bytes,
            status
        )
        VALUES (?, ?, ?, ?, ?, 'UNKNOWN')
        """,
        (
            dataset_id,
            node_id,
            shard_id,
            dataset_shard_number,
            size_bytes,
        ),
    )

    return shard_id



def update_job(job_id: str, status: str, stage: str, message: str) -> None:
    with connect_db() as db:
        row = db.execute("SELECT dataset_id FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        if not row:
            return
        now = utc_now()
        db.execute(
            "UPDATE jobs SET status=?, stage=?, message=?, updated_at=? WHERE job_id=?",
            (status, stage, message, now, job_id),
        )
        db.execute("UPDATE datasets SET status=? WHERE dataset_id=?", (status, row["dataset_id"]))
        add_activity(db, f"{job_id}: {stage} — {message}", "error" if status == "FAILED" else "info")


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


def get_online_nodes():
    """Use heartbeat freshness, rather than trusting an old in-memory online flag."""
    with connect_db() as db:
        rows = db.execute("SELECT node_id, endpoint, last_heartbeat FROM nodes").fetchall()
    now = datetime.now(timezone.utc)
    online = []
    for row in rows:
        fresh = False
        if row["last_heartbeat"]:
            try:
                last = datetime.fromisoformat(row["last_heartbeat"])
                if last.tzinfo is None:
                    last = last.replace(tzinfo=timezone.utc)
                fresh = (now - last).total_seconds() <= HEARTBEAT_TIMEOUT_SECONDS
            except ValueError:
                pass
        if fresh:
            online.append({"node_id": row["node_id"], "endpoint": row["endpoint"]})
    return online


class CoordinatorService(coordinator_pb2_grpc.CoordinatorServicer):
    def RegisterNode(self, req, context):
        node_id = req.node_id.strip()
        endpoint = req.endpoint.strip()
        if not node_id or not endpoint:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Node ID and endpoint are required")

        now = utc_now()
        with connect_db() as db:
            existing = db.execute("SELECT node_id FROM nodes WHERE node_id=?", (node_id,)).fetchone()
            db.execute("""
                INSERT INTO nodes(node_id, endpoint, status, last_heartbeat)
                VALUES (?, ?, 'ONLINE', ?)
                ON CONFLICT(node_id) DO UPDATE SET
                    endpoint=excluded.endpoint,
                    status='ONLINE',
                    last_heartbeat=excluded.last_heartbeat
            """, (node_id, endpoint, now))
            add_activity(db, f"Node {node_id} registered at {endpoint}.")
        return coordinator_pb2.RegisterNodeResponse(
            success=True,
            message="Node is already registered" if existing else "Node registered successfully",
            already_registered=bool(existing),
        )

    def Heartbeat(self, req, context):
        node_id = req.node_id.strip()
        now = utc_now()
        with connect_db() as db:
            existing = db.execute("SELECT node_id FROM nodes WHERE node_id=?", (node_id,)).fetchone()
            if not existing:
                context.abort(grpc.StatusCode.NOT_FOUND, "Node is not registered")
            db.execute("""
                UPDATE nodes
                SET status='ONLINE', last_heartbeat=?, used_bytes=?, total_bytes=?
                WHERE node_id=?
            """, (now, int(req.used_bytes), int(req.total_bytes), node_id))
        return coordinator_pb2.HeartbeatResponse(accepted=True, message="Heartbeat received")


def start_grpc_server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    coordinator_pb2_grpc.add_CoordinatorServicer_to_server(CoordinatorService(), server)
    bound_port = server.add_insecure_port(f"{COORDINATOR_HOST}:{COORDINATOR_PORT}")
    if bound_port == 0:
        raise RuntimeError(f"Could not bind coordinator gRPC to {COORDINATOR_HOST}:{COORDINATOR_PORT}")
    server.start()
    print(f"Coordinator gRPC listening on {COORDINATOR_HOST}:{bound_port}")
    server.wait_for_termination()


def run_ingestion(job_id: str, dataset_id: str, source_url: str, allocations: list[tuple[str, int]]) -> None:
    try:
        update_job(job_id, "RUNNING", "Contacting ingestion worker", f"Connecting to {INGESTION_SERVER}.")
        endpoints = [endpoint for endpoint, count in allocations if count > 0]
        counts = [count for endpoint, count in allocations if count > 0]
        if not endpoints:
            raise ValueError("No positive shard allocations were provided.")

        with grpc.insecure_channel(INGESTION_SERVER) as channel:
            stub = ingest_pb2_grpc.IngestDatasetStub(channel)
            request_message = ingest_pb2.IngestDatasetRequest(
                dataset_id=dataset_id,
                zip_url=source_url,
                node_endpoints=endpoints,
                shard_counts=counts,
            )
            # This is the RPC name from the supplied client code.
            response = stub.downloadDataset(request_message, timeout=300)
            with connect_db() as db:
                db.execute("BEGIN IMMEDIATE")

                dataset_shard_number = db.execute(
                    """
                    SELECT COALESCE(MAX(dataset_shard_number), 0) + 1
                    FROM shards
                    WHERE dataset_id = ?
                    """,
                    (dataset_id,),
                ).fetchone()[0]

                for endpoint, count in allocations:
                    if count <= 0:
                        continue

                    row = db.execute(
                        "SELECT node_id FROM nodes WHERE endpoint = ?",
                        (endpoint,),
                    ).fetchone()

                    if row is None:
                        raise ValueError(f"Unknown node endpoint: {endpoint}")

                    node_id = row["node_id"]

                    for _ in range(count):
                        register_shard(
                            db=db,
                            dataset_id=dataset_id,
                            node_id=node_id,
                            size_bytes=0,
                            dataset_shard_number=dataset_shard_number,
                        )
                        dataset_shard_number += 1

        update_job(
            job_id, "COMPLETED", "Ingestion RPC returned",
            f"Ingestion worker responded successfully: {str(response)[:700]}",
        )
    except grpc.RpcError as exc:
        update_job(
            job_id, "FAILED", "Ingestion RPC failed",
            f"{exc.code().name}: {exc.details() or 'No details returned'}",
        )
    except Exception as exc:
        update_job(job_id, "FAILED", "Ingestion failed", str(exc))


@app.route("/", methods=["GET", "POST"])
def dashboard():
    if request.method == "POST":
        action = request.form.get("action", "")
        if action != "queue_dataset":
            flash("Unknown dashboard action.", "error")
            return redirect(url_for("dashboard"))

        name = request.form.get("dataset_name", "").strip()
        source_url = request.form.get("source_url", "").strip()
        if not name or not source_url:
            flash("Dataset name and source URL are required.", "error")
            return redirect(url_for("dashboard"))
        if not source_url.startswith(("https://", "http://")):
            flash("The source URL must start with http:// or https://.", "error")
            return redirect(url_for("dashboard"))

        online_nodes = get_online_nodes()
        if not online_nodes:
            flash("No nodes with a recent heartbeat are online. Start/register nodes first.", "error")
            return redirect(url_for("dashboard"))

        allocations = []
        total_shards = int(request.form.get("total_shards", "1"))

        if total_shards < 1:
            flash("Total shards must be at least 1.", "error")
            return redirect(url_for("dashboard"))

        base = total_shards // len(online_nodes)
        remainder = total_shards % len(online_nodes)

        for i, node in enumerate(online_nodes):
            count = base + (1 if i < remainder else 0)
            if count > 0:
                allocations.append((node["endpoint"], count))
        if not any(count > 0 for _, count in allocations):
            flash("At least one node must receive one or more shards.", "error")
            return redirect(url_for("dashboard"))

        dataset_id = f"ds_{uuid.uuid4().hex[:12]}"
        job_id = f"job_{uuid.uuid4().hex[:12]}"
        now = utc_now()
        with connect_db() as db:
            db.execute(
                "INSERT INTO datasets(dataset_id, name, source_url, status, created_at) VALUES (?, ?, ?, 'QUEUED', ?)",
                (dataset_id, name, source_url, now),
            )
            db.execute(
                """INSERT INTO jobs(job_id, dataset_id, status, stage, message, created_at, updated_at)
                   VALUES (?, ?, 'QUEUED', 'Queued for ingestion', 'Waiting for background ingestion request.', ?, ?)""",
                (job_id, dataset_id, now, now),
            )
            add_activity(db, f"Queued dataset “{name}” ({dataset_id}) with job {job_id}.")
        # A separate thread keeps the Flask request responsive while the gRPC RPC runs.
        threading.Thread(
            target=run_ingestion,
            args=(job_id, dataset_id, source_url, allocations),
            daemon=True,
        ).start()
        flash(f"Dataset queued: {dataset_id}. The dashboard will update automatically.", "success")
        return redirect(url_for("dashboard"))

    
    data = dashboard_data()

    return render_template(
        "index.html",
        stats=data["stats"],
        datasets=data["datasets"],
        nodes=data["nodes"],
        jobs=data["jobs"],
        activity=data["activity"],
        online_nodes=get_online_nodes(),
    )



def dashboard_data():
    with connect_db() as db:
        stats = {
            "datasets": db.execute("SELECT COUNT(*) FROM datasets").fetchone()[0],
            "nodes": db.execute("SELECT COUNT(*) FROM nodes").fetchone()[0],
            "online_nodes": 0,
            "shards": db.execute("SELECT COUNT(*) FROM shards").fetchone()[0],
            "stored_bytes": db.execute("SELECT COALESCE(SUM(size_bytes),0) FROM shards").fetchone()[0],
            "active_jobs": db.execute(
                "SELECT COUNT(*) FROM jobs WHERE status IN ('QUEUED','RUNNING','DOWNLOADING','EXTRACTING','DISTRIBUTING')"
            ).fetchone()[0],
        }
        node_rows = db.execute("""
            SELECT n.*, COUNT(s.shard_id) AS shard_count
            FROM nodes n LEFT JOIN shards s ON s.node_id=n.node_id
            GROUP BY n.node_id ORDER BY n.node_id
        """).fetchall()
        datasets = db.execute("""
            SELECT d.*, COUNT(DISTINCT s.shard_id) AS shard_count,
                   COALESCE(SUM(s.size_bytes),0) AS size_bytes
            FROM datasets d LEFT JOIN shards s ON s.dataset_id=d.dataset_id
            GROUP BY d.dataset_id ORDER BY d.created_at DESC LIMIT 20
        """).fetchall()
        jobs = db.execute("""
            SELECT j.*, d.name AS dataset_name FROM jobs j
            JOIN datasets d ON d.dataset_id=j.dataset_id
            ORDER BY j.created_at DESC LIMIT 20
        """).fetchall()
        activity = db.execute("SELECT * FROM activity ORDER BY id DESC LIMIT 20").fetchall()

    now = datetime.now(timezone.utc)
    nodes = []
    online_count = 0
    for row in node_rows:
        node = dict(row)
        fresh = False
        if node["last_heartbeat"]:
            try:
                last = datetime.fromisoformat(node["last_heartbeat"])
                if last.tzinfo is None:
                    last = last.replace(tzinfo=timezone.utc)
                fresh = (now - last).total_seconds() <= HEARTBEAT_TIMEOUT_SECONDS
            except ValueError:
                pass
        node["status"] = "ONLINE" if fresh else "OFFLINE"
        if fresh:
            online_count += 1
        nodes.append(node)
    stats["online_nodes"] = online_count

    def clean_rows(rows):
        return [dict(row) for row in rows]

    return {
        "stats": stats,
        "datasets": clean_rows(datasets),
        "nodes": nodes[:20],
        "jobs": clean_rows(jobs),
        "activity": clean_rows(activity),
        "updated_at": utc_now(),
    }


@app.get("/api/dashboard")
def api_dashboard():
    data = dashboard_data()
    # JSON encoding handles SQLite scalar types, but not arbitrary objects.
    return jsonify(data)

@app.get("/api/nodes")
def api_online_nodes():
    return jsonify({
        "nodes": get_online_nodes()
    })

init_db()

if __name__ == "__main__":
    print_network_info()
    grpc_thread = threading.Thread(target=start_grpc_server, daemon=True)
    grpc_thread.start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")), debug=False, threaded=True)