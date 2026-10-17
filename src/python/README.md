# CloudShard Control Center

A single-page Flask dashboard starter for the four-part CloudShard architecture:

1. **Python coordinator** — catalog, node registry, metadata and placement decisions.
2. **C++ ingestion server** — download ZIPs, extract, split into shards and distribute them.
3. **C++ storage nodes** — persist shards and serve retrieval RPCs.
4. **Python dataset client** — resolve indexed records and retrieve the required data.

## Run locally

Python 3.10+ is recommended.

```bash
python -m venv .venv
source .venv/bin/activate        # Linux / WSL / macOS
# .venv\Scripts\activate         # Windows PowerShell

python -m pip install -r requirements.txt
python app.py
```

Open <http://127.0.0.1:5000>.

## Current functionality

- One dashboard page at `/`.
- SQLite-backed dataset catalog, ingestion jobs, storage-node metadata, shard metadata and activity log.
- Queue a dataset name and HTTP(S) ZIP URL through the dashboard.
- Dashboard metrics, node capacity, dataset/shard counts, jobs and activity display.
- Empty states until real nodes and shards register.

## Important: integration status

The dataset form **only creates a local dataset record and a `QUEUED` job**. It does not download the URL or call the C++ server yet. The next integration step is to generate/import the Python gRPC stubs for your ingestion service and call `IngestDataset` when the form is submitted. The C++ worker should update job stages and report dataset/shard/node metadata back to the coordinator.

The node and shard tables are ready as a starting schema, but this starter does not yet expose node-registration or metadata-update API endpoints. Keep the coordinator as the authoritative metadata writer rather than letting the ingestion server and storage nodes independently update the same SQLite database.

## Configuration

- `PORT`: Flask port (default `5000`)
- `CLOUDSHARD_DB`: SQLite database path (default `cloudshard_dashboard.db` beside `app.py`)
- `FLASK_SECRET_KEY`: session/flash-message secret. Set a real random value outside local development.

## Security note

There is intentionally no authentication, as requested. Do not expose this dashboard to an untrusted network: anyone who can reach it can queue jobs and see metadata. `debug=False` is set, but this is still a development starter; use a production WSGI server and network restrictions for deployment.
