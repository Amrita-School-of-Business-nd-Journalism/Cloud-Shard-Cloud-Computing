
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import platform
import statistics
import sys
import time
import urllib.request
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import grpc

try:
    import resource
except ImportError:
    resource = None

try:
    import shard_pb2
    import shard_pb2_grpc
except ImportError as exc:
    raise SystemExit(
        "Could not import shard_pb2 / shard_pb2_grpc. "
        "Make sure the generated protobuf Python modules are importable."
    ) from exc


DEFAULT_FLASK_URL = os.environ.get(
    "FLASK_SERVER", "http://localhost:5000"
)

RPC_TIMEOUT = 120
CHANNEL_READY_TIMEOUT = 10
MAX_RECEIVE_BYTES = 256 * 1024 * 1024


# ============================================================
# General utilities
# ============================================================

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def http_json(url: str, timeout: float = 15.0) -> Any:
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json"},
    )

    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def get_dashboard(flask_url: str) -> dict:
    return http_json(
        f"{flask_url.rstrip('/')}/api/dashboard"
    )


def get_online_nodes(flask_url: str) -> list[dict]:
    data = http_json(
        f"{flask_url.rstrip('/')}/api/nodes"
    )

    return [
        node
        for node in data.get("nodes", [])
        if node.get("endpoint")
    ]


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def percentile(values: list[float], p: float) -> float:
    if not values:
        return float("nan")

    ordered = sorted(values)

    if len(ordered) == 1:
        return ordered[0]

    position = (len(ordered) - 1) * p
    low = math.floor(position)
    high = math.ceil(position)

    if low == high:
        return ordered[low]

    return (
        ordered[low] * (high - position)
        + ordered[high] * (position - low)
    )


def fmt_seconds(value: float) -> str:
    if not math.isfinite(value):
        return "n/a"

    return f"{value:.6f} s"


def fmt_rate(value: float) -> str:
    if not math.isfinite(value):
        return "n/a"

    return f"{value:.3f} MiB/s"


def safe_json(value: Any) -> str:
    return json.dumps(
        value,
        indent=2,
        ensure_ascii=False,
        default=str,
    )


# ============================================================
# Portable client resource measurements
# ============================================================

def get_peak_rss_mb() -> float | None:
    if resource is None:
        return None

    try:
        usage = resource.getrusage(resource.RUSAGE_SELF)
        value = usage.ru_maxrss

        # Linux: KiB; macOS: bytes.
        if sys.platform == "darwin":
            return value / (1024 * 1024)

        return value / 1024

    except Exception:
        return None


def get_cpu_usage() -> tuple[float | None, float | None]:
    if resource is None:
        return None, None

    try:
        usage = resource.getrusage(resource.RUSAGE_SELF)
        return usage.ru_utime, usage.ru_stime

    except Exception:
        return None, None


# ============================================================
# CloudShard node retrieval
# ============================================================

def get_shards_from_node(
    endpoint: str,
    dataset_id: str,
) -> dict:
    """
    Fetch all shards for a dataset from one storage node.
    No global shard-order metadata is required.
    """

    options = [
        ("grpc.max_receive_message_length", MAX_RECEIVE_BYTES),
        ("grpc.max_send_message_length", MAX_RECEIVE_BYTES),
    ]

    started = time.perf_counter()

    with grpc.insecure_channel(
        endpoint,
        options=options,
    ) as channel:

        grpc.channel_ready_future(channel).result(
            timeout=CHANNEL_READY_TIMEOUT
        )

        stub = shard_pb2_grpc.ShardStub(channel)

        rpc_started = time.perf_counter()

        response = stub.listShards(
            shard_pb2.ListShardsRequest(
                dataset_id_=dataset_id
            ),
            timeout=RPC_TIMEOUT,
        )

        rpc_seconds = time.perf_counter() - rpc_started

        shards = []

        for shard in response.shards:
            shards.append({
                "node_endpoint": endpoint,
                "shard_id": int(
                    getattr(shard, "shard_id_", 0)
                ),
                "size_field": int(
                    getattr(shard, "size_", 0)
                ),
                "data": bytes(
                    getattr(shard, "data_", b"")
                ),
            })

    return {
        "shards": shards,
        "rpc_seconds": rpc_seconds,
        "total_seconds": time.perf_counter() - started,
    }


def retrieve_cloudshard(
    flask_url: str,
    dataset_id: str,
) -> dict:
    """
    Retrieve shards from every online node and concatenate them
    without checking or sorting by shard IDs or sequence numbers.
    """

    started = time.perf_counter()

    # ---------------- Node discovery ----------------

    discovery_started = time.perf_counter()

    endpoints = get_online_nodes(flask_url)

    discovery_seconds = (
        time.perf_counter() - discovery_started
    )

    if not endpoints:
        raise RuntimeError(
            "No online storage nodes found from /api/nodes."
        )

    # ---------------- Retrieve from each node ----------------

    rpc_seconds_by_node: dict[str, float] = {}
    shards_per_node: dict[str, int] = {}
    errors: list[str] = []
    all_shards: list[dict] = []

    for node in endpoints:
        endpoint = node["endpoint"]

        try:
            result = get_shards_from_node(
                endpoint,
                dataset_id,
            )

            rpc_seconds_by_node[endpoint] = (
                result["rpc_seconds"]
            )

            shards_per_node[endpoint] = len(
                result["shards"]
            )

            all_shards.extend(result["shards"])

        except grpc.RpcError as exc:
            errors.append(
                f"{endpoint}: {exc.code().name}: "
                f"{exc.details() or 'no details'}"
            )

        except Exception as exc:
            errors.append(
                f"{endpoint}: {type(exc).__name__}: {exc}"
            )

    # Do not silently benchmark incomplete retrievals.
    if errors:
        raise RuntimeError(
            "One or more storage nodes failed:\n"
            + "\n".join(errors)
        )

    if not all_shards:
        raise RuntimeError(
            f"No shards returned for dataset {dataset_id!r}."
        )

    # ---------------- Reconstruct without ordering ----------------

    reconstruction_started = time.perf_counter()

    combined = b"".join(
        shard["data"]
        for shard in all_shards
    )

    reconstruction_seconds = (
        time.perf_counter() - reconstruction_started
    )

    elapsed_seconds = time.perf_counter() - started

    return {
        "bytes": combined,
        "endpoints": endpoints,
        "shards": all_shards,
        "elapsed_seconds": elapsed_seconds,
        "discovery_seconds": discovery_seconds,
        "rpc_seconds_by_node": rpc_seconds_by_node,
        "shards_per_node": shards_per_node,
        "reconstruction_seconds": reconstruction_seconds,
        "ordering_method": "none; arrival order",
        "rpc_payload_bytes": sum(
            len(shard["data"])
            for shard in all_shards
        ),
    }


# ============================================================
# CSV parsing measurements
# ============================================================

def parse_csv_metrics(data: bytes) -> dict:
    started = time.perf_counter()

    text = data.decode("utf-8-sig")

    # newline="" preserves normal CSV newline handling.
    from io import StringIO

    reader = csv.reader(StringIO(text, newline=""))
    rows = list(reader)

    parse_seconds = time.perf_counter() - started

    headers = rows[0] if rows else []

    return {
        "parse_seconds": parse_seconds,
        "row_count": max(0, len(rows) - 1),
        "column_count": len(headers),
        "headers": headers,
        "csv_record_count_including_header": len(rows),
    }


# ============================================================
# Local-file baseline and integrity
# ============================================================

def measure_local_file(
    path: Path,
    runs: int,
) -> dict:
    times = []
    last_data = b""

    for _ in range(runs):
        started = time.perf_counter()
        last_data = path.read_bytes()
        times.append(time.perf_counter() - started)

    median_time = statistics.median(times)
    size_mib = len(last_data) / (1024 * 1024)

    return {
        "path": str(path.resolve()),
        "size_bytes": len(last_data),
        "sha256": sha256_bytes(last_data),
        "times": times,
        "mean_seconds": statistics.mean(times),
        "median_seconds": median_time,
        "p95_seconds": percentile(times, 0.95),
        "throughput_mib_s": (
            size_mib / median_time
            if median_time > 0
            else float("inf")
        ),
    }


# ============================================================
# Dashboard metadata and distribution
# ============================================================

def find_dataset_dashboard_row(
    dashboard: dict,
    dataset_id: str,
) -> dict | None:
    for item in dashboard.get("datasets", []):
        if item.get("dataset_id") == dataset_id:
            return item

    return None


def distribution_metrics(shards: list[dict]) -> dict:
    counts = Counter(
        shard["node_endpoint"]
        for shard in shards
    )

    bytes_per_node = Counter()

    for shard in shards:
        bytes_per_node[shard["node_endpoint"]] += len(
            shard["data"]
        )

    def coefficient_of_variation(
        values: list[float],
    ) -> float | None:
        if not values:
            return None

        mean = statistics.mean(values)

        if mean == 0:
            return None

        return (
            statistics.pstdev(values) / mean
        ) * 100.0

    return {
        "shards_per_node": dict(counts),
        "bytes_per_node": dict(bytes_per_node),
        "shard_count_cv_percent": coefficient_of_variation(
            [float(value) for value in counts.values()]
        ),
        "bytes_cv_percent": coefficient_of_variation(
            [float(value) for value in bytes_per_node.values()]
        ),
        "nodes_holding_shards": len(counts),
    }


# ============================================================
# Report writer
# ============================================================

def write_report(
    path: Path,
    sections: list[tuple[str, str]],
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with path.open("w", encoding="utf-8") as file:
        file.write("CLOUDSHARD BENCHMARK REPORT\n")
        file.write("=" * 78 + "\n")
        file.write(f"Generated: {utc_now()}\n")
        file.write(f"Host: {platform.node()}\n")
        file.write(f"Platform: {platform.platform()}\n")
        file.write(f"Python: {sys.version.split()[0]}\n")
        file.write("=" * 78 + "\n\n")

        for title, body in sections:
            file.write(title.upper() + "\n")
            file.write(
                "-" * min(78, max(12, len(title))) + "\n"
            )
            file.write(body.rstrip() + "\n\n")

        file.write("INTERPRETATION / LIMITATIONS\n")
        file.write("-" * 28 + "\n")
        file.write(
            "* Results describe this machine, dataset, network, "
            "and test configuration.\n"
            "* listShards transfers complete shard payloads; this is "
            "a full-retrieval benchmark.\n"
            "* A local disk read is not a fair network baseline against "
            "remote retrieval.\n"
            "* The script does not automatically test concurrent clients, "
            "node failures, or alternative shard sizes.\n"
            "* Throughput uses MiB (1024 * 1024 bytes).\n"
            "* Process peak RSS is a process-wide peak, not per-node RAM.\n"
            "* Per-node requests are sequential in this version; their "
            "timings should not be interpreted as parallel retrieval.\n"
            "* Repeated runs may benefit from OS and storage caches.\n"
            "* Successful RPCs alone do not prove that every expected "
            "shard exists; compare against expected metadata or an "
            "original-file hash.\n"
        )


# ============================================================
# Benchmark entry point
# ============================================================

def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark CloudShard retrieval and save report.txt."
        )
    )

    parser.add_argument(
        "--dataset-id",
        required=True,
        help="Dataset ID, e.g. ds_ab12cd34ef56",
    )

    parser.add_argument(
        "--flask-url",
        default=DEFAULT_FLASK_URL,
        help="Flask base URL; defaults to FLASK_SERVER or localhost:5000",
    )

    parser.add_argument(
        "--runs",
        type=int,
        default=3,
        help="Number of retrieval trials (default: 3)",
    )

    parser.add_argument(
        "--local-file",
        type=Path,
        default=None,
        help="Optional original CSV for local read baseline and hash check",
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=Path("report.txt"),
        help="Report destination (default: report.txt)",
    )

    args = parser.parse_args()

    if args.runs < 1:
        parser.error("--runs must be at least 1")

    sections: list[tuple[str, str]] = []
    errors: list[str] = []

    # ---------------- Initial connectivity and dashboard ----------------

    try:
        dashboard = get_dashboard(args.flask_url)
        online_nodes = get_online_nodes(args.flask_url)

        sections.append((
            "Configuration",
            "\n".join([
                f"Dataset ID: {args.dataset_id}",
                f"Flask URL: {args.flask_url}",
                f"Requested retrieval trials: {args.runs}",
                f"Online nodes reported by Flask: {len(online_nodes)}",
                "Online endpoints: "
                + (
                    ", ".join(
                        node["endpoint"]
                        for node in online_nodes
                    )
                    or "none"
                ),
                f"RPC timeout: {RPC_TIMEOUT} seconds",
                f"Channel readiness timeout: "
                f"{CHANNEL_READY_TIMEOUT} seconds",
            ]),
        ))

        stats = dashboard.get("stats", {})
        dataset_row = find_dataset_dashboard_row(
            dashboard,
            args.dataset_id,
        )

        sections.append((
            "Dashboard snapshot",
            "\n".join([
                f"Global stats: {safe_json(stats)}",
                (
                    "Dataset row: "
                    + safe_json(dataset_row)
                    if dataset_row
                    else "Dataset row: not found in dashboard response"
                ),
                f"Dashboard node records: "
                f"{len(dashboard.get('nodes', []))}",
                f"Recent jobs visible: "
                f"{len(dashboard.get('jobs', []))}",
                f"Recent activity entries visible: "
                f"{len(dashboard.get('activity', []))}",
            ]),
        ))

    except Exception as exc:
        write_report(
            args.output,
            [(
                "Fatal setup error",
                f"{type(exc).__name__}: {exc}",
            )],
        )

        print(
            f"Setup failed; details written to {args.output.resolve()}",
            file=sys.stderr,
        )

        return 2

    # ---------------- Repeated retrieval trials ----------------

    trial_times: list[float] = []
    trial_bytes: list[int] = []
    trial_parse_times: list[float] = []
    trial_reconstruction_times: list[float] = []

    last_result = None
    last_csv = None

    for run_number in range(1, args.runs + 1):
        try:
            result = retrieve_cloudshard(
                args.flask_url,
                args.dataset_id,
            )

            csv_metrics = parse_csv_metrics(result["bytes"])

            trial_times.append(result["elapsed_seconds"])
            trial_bytes.append(len(result["bytes"]))
            trial_parse_times.append(
                csv_metrics["parse_seconds"]
            )
            trial_reconstruction_times.append(
                result["reconstruction_seconds"]
            )

            last_result = result
            last_csv = csv_metrics

            print(
                f"Run {run_number}/{args.runs}: "
                f"{result['elapsed_seconds']:.3f}s, "
                f"{len(result['bytes']) / (1024 * 1024):.2f} MiB, "
                f"{len(result['shards'])} shards"
            )

        except Exception as exc:
            message = (
                f"Trial {run_number}: "
                f"{type(exc).__name__}: {exc}"
            )

            errors.append(message)

            print(
                f"Run {run_number}/{args.runs} FAILED: {exc}",
                file=sys.stderr,
            )

    if last_result is None:
        sections.append((
            "CloudShard retrieval",
            "All retrieval trials failed.\n"
            + "\n".join(errors),
        ))

        write_report(args.output, sections)

        print(
            f"No successful retrieval trial; report written to "
            f"{args.output.resolve()}",
            file=sys.stderr,
        )

        return 3

    # ---------------- Aggregate performance ----------------

    data = last_result["bytes"]
    size_bytes = len(data)
    size_mib = size_bytes / (1024 * 1024)

    median_time = statistics.median(trial_times)
    mean_time = statistics.mean(trial_times)
    p95_time = percentile(trial_times, 0.95)

    throughput = (
        size_mib / median_time
        if median_time > 0
        else float("inf")
    )

    amplification = (
        last_result["rpc_payload_bytes"] / size_bytes
        if size_bytes
        else float("nan")
    )

    retrieval_lines = [
        f"Successful trials: {len(trial_times)} / {args.runs}",
        f"Failed trials: {args.runs - len(trial_times)}",
        f"Dataset reconstructed bytes: {size_bytes}",
        f"Dataset reconstructed MiB: {size_mib:.6f}",
        f"Shard count: {len(last_result['shards'])}",
        f"Shard ordering: {last_result['ordering_method']}",
        f"Reconstructed SHA-256: {sha256_bytes(data)}",
        f"Retrieval latency mean: {fmt_seconds(mean_time)}",
        f"Retrieval latency median (p50): "
        f"{fmt_seconds(median_time)}",
        f"Retrieval latency p95: {fmt_seconds(p95_time)}",
        f"Retrieval latency min/max: "
        f"{fmt_seconds(min(trial_times))} / "
        f"{fmt_seconds(max(trial_times))}",
        f"Effective throughput at median latency: "
        f"{fmt_rate(throughput)}",
        f"Shard payload bytes received (last run): "
        f"{last_result['rpc_payload_bytes']}",
        f"Retrieval amplification: {amplification:.6f}x",
        f"Node discovery/API time (last run): "
        f"{fmt_seconds(last_result['discovery_seconds'])}",
        f"Shard concatenation time (last run): "
        f"{fmt_seconds(last_result['reconstruction_seconds'])}",
        "",
        "Per-node RPC latency (last run):",
    ]

    for endpoint, seconds in (
        last_result["rpc_seconds_by_node"].items()
    ):
        retrieval_lines.append(
            f"  {endpoint}: {fmt_seconds(seconds)}"
        )

    retrieval_lines.extend([
        "",
        "Shards returned per node (last run):",
    ])

    for endpoint, count in last_result["shards_per_node"].items():
        retrieval_lines.append(
            f"  {endpoint}: {count}"
        )

    if errors:
        retrieval_lines.extend([
            "",
            "Trial errors:",
            *errors,
        ])

    sections.append((
        "CloudShard retrieval performance",
        "\n".join(retrieval_lines),
    ))

    # ---------------- CSV parsing ----------------

    sections.append((
        "CSV parsing",
        "\n".join([
            f"Last-run CSV parse time: "
            f"{fmt_seconds(last_csv['parse_seconds'])}",
            f"Mean CSV parse time across successful trials: "
            f"{fmt_seconds(statistics.mean(trial_parse_times))}",
            f"Median CSV parse time: "
            f"{fmt_seconds(statistics.median(trial_parse_times))}",
            f"Data rows excluding header: {last_csv['row_count']}",
            f"Columns: {last_csv['column_count']}",
            f"Headers: {safe_json(last_csv['headers'])}",
            f"CSV records including header: "
            f"{last_csv['csv_record_count_including_header']}",
        ]),
    ))

    # ---------------- Shard distribution ----------------

    dist = distribution_metrics(
        last_result["shards"]
    )

    sections.append((
        "Shard distribution and balance",
        "\n".join([
            f"Nodes actually returning shards: "
            f"{dist['nodes_holding_shards']}",
            f"Shards per node: "
            f"{safe_json(dist['shards_per_node'])}",
            f"Payload bytes per node: "
            f"{safe_json(dist['bytes_per_node'])}",
            (
                "Shard-count coefficient of variation: "
                f"{dist['shard_count_cv_percent']:.3f}%"
                if dist["shard_count_cv_percent"] is not None
                else "Shard-count coefficient of variation: n/a"
            ),
            (
                "Byte-volume coefficient of variation: "
                f"{dist['bytes_cv_percent']:.3f}%"
                if dist["bytes_cv_percent"] is not None
                else "Byte-volume coefficient of variation: n/a"
            ),
        ]),
    ))

    # ---------------- Original-file baseline and integrity ----------------

    local_report = (
        "No --local-file supplied; local disk baseline and "
        "original hash comparison skipped."
    )

    if args.local_file:
        if not args.local_file.is_file():
            local_report = (
                f"Local file not found: {args.local_file}"
            )

        else:
            try:
                local = measure_local_file(
                    args.local_file,
                    args.runs,
                )

                reconstructed_hash = sha256_bytes(data)
                same = local["sha256"] == reconstructed_hash

                local_report = "\n".join([
                    f"Local file: {local['path']}",
                    f"Local file bytes: {local['size_bytes']}",
                    f"Local read mean: "
                    f"{fmt_seconds(local['mean_seconds'])}",
                    f"Local read median: "
                    f"{fmt_seconds(local['median_seconds'])}",
                    f"Local read p95: "
                    f"{fmt_seconds(local['p95_seconds'])}",
                    f"Local disk read throughput: "
                    f"{fmt_rate(local['throughput_mib_s'])}",
                    f"Original SHA-256: {local['sha256']}",
                    f"Reconstructed SHA-256: {reconstructed_hash}",
                    f"Byte-for-byte identical: "
                    f"{'YES' if same else 'NO'}",
                    "Note: local disk read throughput is not a "
                    "like-for-like remote network baseline.",
                ])

            except Exception as exc:
                local_report = (
                    f"Local baseline failed: "
                    f"{type(exc).__name__}: {exc}"
                )

    sections.append((
        "Local-file baseline and integrity",
        local_report,
    ))

    # ---------------- Client resource usage ----------------

    rss = get_peak_rss_mb()
    user_cpu, system_cpu = get_cpu_usage()

    sections.append((
        "Client resource usage",
        "\n".join([
            (
                f"Process peak RSS: {rss:.3f} MiB"
                if rss is not None
                else "Process peak RSS: unavailable"
            ),
            (
                f"User CPU time: {user_cpu:.3f} seconds"
                if user_cpu is not None
                else "User CPU time: unavailable"
            ),
            (
                f"System CPU time: {system_cpu:.3f} seconds"
                if system_cpu is not None
                else "System CPU time: unavailable"
            ),
        ]),
    ))

    # ---------------- Save final report ----------------

    write_report(args.output, sections)

    print(f"\nReport saved to: {args.output.resolve()}")
    print(
        f"Successful retrieval trials: "
        f"{len(trial_times)}/{args.runs}"
    )
    print(
        f"Median retrieval latency: {median_time:.3f}s"
    )
    print(
        f"Effective throughput: {throughput:.3f} MiB/s"
    )
    print(
        f"Reconstructed SHA-256: {sha256_bytes(data)}"
    )

    # A report is produced even if some trials fail.
    return 0 if len(trial_times) == args.runs else 4


if __name__ == "__main__":
    raise SystemExit(main())
