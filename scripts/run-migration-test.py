#!/usr/bin/env python3
"""Run concurrent migrations and emit one compact report, or summarize an existing run."""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import json
import math
import os
import pathlib
import re
import shlex
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections import defaultdict

DEFAULT_OUTPUT_ROOT = pathlib.Path.cwd() / "artifacts" / "migration-runs"


class TestFailure(RuntimeError):
    """A user-facing test failure."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--concurrency", type=int, help="number of simultaneous migrations; prompted if omitted"
    )
    parser.add_argument("--image", help="complete OCI image reference; prompted if omitted")
    parser.add_argument(
        "--summarize",
        type=pathlib.Path,
        metavar="RUN_DIR",
        help="only summarize saved logs; no network or sandbox operations",
    )
    parser.add_argument(
        "--output",
        type=pathlib.Path,
        help="compact report destination; default: <run_dir>/migration-summary.log",
    )
    parser.add_argument(
        "--output-root",
        type=pathlib.Path,
        default=pathlib.Path(env("AENV_BENCH_OUTPUT_ROOT", str(DEFAULT_OUTPUT_ROOT))),
    )
    parser.add_argument(
        "--source-runtime", default=env("AENV_BENCH_SOURCE_RUNTIME", "http://127.0.0.1:8000")
    )
    parser.add_argument("--gateway", default=env("AENV_BENCH_GATEWAY", "http://127.0.0.1:8081"))
    parser.add_argument("--target-node", default=env("AENV_BENCH_TARGET_NODE", "node-b"))
    parser.add_argument(
        "--gateway-ssh", default=env("AENV_BENCH_GATEWAY_SSH", "local"), metavar="USER@HOST"
    )
    parser.add_argument(
        "--node-a-ssh",
        "--source-ssh",
        dest="source_ssh",
        default=env("AENV_BENCH_SOURCE_SSH", "local"),
        metavar="USER@HOST",
    )
    parser.add_argument(
        "--node-b-ssh",
        "--target-ssh",
        dest="target_ssh",
        default=env("AENV_BENCH_TARGET_SSH", "root@10.10.10.02"),
        metavar="USER@HOST",
    )
    parser.add_argument("--gateway-unit", default=env("AENV_BENCH_GATEWAY_UNIT", "aenv-gateway"))
    parser.add_argument("--runtime-unit", default=env("AENV_BENCH_RUNTIME_UNIT", "agentenv"))
    parser.add_argument(
        "--transport",
        choices=("urma", "iroh", "composite", "disabled", "unknown"),
        default=os.environ.get("AENV_BENCH_TRANSPORT"),
    )
    return parser.parse_args()


def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def run_command(command: list[str], *, timeout: int = 30) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def local_command(command: list[str]) -> list[str]:
    if os.geteuid() == 0:
        return command
    return ["sudo", "-n", *command]


def host_command(target_ssh: str | None, command: list[str]) -> list[str]:
    if target_ssh in (None, "local"):
        return local_command(command)
    return [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=10",
        "--",
        target_ssh,
        shlex.join(command),
    ]


def ensure_service_active(target_ssh: str | None, unit: str, label: str) -> None:
    result = run_command(host_command(target_ssh, ["systemctl", "is-active", unit]))
    if result.returncode != 0 or result.stdout.strip() != "active":
        detail = result.stderr.strip() or result.stdout.strip() or "unknown status"
        raise TestFailure(f"{label} service {unit!r} is not active: {detail}")


def journal_cursor(target_ssh: str | None, unit: str, label: str) -> str:
    result = run_command(
        host_command(
            target_ssh,
            ["journalctl", "-u", unit, "-n", "1", "--show-cursor", "--no-pager", "-o", "cat"],
        )
    )
    if result.returncode != 0:
        raise TestFailure(f"cannot read {label} journal cursor: {result.stderr.strip()}")
    for line in reversed(result.stdout.splitlines()):
        if line.startswith("-- cursor: "):
            return line.removeprefix("-- cursor: ").strip()
    raise TestFailure(f"cannot find a journald cursor for {label}")


def collect_journal(
    target_ssh: str | None,
    unit: str,
    cursor: str,
    destination: pathlib.Path,
) -> str | None:
    # Journals may be large: stream directly to disk instead of buffering stdout.
    command = host_command(
        target_ssh,
        ["journalctl", "-u", unit, "--after-cursor", cursor, "--no-pager", "--all", "-o", "json"],
    )
    with destination.open("w", encoding="utf-8") as stream:
        result = subprocess.run(
            command, stdout=stream, stderr=subprocess.PIPE, text=True, check=False, timeout=60
        )
    if result.returncode != 0:
        return result.stderr.strip() or f"journalctl exited with {result.returncode}"
    return None


def http_request(
    method: str,
    url: str,
    api_key: str,
    *,
    body: dict | None = None,
    timeout: int,
) -> dict:
    encoded = None if body is None else json.dumps(body).encode()
    headers = {"X-API-Key": api_key}
    if encoded is not None:
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=encoded, method=method, headers=headers)
    # These are control-plane API calls; environment proxies must not intercept them.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    started_ns = time.monotonic_ns()
    try:
        with opener.open(request, timeout=timeout) as response:
            response_body = response.read().decode(errors="replace")
            status = response.status
    except urllib.error.HTTPError as error:
        response_body = error.read().decode(errors="replace")
        status = error.code
    except Exception as error:
        response_body = str(error)
        status = 0
    finished_ns = time.monotonic_ns()
    return {
        "http_status": status,
        "ok": 200 <= status < 300,
        "duration_ms": round((finished_ns - started_ns) / 1_000_000, 3),
        "response": response_body,
    }


def create_sandbox(source_runtime: str, api_key: str, image: str) -> dict:
    result = http_request(
        "POST",
        f"{source_runtime.rstrip('/')}/sandboxes-cold",
        api_key,
        body={
            "image": image,
            "timeout": 3600,
            "cpuCount": 1,
            "memoryMB": 512,
            "diskSizeMB": 65536,
            "secure": False,
        },
        timeout=600,
    )
    if result["ok"]:
        try:
            payload = json.loads(result["response"])
            result["sandbox_id"] = payload["sandboxID"]
            result["template_id"] = payload.get("templateID", "")
            del result["response"]
        except (json.JSONDecodeError, KeyError, TypeError) as error:
            result["ok"] = False
            result["parse_error"] = f"invalid create response: {error}"
    return result


def wait_for_gateway_bindings(
    gateway: str,
    api_key: str,
    sandbox_ids: list[str],
    timeout: int = 90,
) -> None:
    pending = set(sandbox_ids)
    deadline = time.monotonic() + timeout
    last_status: dict[str, int] = {}
    while pending and time.monotonic() < deadline:
        for sandbox_id in list(pending):
            result = http_request(
                "GET",
                f"{gateway.rstrip('/')}/sandboxes/{sandbox_id}",
                api_key,
                timeout=10,
            )
            last_status[sandbox_id] = result["http_status"]
            if result["ok"]:
                pending.remove(sandbox_id)
        if pending:
            time.sleep(2)
    if pending:
        details = ", ".join(
            f"{sandbox_id}=HTTP {last_status.get(sandbox_id, 0)}" for sandbox_id in pending
        )
        raise TestFailure(
            f"Gateway did not resolve all source sandboxes within {timeout}s: {details}"
        )


def migrate_sandbox(
    gateway: str,
    api_key: str,
    target_node: str,
    sandbox_id: str,
) -> dict:
    result = http_request(
        "POST",
        f"{gateway.rstrip('/')}/sandboxes/{sandbox_id}/migrate",
        api_key,
        body={"targetNodeID": target_node},
        timeout=360,
    )
    result["sandbox_id"] = sandbox_id
    return result


def write_run_info(path: pathlib.Path, run_info: dict) -> None:
    path.write_text(json.dumps(run_info, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def make_run_directory(output_root: pathlib.Path, concurrency: int) -> pathlib.Path:
    stamp = dt.datetime.now().astimezone().strftime("%Y%m%d-%H%M%S")
    candidate = output_root / f"{stamp}-c{concurrency}"
    suffix = 1
    while candidate.exists():
        candidate = output_root / f"{stamp}-c{concurrency}-{suffix}"
        suffix += 1
    (candidate / "logs").mkdir(parents=True)
    return candidate


# Both the live benchmark and --summarize use the same parser/report writer.
MARKER = "AENV_MIGRATION_STAGE "
ANSI = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
FIELD = re.compile(r'\b([a-z_]+)=("(?:\\.|[^"\\])*"|[^\s{}]+)')
SNAPSHOT_KEY = re.compile(r"^snapshot/v1/artifacts/([^/]+)/")
BREAKDOWN = {
    "connect_ms": "read_connect",
    "import_ms": "read_import_segment",
    "post_ms": "read_post",
    "completion_wait_ms": "read_completion_wait",
    "reads_ms": "read_loop",
}


def decode_line(line: str) -> tuple[str, dict]:
    """Accept journal JSON, tracing JSON/Bunyan, and old plain/ANSI journal exports."""
    line = ANSI.sub("", line).strip()
    fields: dict = {}
    timestamp = re.search(r"\d{4}-\d{2}-\d{2}T[0-9:.]+(?:Z|[+-]\d{2}:?\d{2})?", line)
    if timestamp:
        fields["timestamp"] = timestamp[0]
    # A JSON journal record can itself contain a JSON tracing MESSAGE.
    for _ in range(2):
        start = line.find("{")
        if start < 0:
            break
        try:
            record = json.loads(line[start:])
        except (ValueError, TypeError):
            break
        if not isinstance(record, dict):
            break
        if "MESSAGE" in record:
            fields["timestamp_unix_ms"] = int(record.get("__REALTIME_TIMESTAMP", 0)) / 1000
            message = record["MESSAGE"]
            if isinstance(message, list):
                # journalctl JSON encodes non-printable MESSAGE bytes (e.g. ANSI) as integers.
                message = bytes(message).decode("utf-8", errors="replace")
                fields["_journal_message_bytes"] = True
            line = ANSI.sub("", str(message)).strip()
            continue
        for span in [*record.get("spans", []), record.get("span", {})]:
            if isinstance(span, dict):
                fields.update(span)
        fields.update(record)
        if isinstance(record.get("fields"), dict):
            fields.update(record["fields"])
        line = ANSI.sub("", str(fields.get("message", fields.get("msg", line)))).strip()
        break
    for name, value in FIELD.findall(line):
        if value.startswith('"'):
            value = json.loads(value)
        fields[name] = value
    return line, fields


def read_events(log_paths: list[pathlib.Path]) -> tuple[list[dict], list[str], dict]:
    events: list[dict] = []
    warnings: list[str] = []
    counts = {
        "lines": 0,
        "parse_errors": 0,
        "warning_lines": 0,
        "transports": set(),
        "nodes": defaultdict(lambda: {"lines": 0, "byte_messages": 0}),
    }
    for path in log_paths:
        node = (
            path.stem.lower()
            .replace("_", "-")
            .replace("nodea", "node-a")
            .replace("nodeb", "node-b")
        )
        coverage = counts["nodes"][node]  # Include empty journals in coverage diagnostics.
        with path.open(encoding="utf-8", errors="replace") as stream:
            for number, raw in enumerate(stream, 1):
                counts["lines"] += 1
                coverage["lines"] += 1
                try:
                    message, fields = decode_line(raw)
                    coverage["byte_messages"] += bool(fields.get("_journal_message_bytes"))
                    lower = (message + " " + str(fields.get("target", ""))).lower()
                    for backend in ("urma", "iroh"):
                        if (
                            f"{backend} artifact transport started" in lower
                            or f"agentenv::p2p::{backend}" in lower
                            or f'backend="{backend}"' in lower
                            or fields.get("backend") == backend
                        ):
                            counts["transports"].add(backend)
                    if MARKER in message:
                        event = json.JSONDecoder().raw_decode(message.split(MARKER, 1)[1].strip())[
                            0
                        ]
                        if (
                            not isinstance(event, dict)
                            or event.get("schema") != "agentenv.migration-stage.v1"
                        ):
                            continue
                        event = {**event, "node": node, "kind": "migration"}
                        if not isinstance(event.get("stage"), str):
                            raise ValueError("missing stage")
                        duration = float(event["duration_ms"])
                        if not math.isfinite(duration) or duration < 0:
                            raise ValueError("invalid duration_ms")
                        events.append(event)
                        continue
                    if "snapshot P2P artifact publication started" in message:
                        events.append({**fields, "node": node, "kind": "mapping"})
                        continue
                    if "urma read window started" in message:
                        events.append({**fields, "node": node, "kind": "window"})
                        continue
                    if "urma stage timing" in message or "urma read range timing" in message:
                        durations = (
                            BREAKDOWN
                            if "urma read range timing" in message
                            else {"duration_ms": fields["stage"]}
                        )
                        for field, stage in durations.items():
                            value = float(fields[field])
                            if not math.isfinite(value) or value < 0:
                                raise ValueError(f"invalid {field}")
                            events.append(
                                {
                                    **fields,
                                    "node": node,
                                    "kind": "urma",
                                    "stage": stage,
                                    "duration_ms": value,
                                    "status": fields.get("status", "ok"),
                                }
                            )
                        continue
                    # Decode before checking levels: gateway JSON uses lowercase warn/error.
                    level = fields.get("level", "")
                    if (
                        str(level).upper() in ("WARN", "WARNING", "ERROR", "FATAL")
                        or (isinstance(level, (int, float)) and level >= 40)
                        or re.search(
                            r"\b(WARN|ERROR|WARNING)\b|catalog attempt failed|Suppressed \d+ messages",
                            message,
                            re.IGNORECASE,
                        )
                    ):
                        counts["warning_lines"] += 1
                        if len(warnings) < 30:
                            details = {
                                key: fields[key]
                                for key in (
                                    "error",
                                    "err",
                                    "cause",
                                    "sandbox_id",
                                    "snapshot_id",
                                    "target_node",
                                    "attempt",
                                    "peer",
                                )
                                if key in fields
                            }
                            evidence = message + (
                                " " + json.dumps(details, ensure_ascii=False) if details else ""
                            )
                            warnings.append(f"{node}:{number}: {evidence[:700]}")
                except (ValueError, TypeError, KeyError) as error:
                    counts["parse_errors"] += 1
                    if len(warnings) < 30:
                        warnings.append(f"{node}:{number}: parse error: {error}")
    return events, warnings, counts


def expected_sandbox_ids(run_info: dict) -> set[str]:
    return {
        item["sandbox_id"]
        for group in ("creations", "migrations")
        for item in run_info.get(group, [])
        if item.get("sandbox_id")
    }


def correlate_events(events: list[dict], run_info: dict) -> tuple[list[dict], list[dict], int]:
    allowed = expected_sandbox_ids(run_info)
    migration_events = [event for event in events if event["kind"] == "migration"]
    snapshots = {
        event["snapshot_id"]
        for event in migration_events
        if event.get("snapshot_id") and (not allowed or event.get("sandbox_id") in allowed)
    }
    migrations = {
        event["migration_id"]
        for event in migration_events
        if event.get("migration_id") and (not allowed or event.get("sandbox_id") in allowed)
    }
    # Client responses still identify the snapshot if some gateway logs are missing.
    for result in run_info.get("migrations", []):
        try:
            response = result.get("response", {})
            response = json.loads(response) if isinstance(response, str) else response
            if isinstance(response, dict) and response.get("snapshotID"):
                snapshots.add(response["snapshotID"])
        except ValueError:
            pass
    for event in migration_events:
        if event.get("migration_id") in migrations and event.get("snapshot_id"):
            snapshots.add(event["snapshot_id"])
    pipeline = [
        event
        for event in migration_events
        if not allowed
        or event.get("sandbox_id") in allowed
        or event.get("snapshot_id") in snapshots
        or event.get("migration_id") in migrations
    ]
    by_key: dict[str, set[str]] = defaultdict(set)
    by_va: dict[tuple[str, str], set[str]] = defaultdict(set)
    for event in events:
        key = event.get("key", "")
        if event["kind"] == "mapping" and key and event.get("snapshot_id") in snapshots:
            by_key[key].add(event["snapshot_id"])
        if event["kind"] == "window" and key and "remote_va" in event:
            by_va[(event["node"], str(event["remote_va"]))].add(key)
    timings = []
    unlinked = 0
    for event in events:
        if event["kind"] != "urma":
            continue
        key = event.get("key", "")
        if not key and "remote_va" in event:
            candidates = by_va[(event["node"], str(event["remote_va"]))]
            if len(candidates) == 1:
                key = next(iter(candidates))
        matched = SNAPSHOT_KEY.match(key)
        references = by_key.get(key, set()) | (
            {matched[1]} if matched and matched[1] in snapshots else set()
        )
        if not references:
            unlinked += 1
            continue
        timings.append({**event, "key": key, "snapshot_ids": sorted(references)})
    return pipeline, timings, unlinked


def stage_statistics(events: list[dict]) -> list[dict]:
    grouped: dict[tuple, list[dict]] = defaultdict(list)
    for event in events:
        grouped[
            (
                event["node"],
                event["stage"],
                event.get("source") or "-",
                event.get("status") or "unknown",
            )
        ].append(event)
    rows = []
    for (node, stage, source, status), samples in sorted(grouped.items()):
        values = sorted(float(event["duration_ms"]) for event in samples)
        rows.append(
            {
                "node": node,
                "stage": stage,
                "source": source,
                "status": status,
                "n": len(values),
                "mean": statistics.fmean(values),
                "p50": statistics.median(values),
                "p95": values[max(0, math.ceil(len(values) * 0.95) - 1)],
                "min": values[0],
                "max": values[-1],
                "slowest": sorted(
                    samples, key=lambda event: float(event["duration_ms"]), reverse=True
                )[:2],
            }
        )
    return rows


def summarize_run(
    run_dir: pathlib.Path, output: pathlib.Path | None = None, transport: str | None = None
) -> pathlib.Path:
    run_info = json.loads((run_dir / "run-info.json").read_text(encoding="utf-8"))
    output = output or run_dir / "migration-summary.log"
    if output.resolve() == (run_dir / "run-info.json").resolve():
        raise TestFailure("report output must not overwrite run-info.json")
    log_dir = next(
        (run_dir / name for name in ("logs", "log") if (run_dir / name).is_dir()), run_dir
    )
    logs = sorted(
        path
        for path in log_dir.glob("*.log")
        if path.name != "migration-summary.log" and path.resolve() != output.resolve()
    )
    if not logs:
        raise TestFailure(f"no journal logs found in {log_dir}")
    events, warnings, counts = read_events(logs)
    pipeline, timings, unlinked = correlate_events(events, run_info)
    detected = next(iter(counts["transports"])) if len(counts["transports"]) == 1 else "unknown"
    transport = (
        transport
        or run_info.get("transport")
        or ("urma" if any(e["kind"] == "urma" for e in events) else detected)
    )
    lines = [
        "AgentENV migration report v2 (milliseconds)",
        f"run={run_dir.name} transport={transport}",
    ]
    for field in (
        "status",
        "started_at",
        "finished_at",
        "image",
        "concurrency",
        "source_runtime",
        "gateway",
        "target_node",
        "sandbox_resources",
        "creation_batch_duration_ms",
        "migration_batch_duration_ms",
        "journal_scope",
        "hosts",
        "errors",
    ):
        if field in run_info:
            lines.append(f"{field}={json.dumps(run_info[field], ensure_ascii=False)}")
    lines += [
        "",
        "NOTES: totals include sub-stages; concurrent durations must not be added.",
        "Wait times include executor scheduling. URMA samples are operations, not migrations.",
        "Shared layer keys can reference multiple snapshots; global statistics count each timing once.",
        "P2P/OSS fetch observations cover logged artifacts, not all lazy disk/memory reads.",
    ]

    def table(title: str, rows: list[dict]) -> None:
        lines.extend(["", title, "node stage source status n mean p50 p95 min max"])
        for row in rows:
            lines.append(
                "{node} {stage} {source} {status} {n} {mean:.3f} {p50:.3f} {p95:.3f} {min:.3f} {max:.3f}".format(
                    **row
                )
            )

    table("MIGRATION STAGES", stage_statistics(pipeline))
    lines += [
        "",
        "PER SANDBOX (multiple values are preserved for retries)",
        "Stage entries are node/stage/source/status=duration_ms; client_ms is the HTTP request duration.",
    ]
    results = {result["sandbox_id"]: result for result in run_info.get("migrations", [])}
    for sandbox_id in sorted(expected_sandbox_ids(run_info)):
        result = results.get(sandbox_id, {"response": "migration result not recorded"})
        related_ids = {
            e["migration_id"]
            for e in pipeline
            if e.get("sandbox_id") == sandbox_id and e.get("migration_id")
        }
        related_snapshots = {
            e["snapshot_id"]
            for e in pipeline
            if e.get("sandbox_id") == sandbox_id and e.get("snapshot_id")
        }
        for e in pipeline:
            if e.get("migration_id") in related_ids and e.get("snapshot_id"):
                related_snapshots.add(e["snapshot_id"])
        try:
            response = result.get("response", {})
            response = json.loads(response) if isinstance(response, str) else response
            if isinstance(response, dict) and response.get("snapshotID"):
                related_snapshots.add(response["snapshotID"])
        except ValueError:
            pass
        lines.append(
            f"sandbox={sandbox_id} snapshots={','.join(sorted(related_snapshots)) or '-'} migration_ids={','.join(sorted(related_ids)) or '-'} "
            f"HTTP={result.get('http_status', 'not-recorded')} ok={result.get('ok', False)} client_ms={result.get('duration_ms', '-')}"
        )
        related = [
            e
            for e in pipeline
            if e.get("sandbox_id") == sandbox_id
            or e.get("migration_id") in related_ids
            or e.get("snapshot_id") in related_snapshots
        ]
        lines.append(
            "  "
            + " ".join(
                f"{e['node']}/{e['stage']}/{e.get('source', '-')}/{e.get('status', '-')}={float(e['duration_ms']):.3f}"
                for e in related
            )
        )
        if not result.get("ok"):
            lines.append(f"  response={str(result.get('response', ''))[:700]}")
    urma_rows = stage_statistics(timings)
    table("URMA STAGES", urma_rows)
    lines += [
        "",
        "SLOWEST URMA SAMPLES (up to 2 per node/stage/status; shared keys are not assigned to one sandbox)",
    ]
    for row in urma_rows:
        for event in row["slowest"]:
            compact = {
                key: event[key]
                for key in (
                    "node",
                    "stage",
                    "status",
                    "duration_ms",
                    "key",
                    "snapshot_ids",
                    "timestamp_unix_ms",
                    "timestamp",
                    "bytes",
                    "offset",
                    "read_count",
                )
                if key in event
            }
            lines.append(json.dumps(compact, ensure_ascii=False, separators=(",", ":")))
    missing = sorted(
        expected_sandbox_ids(run_info)
        - {e.get("sandbox_id") for e in pipeline if e.get("stage") == "migration_total"}
    )
    lines += [
        "",
        "DIAGNOSTICS",
        f"input_lines={counts['lines']} migration_events={len(pipeline)} linked_urma_timings={len(timings)} unlinked_urma_timings={unlinked}",
        f"parse_errors={counts['parse_errors']} warning_lines={counts['warning_lines']} retained_warning_lines={len(warnings)}",
        f"missing_migration_total={','.join(missing) or 'none'}",
    ]
    for node, coverage in sorted(counts["nodes"].items()):
        node_pipeline = [event for event in pipeline if event["node"] == node]
        node_timings = [event for event in timings if event["node"] == node]
        lines.append(
            f"{node}: input_lines={coverage['lines']} byte_messages={coverage['byte_messages']} migration_events={len(node_pipeline)} linked_urma_timings={len(node_timings)}"
        )
        if node in ("node-a", "node-b") and not node_pipeline:
            lines.append(
                f"WARNING: {node} has no linked runtime stages; check journal unit, server version and RUST_LOG."
            )
    if transport == "urma" and not timings:
        lines.append(
            "WARNING: no linked URMA timings; check both server versions, INFO filters, and artifact mapping logs."
        )
    if not pipeline:
        lines.append("WARNING: no migration stage events; results are incomplete.")
    lines.extend(warnings)
    lines.append("raw_logs=" + ",".join(str(path.relative_to(run_dir)) for path in logs))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return output


def main() -> int:
    args = parse_args()
    if args.summarize:
        print(summarize_run(args.summarize.resolve(), args.output, args.transport))
        return 0
    if args.concurrency is None or not args.image:
        if not sys.stdin.isatty():
            raise TestFailure(
                "--concurrency and --image are required without an interactive terminal"
            )
        if args.concurrency is None:
            args.concurrency = int(input("并发数: "))
        if not args.image:
            args.image = input("完整镜像地址: ").strip()
    if args.concurrency < 1 or not args.image:
        raise TestFailure("concurrency must be >= 1 and image must not be empty")
    api_key = os.environ.get("AENV_API_KEY", "")
    if not api_key:
        raise TestFailure("AENV_API_KEY is required in the environment")
    hosts = {
        "gateway": (args.gateway_ssh, args.gateway_unit),
        "node-a": (args.source_ssh, args.runtime_unit),
        "node-b": (args.target_ssh, args.runtime_unit),
    }
    for target, _ in hosts.values():
        if target != "local" and (
            not target or target.startswith("-") or any(c.isspace() for c in target)
        ):
            raise TestFailure(f"invalid SSH target: {target!r}; use USER@HOST, SSH alias, or local")
    run_dir = make_run_directory(args.output_root, args.concurrency)
    run_info_path = run_dir / "run-info.json"
    run_info: dict = {
        "status": "running",
        "started_at": utc_now(),
        "image": args.image,
        "concurrency": args.concurrency,
        "source_runtime": args.source_runtime,
        "gateway": args.gateway,
        "target_node": args.target_node,
        "hosts": {label: {"ssh": target, "unit": unit} for label, (target, unit) in hosts.items()},
        "transport": args.transport,
        "sandbox_resources": {
            "cpu_count": 1,
            "memory_mb": 512,
            "disk_size_mb": 65536,
            "timeout_seconds": 3600,
            "secure": False,
        },
        "creations": [],
        "migrations": [],
        "errors": [],
        "journal_scope": "preparation",
    }
    write_run_info(run_info_path, run_info)
    cursors: dict[str, str] = {}
    exit_code = 0
    try:
        for label, (target, unit) in hosts.items():
            ensure_service_active(target, unit, label)
            cursors[label] = journal_cursor(target, unit, label)
        creation_started = time.monotonic()
        for index in range(args.concurrency):
            print(f"[create {index + 1}/{args.concurrency}] {args.image}", flush=True)
            creation = create_sandbox(args.source_runtime, api_key, args.image)
            creation["index"] = index + 1
            run_info["creations"].append(creation)
            write_run_info(run_info_path, run_info)
            if not creation["ok"]:
                raise TestFailure(f"sandbox creation failed: {creation}")
        run_info["creation_batch_duration_ms"] = round(
            (time.monotonic() - creation_started) * 1000, 3
        )
        sandbox_ids = [creation["sandbox_id"] for creation in run_info["creations"]]
        print("[prepare] waiting for Gateway/Scheduler bindings", flush=True)
        wait_for_gateway_bindings(args.gateway, api_key, sandbox_ids)
        # Exclude image creation and its URMA traffic from migration statistics.
        for label, (target, unit) in hosts.items():
            cursors[label] = journal_cursor(target, unit, label)
        run_info["journal_scope"] = "migration plus 1s drain"
        print(f"[migrate] submitting {len(sandbox_ids)} concurrent migrations", flush=True)
        migration_started = time.monotonic()
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as executor:
            futures = [
                executor.submit(
                    migrate_sandbox, args.gateway, api_key, args.target_node, sandbox_id
                )
                for sandbox_id in sandbox_ids
            ]
            for future in concurrent.futures.as_completed(futures):
                run_info["migrations"].append(future.result())
                write_run_info(run_info_path, run_info)
        run_info["migration_batch_duration_ms"] = round(
            (time.monotonic() - migration_started) * 1000, 3
        )
        run_info["migrations"].sort(key=lambda result: result["sandbox_id"])
        failed = [result for result in run_info["migrations"] if not result["ok"]]
        if failed:
            raise TestFailure(
                f"{len(failed)} of {len(sandbox_ids)} migrations failed; see per-sandbox results"
            )
        run_info["status"] = "succeeded"
    except KeyboardInterrupt:
        exit_code = 130
        run_info["status"] = "interrupted"
        run_info["errors"].append("interrupted by user")
    except (TestFailure, OSError, subprocess.SubprocessError) as error:
        exit_code = 1
        run_info["status"] = "failed"
        run_info["errors"].append(str(error))
        print(f"ERROR: {error}", file=sys.stderr)
    finally:
        time.sleep(1)
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            jobs = {}
            for label, (target, unit) in hosts.items():
                destination = run_dir / "logs" / f"{label}.log"
                if label in cursors:
                    jobs[
                        executor.submit(collect_journal, target, unit, cursors[label], destination)
                    ] = label
                else:
                    destination.touch()
            for future in concurrent.futures.as_completed(jobs):
                try:
                    error = future.result()
                except (OSError, subprocess.SubprocessError) as caught:
                    error = str(caught)
                if error:
                    run_info["errors"].append(f"failed to collect {jobs[future]} log: {error}")
                    if exit_code == 0:
                        exit_code = 1
                        run_info["status"] = "failed"
        run_info["finished_at"] = utc_now()
        write_run_info(run_info_path, run_info)
        report = summarize_run(run_dir, args.output, args.transport)
    print(f"REPORT: {report}", flush=True)
    print(f"Raw journals retained in: {run_dir / 'logs'}")
    if exit_code == 0:
        print(f"SUCCESS: all {args.concurrency} sandboxes migrated to {args.target_node}")
    return exit_code


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (TestFailure, OSError, ValueError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(2) from error
