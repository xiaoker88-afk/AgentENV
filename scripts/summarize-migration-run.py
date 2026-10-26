#!/usr/bin/env python3
"""Condense one migration benchmark run into a portable JSON report."""

from __future__ import annotations

import argparse
import json
import math
import pathlib
import statistics
import sys
from collections import defaultdict


MARKER = "AENV_MIGRATION_STAGE "

STAGE_LABELS = {
    "source_lookup": "查询源节点",
    "target_lookup": "查询目标节点",
    "source_metadata": "读取源沙箱信息",
    "source_snapshot": "源节点制作并发布快照",
    "source_delete": "删除源节点沙箱",
    "target_restore": "目标节点获取快照并恢复沙箱",
    "assignment_update": "更新 Scheduler 节点归属",
    "migration_total": "迁移总耗时",
    "snapshot_repository_publish": "将快照提交到 OSS/S3 或 POSIX 仓库",
    "snapshot_p2p_publish": "将快照 Artifact 发布到 P2P",
    "snapshot_record_load": "读取快照记录",
    "snapshot_runtime_resolve": "解析快照运行时配置",
    "snapshot_vm_state_fetch": "获取 vm_state.bin",
    "snapshot_manifest_fetch": "获取 Firecracker manifest",
}

GATEWAY_STAGE_ORDER = {
    stage: index
    for index, stage in enumerate(
        (
            "source_lookup",
            "target_lookup",
            "source_metadata",
            "source_snapshot",
            "source_delete",
            "target_restore",
            "assignment_update",
            "migration_total",
        )
    )
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract per-sandbox migration latency from one benchmark run directory."
    )
    parser.add_argument("run_dir", type=pathlib.Path)
    parser.add_argument(
        "--output",
        type=pathlib.Path,
        help="default: <run_dir>/migration-summary.json",
    )
    parser.add_argument(
        "--transport",
        choices=("iroh", "urma", "disabled", "unknown"),
        help="override transport auto-detection",
    )
    return parser.parse_args()


def load_run_info(run_dir: pathlib.Path) -> tuple[pathlib.Path, dict]:
    preferred = run_dir / "run-info.json"
    candidates = [preferred] if preferred.is_file() else sorted(run_dir.glob("*.json"))
    candidates = [path for path in candidates if path.name != "migration-summary.json"]
    if not candidates:
        raise ValueError(f"no run-info JSON found directly under {run_dir}")
    if len(candidates) > 1 and preferred not in candidates:
        names = ", ".join(path.name for path in candidates)
        raise ValueError(f"multiple JSON files found ({names}); rename run metadata to run-info.json")
    path = candidates[0]
    try:
        return path, json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"cannot read {path}: {error}") from error


def find_logs(run_dir: pathlib.Path) -> list[pathlib.Path]:
    for directory_name in ("logs", "log"):
        directory = run_dir / directory_name
        if directory.is_dir():
            logs = sorted(directory.glob("*.log"))
            if logs:
                return logs
    logs = sorted(run_dir.glob("*.log"))
    if not logs:
        raise ValueError(f"no .log files found under {run_dir}/logs, {run_dir}/log, or {run_dir}")
    return logs


def log_node(path: pathlib.Path) -> str:
    name = path.stem.lower().replace("_", "-")
    if "gateway" in name:
        return "gateway"
    if "node-a" in name or "nodea" in name:
        return "node-a"
    if "node-b" in name or "nodeb" in name:
        return "node-b"
    return name


def extract_events(log_paths: list[pathlib.Path]) -> tuple[list[dict], list[str], str]:
    events: list[dict] = []
    errors: list[str] = []
    transport_evidence: set[str] = set()
    for path in log_paths:
        node = log_node(path)
        with path.open(encoding="utf-8", errors="replace") as stream:
            for line_number, line in enumerate(stream, 1):
                lower = line.lower()
                if (
                    "iroh artifact transport started" in lower
                    or 'backend="iroh"' in lower
                    or "agentenv::p2p::iroh" in lower
                ):
                    transport_evidence.add("iroh")
                if (
                    "urma artifact transport started" in lower
                    or 'backend="urma"' in lower
                    or "agentenv::p2p::urma" in lower
                ):
                    transport_evidence.add("urma")
                marker_at = line.find(MARKER)
                if marker_at < 0:
                    continue
                payload = line[marker_at + len(MARKER) :].strip()
                try:
                    event = json.JSONDecoder().raw_decode(payload)[0]
                except (json.JSONDecodeError, TypeError) as error:
                    errors.append(f"{path}:{line_number}: invalid migration event: {error}")
                    continue
                if event.get("schema") != "agentenv.migration-stage.v1":
                    continue
                event["node"] = node
                events.append(event)
    transport = next(iter(transport_evidence)) if len(transport_evidence) == 1 else "unknown"
    return events, errors, transport


def expected_sandbox_ids(run_info: dict) -> set[str]:
    sandbox_ids = {
        item.get("sandbox_id", "")
        for item in run_info.get("creations", [])
        if item.get("sandbox_id")
    }
    sandbox_ids.update(
        item.get("sandbox_id", "")
        for item in run_info.get("migrations", [])
        if item.get("sandbox_id")
    )
    return sandbox_ids


def attach_migration_ids(events: list[dict], allowed_sandboxes: set[str]) -> list[dict]:
    by_snapshot: dict[str, str] = {}
    by_sandbox: dict[str, set[str]] = defaultdict(set)
    for event in events:
        migration_id = event.get("migration_id")
        if not migration_id:
            continue
        if event.get("snapshot_id"):
            by_snapshot[event["snapshot_id"]] = migration_id
        if event.get("sandbox_id"):
            by_sandbox[event["sandbox_id"]].add(migration_id)

    allowed_migrations = {
        migration_id
        for sandbox_id in allowed_sandboxes
        for migration_id in by_sandbox.get(sandbox_id, set())
    }

    retained = []
    for event in events:
        if not event.get("migration_id"):
            snapshot_id = event.get("snapshot_id")
            if snapshot_id and snapshot_id in by_snapshot:
                event["migration_id"] = by_snapshot[snapshot_id]
            else:
                candidates = by_sandbox.get(event.get("sandbox_id", ""), set())
                if len(candidates) == 1:
                    event["migration_id"] = next(iter(candidates))
        if allowed_sandboxes:
            sandbox_allowed = event.get("sandbox_id") in allowed_sandboxes
            migration_allowed = event.get("migration_id") in allowed_migrations
            if not sandbox_allowed and not migration_allowed:
                continue
        retained.append(event)
    return retained


def nearest_rank(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    rank = max(0, math.ceil(fraction * len(ordered)) - 1)
    return ordered[rank]


def summarize_stages(events: list[dict]) -> list[dict]:
    groups: dict[tuple[str, str, str, str, str], list[float]] = defaultdict(list)
    for event in events:
        key = (
            event.get("node", "unknown"),
            event.get("component", "unknown"),
            event.get("stage", "unknown"),
            event.get("source", ""),
            event.get("status", "unknown"),
        )
        groups[key].append(float(event.get("duration_ms", 0)))

    rows = []
    for (node, component, stage, source, status), values in sorted(groups.items()):
        rows.append(
            {
                "node": node,
                "component": component,
                "stage": stage,
                "stage_zh": STAGE_LABELS.get(stage, stage),
                "source": source or None,
                "status": status,
                "samples": len(values),
                "mean_ms": round(statistics.fmean(values), 3),
                "p50_ms": round(statistics.median(values), 3),
                "p95_ms": round(nearest_rank(values, 0.95), 3),
                "min_ms": round(min(values), 3),
                "max_ms": round(max(values), 3),
            }
        )
    return rows


def compact_event(event: dict) -> dict:
    compact = {
        "node": event.get("node", "unknown"),
        "component": event.get("component", "unknown"),
        "stage": event.get("stage", "unknown"),
        "stage_zh": STAGE_LABELS.get(event.get("stage", ""), event.get("stage", "unknown")),
        "duration_ms": event.get("duration_ms", 0),
        "status": event.get("status", "unknown"),
    }
    for field in ("source", "backend", "size_bytes", "timestamp_unix_ms"):
        if event.get(field) not in (None, ""):
            compact[field] = event[field]
    return compact


def client_result(run_info: dict, sandbox_id: str) -> dict | None:
    result = next(
        (item for item in run_info.get("migrations", []) if item.get("sandbox_id") == sandbox_id),
        None,
    )
    if result is None:
        return None
    compact = {
        "ok": result.get("ok", False),
        "http_status": result.get("http_status", 0),
        "duration_ms": result.get("duration_ms"),
    }
    response = result.get("response")
    if response:
        try:
            compact["response"] = json.loads(response)
        except (json.JSONDecodeError, TypeError):
            compact["response"] = response
    return compact


def build_sandbox_reports(run_info: dict, events: list[dict]) -> list[dict]:
    migration_events: dict[str, list[dict]] = defaultdict(list)
    sandbox_to_migrations: dict[str, set[str]] = defaultdict(set)
    for event in events:
        migration_id = event.get("migration_id")
        if not migration_id:
            continue
        migration_events[migration_id].append(event)
        if event.get("sandbox_id"):
            sandbox_to_migrations[event["sandbox_id"]].add(migration_id)

    reports = []
    for sandbox_id in sorted(expected_sandbox_ids(run_info)):
        migration_ids = sandbox_to_migrations.get(sandbox_id, set())
        related = [
            event
            for migration_id in migration_ids
            for event in migration_events[migration_id]
        ]
        related.sort(key=lambda event: (event.get("timestamp_unix_ms", 0), event.get("stage", "")))
        snapshots = sorted({event["snapshot_id"] for event in related if event.get("snapshot_id")})
        gateway_events = [event for event in related if event.get("component") == "gateway"]
        gateway_events.sort(
            key=lambda event: (
                GATEWAY_STAGE_ORDER.get(event.get("stage", ""), 999),
                event.get("timestamp_unix_ms", 0),
            )
        )
        runtime_events = [event for event in related if event.get("component") != "gateway"]
        fetch_events = [
            event for event in runtime_events if event.get("stage", "").endswith("_fetch")
        ]
        p2p_ok = any(
            event.get("source") == "p2p" and event.get("status") == "ok"
            for event in fetch_events
        )
        p2p_miss = any(
            event.get("source") == "p2p" and event.get("status") != "ok"
            for event in fetch_events
        )
        oss_ok = any(
            event.get("source") == "oss" and event.get("status") == "ok"
            for event in fetch_events
        )
        reports.append(
            {
                "sandbox_id": sandbox_id,
                "migration_ids": sorted(migration_ids),
                "snapshot_ids": snapshots,
                "client": client_result(run_info, sandbox_id),
                "data_path": {
                    "p2p_success_observed": p2p_ok,
                    "p2p_miss_or_error_observed": p2p_miss,
                    "oss_success_observed": oss_ok,
                    "fallback_to_oss_observed": p2p_miss and oss_ok,
                },
                "gateway_stages": [compact_event(event) for event in gateway_events],
                "runtime_stages": [compact_event(event) for event in runtime_events],
            }
        )
    return reports


def compact_run_info(run_info: dict) -> dict:
    fields = (
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
        "errors",
    )
    return {field: run_info[field] for field in fields if field in run_info}


def main() -> int:
    args = parse_args()
    run_dir = args.run_dir.resolve()
    if not run_dir.is_dir():
        raise ValueError(f"run directory does not exist: {run_dir}")

    run_info_path, run_info = load_run_info(run_dir)
    log_paths = find_logs(run_dir)
    events, parse_errors, detected_transport = extract_events(log_paths)
    allowed_sandboxes = expected_sandbox_ids(run_info)
    events = attach_migration_ids(events, allowed_sandboxes)
    events.sort(key=lambda event: (event.get("timestamp_unix_ms", 0), event.get("stage", "")))
    transport = args.transport or detected_transport
    sandboxes = build_sandbox_reports(run_info, events)

    linked_events = sum(
        len(sandbox["gateway_stages"]) + len(sandbox["runtime_stages"])
        for sandbox in sandboxes
    )
    diagnostics = {
        "input_event_count": len(events),
        "linked_event_count": linked_events,
        "unlinked_event_count": sum(not event.get("migration_id") for event in events),
        "parse_errors": parse_errors,
        "sandboxes_without_migration_id": [
            sandbox["sandbox_id"] for sandbox in sandboxes if not sandbox["migration_ids"]
        ],
        "sandboxes_without_migration_total": [
            sandbox["sandbox_id"]
            for sandbox in sandboxes
            if not any(stage["stage"] == "migration_total" for stage in sandbox["gateway_stages"])
        ],
    }
    report = {
        "schema": "agentenv.migration-summary.v1",
        "run_directory": run_dir.name,
        "transport": transport,
        "run": compact_run_info(run_info),
        "stage_summary": summarize_stages(events),
        "sandboxes": sandboxes,
        "diagnostics": diagnostics,
        "inputs": {
            "run_info": run_info_path.name,
            "logs": [str(path.relative_to(run_dir)) for path in log_paths],
        },
    }

    output = args.output.resolve() if args.output else run_dir / "migration-summary.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(output)
    if not events:
        print("ERROR: no AENV_MIGRATION_STAGE events found", file=sys.stderr)
        return 2
    if parse_errors:
        print(f"WARNING: {len(parse_errors)} malformed migration events were skipped", file=sys.stderr)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(2) from error
