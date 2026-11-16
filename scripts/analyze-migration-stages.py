#!/usr/bin/env python3
"""Aggregate AENV_MIGRATION_STAGE records from Gateway and Runtime logs."""

from __future__ import annotations

import argparse
import csv
import json
import math
import pathlib
import statistics
import sys
from collections import defaultdict

MARKER = "AENV_MIGRATION_STAGE "


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate JSON, CSV, and Markdown reports from AgentENV migration timing logs."
    )
    parser.add_argument("logs", nargs="+", type=pathlib.Path, help="journalctl/plain log files")
    parser.add_argument("--output-dir", type=pathlib.Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--transport", choices=("disabled", "iroh", "urma"), required=True)
    parser.add_argument("--sandbox-profile", required=True, help="e.g. idle-1g or memory-dirty-4g")
    parser.add_argument("--concurrency", type=int, required=True)
    return parser.parse_args()


def extract_events(paths: list[pathlib.Path]) -> list[dict]:
    events: list[dict] = []
    for path in paths:
        with path.open(encoding="utf-8", errors="replace") as stream:
            for line_number, line in enumerate(stream, 1):
                marker_at = line.find(MARKER)
                if marker_at < 0:
                    continue
                payload = line[marker_at + len(MARKER) :].strip()
                try:
                    event = json.JSONDecoder().raw_decode(payload)[0]
                except (json.JSONDecodeError, TypeError) as error:
                    raise ValueError(f"{path}:{line_number}: invalid migration event: {error}") from error
                if event.get("schema") != "agentenv.migration-stage.v1":
                    continue
                event["log_file"] = str(path)
                events.append(event)
    return events


def attach_migration_ids(events: list[dict]) -> None:
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

    for event in events:
        if event.get("migration_id"):
            continue
        snapshot_id = event.get("snapshot_id")
        if snapshot_id and snapshot_id in by_snapshot:
            event["migration_id"] = by_snapshot[snapshot_id]
            continue
        sandbox_ids = by_sandbox.get(event.get("sandbox_id", ""), set())
        if len(sandbox_ids) == 1:
            event["migration_id"] = next(iter(sandbox_ids))


def percentile(values: list[float], percentile_value: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    rank = max(0, math.ceil(percentile_value * len(ordered)) - 1)
    return ordered[rank]


def summarize(events: list[dict]) -> list[dict]:
    groups: dict[tuple[str, str, str], list[float]] = defaultdict(list)
    for event in events:
        key = (event.get("component", "unknown"), event.get("stage", "unknown"), event.get("source", ""))
        groups[key].append(float(event.get("duration_ms", 0)))

    rows = []
    for (component, stage, source), values in sorted(groups.items()):
        rows.append(
            {
                "component": component,
                "stage": stage,
                "source": source,
                "samples": len(values),
                "mean_ms": round(statistics.fmean(values), 3),
                "p50_ms": round(statistics.median(values), 3),
                "p95_ms": round(percentile(values, 0.95), 3),
                "min_ms": round(min(values), 3),
                "max_ms": round(max(values), 3),
            }
        )
    return rows


def write_csv(path: pathlib.Path, rows: list[dict], fieldnames: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_markdown(path: pathlib.Path, metadata: dict, events: list[dict], summary: list[dict]) -> None:
    migrations = {event.get("migration_id") for event in events if event.get("migration_id")}
    with path.open("w", encoding="utf-8") as stream:
        stream.write(f"# AgentENV 迁移时延报告：{metadata['run_id']}\n\n")
        stream.write("## 实验信息\n\n")
        stream.write(f"- 传输后端：`{metadata['transport']}`\n")
        stream.write(f"- 沙箱模型：`{metadata['sandbox_profile']}`\n")
        stream.write(f"- 并发数：`{metadata['concurrency']}`\n")
        stream.write(f"- 识别到的迁移数：`{len(migrations)}`\n")
        stream.write(f"- 阶段事件数：`{len(events)}`\n\n")
        stream.write("## 阶段汇总\n\n")
        stream.write("| 组件 | 阶段 | 数据来源 | 样本 | 平均 ms | P50 ms | P95 ms | 最小 ms | 最大 ms |\n")
        stream.write("|---|---|---|---:|---:|---:|---:|---:|---:|\n")
        for row in summary:
            stream.write(
                f"| {row['component']} | {row['stage']} | {row['source'] or '-'} | "
                f"{row['samples']} | {row['mean_ms']} | {row['p50_ms']} | {row['p95_ms']} | "
                f"{row['min_ms']} | {row['max_ms']} |\n"
            )
        stream.write("\n> P95 使用 nearest-rank；样本较少时仅用于观察，不应作为稳定结论。\n")


def main() -> int:
    args = parse_args()
    if args.concurrency < 1:
        raise ValueError("--concurrency must be at least 1")
    events = extract_events(args.logs)
    if not events:
        print("no AENV_MIGRATION_STAGE events found", file=sys.stderr)
        return 2
    attach_migration_ids(events)
    events.sort(key=lambda event: (event.get("timestamp_unix_ms", 0), event.get("stage", "")))
    summary = summarize(events)
    metadata = {
        "run_id": args.run_id,
        "transport": args.transport,
        "sandbox_profile": args.sandbox_profile,
        "concurrency": args.concurrency,
        "input_logs": [str(path) for path in args.logs],
    }

    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "report.json").write_text(
        json.dumps({"metadata": metadata, "summary": summary, "events": events}, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    event_fields = [
        "timestamp_unix_ms", "migration_id", "component", "stage", "duration_ms", "status",
        "sandbox_id", "snapshot_id", "source_node", "target_node", "backend", "source",
        "size_bytes", "log_file",
    ]
    write_csv(args.output_dir / "events.csv", events, event_fields)
    write_csv(
        args.output_dir / "summary.csv",
        summary,
        ["component", "stage", "source", "samples", "mean_ms", "p50_ms", "p95_ms", "min_ms", "max_ms"],
    )
    write_markdown(args.output_dir / "report.md", metadata, events, summary)
    print(args.output_dir / "report.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
