#!/usr/bin/env python3
"""Submit a controlled batch of concurrent AgentENV migrations."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import pathlib
import time
import urllib.error
import urllib.request


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--gateway", required=True, help="e.g. http://10.10.10.1:8080")
    parser.add_argument("--target-node", required=True)
    parser.add_argument("--sandbox-ids", type=pathlib.Path, required=True, help="one sandbox ID per line")
    parser.add_argument("--concurrency", type=int, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--timeout", type=int, default=360)
    return parser.parse_args()


def migrate(gateway: str, api_key: str, target_node: str, sandbox_id: str, timeout: int) -> dict:
    body = json.dumps({"targetNodeID": target_node}).encode()
    request = urllib.request.Request(
        f"{gateway.rstrip('/')}/sandboxes/{sandbox_id}/migrate",
        data=body,
        method="POST",
        headers={"Content-Type": "application/json", "X-API-Key": api_key},
    )
    started_ns = time.time_ns()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            response_body = response.read().decode(errors="replace")
            status = response.status
            ok = 200 <= status < 300
    except urllib.error.HTTPError as error:
        response_body = error.read().decode(errors="replace")
        status = error.code
        ok = False
    except Exception as error:  # Preserve connection/timeout failures in the batch report.
        response_body = str(error)
        status = 0
        ok = False
    finished_ns = time.time_ns()
    return {
        "sandbox_id": sandbox_id,
        "target_node": target_node,
        "started_unix_ns": started_ns,
        "finished_unix_ns": finished_ns,
        "client_duration_ms": round((finished_ns - started_ns) / 1_000_000, 3),
        "http_status": status,
        "ok": ok,
        "response": response_body,
    }


def main() -> int:
    args = parse_args()
    if args.concurrency < 1:
        raise ValueError("--concurrency must be at least 1")
    api_key = os.environ.get("AENV_API_KEY", "")
    if not api_key:
        raise ValueError("AENV_API_KEY is required in the environment")
    sandbox_ids = [
        line.strip()
        for line in args.sandbox_ids.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    if not sandbox_ids:
        raise ValueError("sandbox ID file is empty")

    batch_started_ns = time.time_ns()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        futures = [
            executor.submit(
                migrate, args.gateway, api_key, args.target_node, sandbox_id, args.timeout
            )
            for sandbox_id in sandbox_ids
        ]
        results = [future.result() for future in concurrent.futures.as_completed(futures)]
    batch_finished_ns = time.time_ns()

    report = {
        "gateway": args.gateway,
        "target_node": args.target_node,
        "concurrency": args.concurrency,
        "batch_duration_ms": round((batch_finished_ns - batch_started_ns) / 1_000_000, 3),
        "requested": len(results),
        "succeeded": sum(result["ok"] for result in results),
        "failed": sum(not result["ok"] for result in results),
        "results": sorted(results, key=lambda result: result["started_unix_ns"]),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(args.output)
    return 0 if report["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
