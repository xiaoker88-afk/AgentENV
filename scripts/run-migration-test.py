#!/usr/bin/env python3
"""Create sandboxes, migrate them concurrently, and retain this run's journals."""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import json
import os
import pathlib
import shlex
import subprocess
import sys
import time
import urllib.error
import urllib.request


REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "artifacts" / "migration-runs"


class TestFailure(RuntimeError):
    """A user-facing test failure."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create independent sandboxes on node-a and migrate them concurrently to node-b."
    )
    parser.add_argument("--concurrency", type=int, required=True)
    parser.add_argument("--image", required=True, help="complete OCI image reference")
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
    if target_ssh is None:
        return local_command(command)
    return [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=10",
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
    result = run_command(
        host_command(
            target_ssh,
            [
                "journalctl",
                "-u",
                unit,
                "--after-cursor",
                cursor,
                "--no-pager",
                "-o",
                "short-iso",
            ],
        ),
        timeout=60,
    )
    destination.write_text(result.stdout, encoding="utf-8")
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
    started_ns = time.time_ns()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            response_body = response.read().decode(errors="replace")
            status = response.status
    except urllib.error.HTTPError as error:
        response_body = error.read().decode(errors="replace")
        status = error.code
    except Exception as error:
        response_body = str(error)
        status = 0
    finished_ns = time.time_ns()
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
        details = ", ".join(f"{sandbox_id}=HTTP {last_status.get(sandbox_id, 0)}" for sandbox_id in pending)
        raise TestFailure(f"Gateway did not resolve all source sandboxes within {timeout}s: {details}")


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


def main() -> int:
    args = parse_args()
    if args.concurrency < 1:
        raise TestFailure("--concurrency must be at least 1")

    api_key = os.environ.get("AENV_API_KEY", "")
    if not api_key:
        raise TestFailure("AENV_API_KEY is required in the environment")

    source_runtime = env("AENV_BENCH_SOURCE_RUNTIME", "http://141.61.17.188:8000")
    gateway = env("AENV_BENCH_GATEWAY", "http://127.0.0.1:8081")
    target_node = env("AENV_BENCH_TARGET_NODE", "node-b")
    target_ssh = env("AENV_BENCH_TARGET_SSH", "root@10.10.10.2")
    gateway_unit = env("AENV_BENCH_GATEWAY_UNIT", "aenv-gateway")
    runtime_unit = env("AENV_BENCH_RUNTIME_UNIT", "agentenv")
    output_root = pathlib.Path(env("AENV_BENCH_OUTPUT_ROOT", str(DEFAULT_OUTPUT_ROOT)))
    run_dir = make_run_directory(output_root, args.concurrency)
    run_info_path = run_dir / "run-info.json"

    run_info: dict = {
        "status": "running",
        "started_at": utc_now(),
        "image": args.image,
        "concurrency": args.concurrency,
        "source_runtime": source_runtime,
        "gateway": gateway,
        "target_node": target_node,
        "target_ssh": target_ssh,
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
    }
    write_run_info(run_info_path, run_info)

    cursors: dict[str, str] = {}
    exit_code = 0
    try:
        ensure_service_active(None, gateway_unit, "gateway")
        ensure_service_active(None, runtime_unit, "node-a runtime")
        ensure_service_active(target_ssh, runtime_unit, "node-b runtime")

        cursors = {
            "gateway": journal_cursor(None, gateway_unit, "gateway"),
            "node-a": journal_cursor(None, runtime_unit, "node-a runtime"),
            "node-b": journal_cursor(target_ssh, runtime_unit, "node-b runtime"),
        }

        creation_started_ns = time.time_ns()
        for index in range(args.concurrency):
            print(f"[create {index + 1}/{args.concurrency}] {args.image}", flush=True)
            creation = create_sandbox(source_runtime, api_key, args.image)
            creation["index"] = index + 1
            run_info["creations"].append(creation)
            write_run_info(run_info_path, run_info)
            if not creation["ok"]:
                raise TestFailure(
                    f"sandbox {index + 1}/{args.concurrency} creation failed: "
                    f"HTTP {creation['http_status']}: {creation['response']}"
                )
        creation_finished_ns = time.time_ns()
        run_info["creation_batch_duration_ms"] = round(
            (creation_finished_ns - creation_started_ns) / 1_000_000, 3
        )

        sandbox_ids = [creation["sandbox_id"] for creation in run_info["creations"]]
        print("[prepare] waiting for Gateway/Scheduler source bindings", flush=True)
        wait_for_gateway_bindings(gateway, api_key, sandbox_ids)

        print(f"[migrate] submitting {len(sandbox_ids)} migrations concurrently", flush=True)
        migration_started_ns = time.time_ns()
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as executor:
            futures = [
                executor.submit(migrate_sandbox, gateway, api_key, target_node, sandbox_id)
                for sandbox_id in sandbox_ids
            ]
            run_info["migrations"] = [
                future.result() for future in concurrent.futures.as_completed(futures)
            ]
        migration_finished_ns = time.time_ns()
        run_info["migration_batch_duration_ms"] = round(
            (migration_finished_ns - migration_started_ns) / 1_000_000, 3
        )
        run_info["migrations"].sort(key=lambda result: result["sandbox_id"])

        failed = [result for result in run_info["migrations"] if not result["ok"]]
        if failed:
            failures = "; ".join(
                f"{result['sandbox_id']}=HTTP {result['http_status']}: {result['response']}"
                for result in failed
            )
            raise TestFailure(f"{len(failed)} of {len(sandbox_ids)} migrations failed: {failures}")
        run_info["status"] = "succeeded"
    except KeyboardInterrupt:
        exit_code = 130
        run_info["status"] = "interrupted"
        run_info["errors"].append("interrupted by user")
    except (TestFailure, subprocess.SubprocessError) as error:
        exit_code = 1
        run_info["status"] = "failed"
        run_info["errors"].append(str(error))
        print(f"ERROR: {error}", file=sys.stderr)
    finally:
        time.sleep(1)
        log_errors = []
        destinations = {
            "gateway": (None, gateway_unit, run_dir / "logs" / "gateway.log"),
            "node-a": (None, runtime_unit, run_dir / "logs" / "node-a.log"),
            "node-b": (target_ssh, runtime_unit, run_dir / "logs" / "node-b.log"),
        }
        for label, (ssh_target, unit, destination) in destinations.items():
            cursor = cursors.get(label)
            if cursor is None:
                destination.touch()
                continue
            try:
                error = collect_journal(ssh_target, unit, cursor, destination)
            except (OSError, subprocess.SubprocessError) as error_value:
                error = str(error_value)
            if error:
                message = f"failed to collect {label} log: {error}"
                log_errors.append(message)
                print(f"ERROR: {message}", file=sys.stderr)
        if log_errors:
            run_info["errors"].extend(log_errors)
            if exit_code == 0:
                exit_code = 1
                run_info["status"] = "failed"
        run_info["finished_at"] = utc_now()
        write_run_info(run_info_path, run_info)

    print(f"run directory: {run_dir}")
    if exit_code == 0:
        print(f"SUCCESS: all {args.concurrency} sandboxes migrated to {target_node}")
    return exit_code


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except TestFailure as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(2) from error
