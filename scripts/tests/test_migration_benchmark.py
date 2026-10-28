#!/usr/bin/env python3
"""Offline fixtures for old/new migration logs and the unified benchmark entry point."""

import http.server
import importlib.util
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest import mock

SCRIPT = pathlib.Path(__file__).resolve().parents[1] / "run-migration-test.py"
spec = importlib.util.spec_from_file_location("migration_benchmark", SCRIPT)
bench = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench)


def marker(stage="migration_total", sandbox="s1", snapshot="snap1", **fields):
    return "AENV_MIGRATION_STAGE " + json.dumps(
        {
            "schema": "agentenv.migration-stage.v1",
            "component": "gateway",
            "stage": stage,
            "sandbox_id": sandbox,
            "snapshot_id": snapshot,
            "migration_id": f"m-{sandbox}",
            "duration_ms": 123,
            "status": "ok",
            **fields,
        }
    )


def info():
    return {
        "concurrency": 1,
        "status": "succeeded",
        "transport": "urma",
        "migrations": [
            {
                "sandbox_id": "s1",
                "ok": True,
                "http_status": 200,
                "duration_ms": 124,
                "response": '{"snapshotID":"snap1"}',
            }
        ],
    }


class ReportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = pathlib.Path(self.temp.name)
        (self.root / "logs").mkdir()
        (self.root / "run-info.json").write_text(json.dumps(info()))

    def log(self, name, *lines):
        path = self.root / "logs" / f"{name}.log"
        path.write_text("\n".join(lines) + "\n")
        return path

    def test_plain_ansi_span_and_journal_json(self):
        key = "snapshot/v1/artifacts/snap1/vm_state.bin"
        plain = f'\x1b[32mINFO\x1b[0m urma_stream{{key={key}}}: urma stage timing stage="read_window" status="ok" duration_ms=62.125'
        structured = json.dumps({"MESSAGE": plain, "__REALTIME_TIMESTAMP": "1793008189000123"})
        read = f"urma_stream{{key={key}}}: urma read range timing remote_va=4096 offset=0 bytes=512 connect_ms=1 import_ms=2 post_ms=3 completion_wait_ms=50 reads_ms=53 read_count=1"
        events, _, counts = bench.read_events([self.log("node-b", plain, structured, read)])
        self.assertEqual(counts["parse_errors"], 0)
        self.assertEqual(len(events), 7)
        self.assertTrue(all(event["key"] == key for event in events))
        self.assertEqual(events[0]["duration_ms"], 62.125)
        self.assertEqual(
            next(e for e in events if e["stage"] == "read_completion_wait")["duration_ms"], 50
        )

    def test_nested_tracing_json_and_bunyan(self):
        key = "snapshot/v1/artifacts/snap1/firecracker-manifest.json"
        traced = {
            "fields": {
                "message": "urma stage timing",
                "stage": "fetch_bytes_total",
                "duration_ms": 60.5,
                "status": "ok",
            },
            "spans": [{"name": "urma_stream", "key": key}],
        }
        bunyan = {
            "msg": "urma stage timing",
            "key": key,
            "stage": "catalog_response_wait",
            "duration_ms": 59,
            "status": "ok",
        }
        gateway = {"MESSAGE": json.dumps({"fields": {"message": marker()}})}
        events, _, counts = bench.read_events(
            [
                self.log("nodeb", json.dumps(traced), json.dumps({"MESSAGE": json.dumps(bunyan)})),
                self.log("gateway", json.dumps(gateway)),
            ]
        )
        self.assertEqual(counts["parse_errors"], 0)
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0]["node"], "node-b")
        self.assertEqual(events[1]["key"], key)
        self.assertEqual(events[2]["stage"], "migration_total")

    def test_journal_byte_arrays_decode_runtime_timings_and_warning_levels(self):
        key = "snapshot/v1/artifacts/snap1/vm_state.bin"
        runtime = "\x1b[32mINFO\x1b[0m " + marker(
            stage="snapshot_repository_publish", component="runtime"
        )
        timing = f'\x1b[32mINFO\x1b[0m urma stage timing key={key} stage="publish_total" status="ok" duration_ms=12.5'
        warning = json.dumps(
            {
                "level": "warn",
                "msg": "migrate: create on target failed",
                "error": "context deadline exceeded",
                "sandbox_id": "s1",
            }
        )
        path = self.log(
            "node-a",
            *(
                json.dumps(
                    {"MESSAGE": list(message.encode()), "__REALTIME_TIMESTAMP": "1793175217000000"}
                )
                for message in (runtime, timing, warning)
            ),
        )
        events, warnings, counts = bench.read_events([path])
        self.assertEqual(counts["parse_errors"], 0)
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0]["stage"], "snapshot_repository_publish")
        self.assertEqual(events[1]["duration_ms"], 12.5)
        self.assertEqual(events[1]["key"], key)
        self.assertEqual(counts["warning_lines"], 1)
        self.assertIn("context deadline exceeded", warnings[0])

    def test_lowercase_gateway_warning_is_not_dropped(self):
        path = self.log(
            "gateway",
            json.dumps(
                {
                    "MESSAGE": json.dumps(
                        {
                            "level": "warn",
                            "msg": "migrate: create on target failed",
                            "error": "deadline exceeded",
                        }
                    )
                }
            ),
        )
        _, warnings, counts = bench.read_events([path])
        self.assertEqual(counts["warning_lines"], 1)
        self.assertIn("deadline exceeded", warnings[0])

    def test_shared_keys_not_double_counted_and_unrelated_events_excluded(self):
        run = info()
        run["migrations"].append({"sandbox_id": "s2", "response": '{"snapshotID":"snap2"}'})
        path = self.log(
            "node-a",
            marker(),
            marker(sandbox="s2", snapshot="snap2"),
            marker(sandbox="other", snapshot="unrelated"),
            "snapshot P2P artifact publication started snapshot_id=snap1 key=sha256/shared",
            "snapshot P2P artifact publication started snapshot_id=snap2 key=sha256/shared",
            'urma stage timing key=sha256/shared stage="publish_total" duration_ms=1800 status="ok"',
            'urma stage timing key=snapshot/v1/artifacts/unrelated/vm_state.bin stage="fetch_file_total" duration_ms=999 status="ok"',
        )
        events, _, _ = bench.read_events([path])
        pipeline, timings, unlinked = bench.correlate_events(events, run)
        self.assertEqual(len(pipeline), 2)
        self.assertEqual(unlinked, 1)
        self.assertEqual(timings[0]["snapshot_ids"], ["snap1", "snap2"])
        self.assertEqual(bench.stage_statistics(timings)[0]["n"], 1)

    def test_remote_va_fallback_does_not_guess_ambiguous_key(self):
        events, _, _ = bench.read_events(
            [
                self.log(
                    "node-b",
                    marker(),
                    "urma read window started key=snapshot/v1/artifacts/snap1/vm_state.bin remote_va=100",
                    "urma read range timing remote_va=100 connect_ms=0 import_ms=1 post_ms=2 completion_wait_ms=3 reads_ms=5",
                )
            ]
        )
        _, timings, _ = bench.correlate_events(events, info())
        self.assertEqual(len(timings), 5)
        events.append({"kind": "window", "node": "node-b", "key": "other", "remote_va": 100})
        _, timings, unlinked = bench.correlate_events(events, info())
        self.assertEqual(len(timings), 0)
        self.assertEqual(unlinked, 5)

    def test_corrupt_and_nonfinite_events_are_reported(self):
        path = self.log(
            "node-a",
            "AENV_MIGRATION_STAGE {broken",
            'urma stage timing key=k stage="publish_total" duration_ms=nan',
            marker(duration_ms=-1),
            "WARN: retry happened",
        )
        events, warnings, counts = bench.read_events([path])
        self.assertEqual(events, [])
        self.assertEqual(counts["parse_errors"], 3)
        self.assertEqual(counts["warning_lines"], 1)
        self.assertEqual(len(warnings), 4)

    def test_compact_report_keeps_failures_and_slowest_only(self):
        self.log("gateway", marker(), marker(stage="target_restore", duration_ms=70))
        lines = ["DEBUG irrelevant image work " + "x" * 100] * 1000
        lines += [
            f'urma stage timing key=snapshot/v1/artifacts/snap1/vm_state.bin stage="read_window" duration_ms={n} status="ok"'
            for n in range(100)
        ]
        lines += [
            'urma stage timing key=snapshot/v1/artifacts/snap1/vm_state.bin stage="read_window" duration_ms=4 status="error_or_cancelled"',
            "WARN catalog retry",
        ]
        raw = self.log("node-b", *lines)
        output = bench.summarize_run(self.root)
        text = output.read_text()
        self.assertIn("node-b read_window - ok 100 49.500 49.500 94.000 0.000 99.000", text)
        self.assertIn("error_or_cancelled", text)
        self.assertIn("WARN catalog retry", text)
        self.assertNotIn("DEBUG irrelevant", text)
        self.assertLess(output.stat().st_size, raw.stat().st_size // 10)
        self.assertEqual(text.count('"stage":"read_window"'), 3)  # two successes, one failure

    def test_created_sandbox_without_result_is_visible(self):
        run = info()
        run["creations"] = [{"sandbox_id": "never-migrated"}]
        (self.root / "run-info.json").write_text(json.dumps(run))
        self.log("gateway", marker())
        text = bench.summarize_run(self.root).read_text()
        self.assertIn("sandbox=never-migrated", text)
        self.assertIn("HTTP=not-recorded", text)
        self.assertIn("missing_migration_total=never-migrated", text)

    def test_collection_streams_full_journal_and_reports_command_failure(self):
        path = self.root / "logs" / "node-b.log"

        def run(command, **kwargs):
            self.assertIn("BatchMode=yes", command)
            self.assertIn("--all", command[-1])
            self.assertIn("--after-cursor", command[-1])
            self.assertEqual(command[-2], "root@b")
            kwargs["stdout"].write('{"MESSAGE":"some log"}\n')
            return subprocess.CompletedProcess(command, 1, stderr="journal error")

        with mock.patch.object(bench.subprocess, "run", side_effect=run):
            self.assertEqual(
                bench.collect_journal("root@b", "agentenv", "cursor", path), "journal error"
            )
        self.assertIn("some log", path.read_text())

    def test_legacy_script_invocation_and_offline_mode(self):
        self.log("gateway", marker())
        for script, args in (
            (SCRIPT, ["--summarize", str(self.root)]),
            (SCRIPT.with_name("summarize-migration-run.py"), [str(self.root)]),
        ):
            result = subprocess.run(
                [sys.executable, str(script), *args],
                capture_output=True,
                text=True,
                env={key: value for key, value in os.environ.items() if key != "AENV_API_KEY"},
            )
            self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("migration_total", (self.root / "migration-summary.log").read_text())


class HttpTests(unittest.TestCase):
    def test_api_requests_bypass_environment_proxy_and_preserve_http_errors(self):
        received = []
        proxy_requests = []

        class API(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                received.append((self.command, self.path, self.headers.get("X-API-Key"), body))
                self.send_response(503 if self.path == "/fail" else 200)
                self.end_headers()
                self.wfile.write(b'{"message":"API response"}')

            do_POST = do_GET
            do_DELETE = do_GET

            def log_message(self, *args):
                pass

        class Proxy(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                proxy_requests.append(self.path)
                self.send_response(504)
                self.end_headers()
                self.wfile.write(b"proxy error page")

            do_POST = do_GET
            do_DELETE = do_GET

            def log_message(self, *args):
                pass

        servers = [
            http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler) for handler in (API, Proxy)
        ]
        threads = [
            threading.Thread(
                target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True
            )
            for server in servers
        ]
        for thread in threads:
            thread.start()
        try:
            api_url, proxy_url = [f"http://127.0.0.1:{server.server_port}" for server in servers]
            overrides = {
                name: proxy_url
                for name in (
                    "http_proxy",
                    "https_proxy",
                    "all_proxy",
                    "HTTP_PROXY",
                    "HTTPS_PROXY",
                    "ALL_PROXY",
                )
            }
            overrides.update({"no_proxy": "", "NO_PROXY": "", "noproxy": "*"})
            with mock.patch.dict(os.environ, overrides):
                for method in ("GET", "POST", "DELETE"):
                    result = bench.http_request(
                        method,
                        api_url + "/sandboxes",
                        "test-key",
                        body={"image": "example:v1"} if method == "POST" else None,
                        timeout=2,
                    )
                    self.assertTrue(result["ok"], result)
                failed = bench.http_request("GET", api_url + "/fail", "test-key", timeout=2)
            self.assertEqual(proxy_requests, [])
            self.assertEqual([request[0] for request in received], ["GET", "POST", "DELETE", "GET"])
            self.assertTrue(all(request[2] == "test-key" for request in received))
            self.assertEqual(json.loads(received[1][3]), {"image": "example:v1"})
            self.assertFalse(failed["ok"])
            self.assertEqual(failed["http_status"], 503)
            self.assertIn("API response", failed["response"])
        finally:
            for server, thread in zip(servers, threads):
                server.shutdown()
                thread.join(timeout=2)
                server.server_close()


class RunnerTests(unittest.TestCase):
    def test_default_topology_runs_on_a_and_only_uses_ssh_for_b(self):
        with mock.patch.dict(os.environ, {}, clear=True), mock.patch.object(
            sys, "argv", [str(SCRIPT)]
        ):
            args = bench.parse_args()
        self.assertEqual(args.source_runtime, "http://127.0.0.1:8000")
        self.assertEqual(args.gateway, "http://127.0.0.1:8081")
        self.assertEqual(args.gateway_ssh, "local")
        self.assertEqual(args.source_ssh, "local")
        self.assertEqual(args.target_ssh, "root@10.10.10.02")
        self.assertEqual(args.target_node, "node-b")
        command = ["journalctl", "-u", "agentenv"]
        with mock.patch.object(os, "geteuid", return_value=0):
            self.assertEqual(bench.host_command(args.gateway_ssh, command), command)
            self.assertEqual(bench.host_command(args.source_ssh, command), command)
        remote = bench.host_command(args.target_ssh, command)
        self.assertEqual(remote[0], "ssh")
        self.assertEqual(remote[-2], "root@10.10.10.02")

    def test_run_collects_three_hosts_after_creation_and_reports_failures(self):
        for fail_migration in (False, True):
            with self.subTest(fail_migration=fail_migration), tempfile.TemporaryDirectory() as root:
                argv = [
                    str(SCRIPT),
                    "--concurrency",
                    "8",
                    "--image",
                    "registry/small:v1",
                    "--output-root",
                    root,
                    "--gateway-ssh",
                    "root@gw",
                    "--node-a-ssh",
                    "root@a",
                    "--node-b-ssh",
                    "root@b",
                ]
                with mock.patch.object(sys, "argv", argv):
                    args = bench.parse_args()
                creations = []
                cursors = []

                def create(*_):
                    sandbox = f"s{len(creations)}"
                    creations.append(sandbox)
                    return {"ok": True, "sandbox_id": sandbox}

                def cursor(target, unit, label):
                    cursors.append((target, len(creations)))
                    return f"cursor-{len(creations)}"

                def migrate(gateway, api_key, target_node, sandbox):
                    return {
                        "sandbox_id": sandbox,
                        "ok": not fail_migration,
                        "http_status": 500 if fail_migration else 200,
                        "response": json.dumps({"snapshotID": f"snap-{sandbox}"}),
                        "duration_ms": 12,
                    }

                def collect(target, unit, cursor_value, path):
                    self.assertEqual(cursor_value, "cursor-8")
                    path.write_text(
                        "\n".join(marker(sandbox=s, snapshot=f"snap-{s}") for s in creations)
                    )
                    return None

                with mock.patch.object(bench, "parse_args", return_value=args), mock.patch.dict(
                    os.environ, {"AENV_API_KEY": "do-not-persist-this"}
                ), mock.patch.object(bench, "ensure_service_active"), mock.patch.object(
                    bench, "journal_cursor", side_effect=cursor
                ), mock.patch.object(
                    bench, "create_sandbox", side_effect=create
                ), mock.patch.object(
                    bench, "wait_for_gateway_bindings"
                ), mock.patch.object(
                    bench, "migrate_sandbox", side_effect=migrate
                ), mock.patch.object(
                    bench, "collect_journal", side_effect=collect
                ) as collector, mock.patch.object(
                    bench.time, "sleep"
                ):
                    self.assertEqual(bench.main(), 1 if fail_migration else 0)
                run_dir = next(pathlib.Path(root).iterdir())
                report = (run_dir / "migration-summary.log").read_text()
                self.assertIn('"root@gw"', report)
                self.assertIn('"root@a"', report)
                self.assertIn('"root@b"', report)
                self.assertNotIn("do-not-persist-this", report)
                self.assertEqual(
                    cursors, [(host, n) for n in (0, 8) for host in ("root@gw", "root@a", "root@b")]
                )
                self.assertEqual(collector.call_count, 3)
                self.assertEqual(
                    len(json.loads((run_dir / "run-info.json").read_text())["migrations"]), 8
                )

    def test_preflight_failure_still_writes_report(self):
        with tempfile.TemporaryDirectory() as root:
            with mock.patch.object(
                sys,
                "argv",
                [str(SCRIPT), "--concurrency", "1", "--image", "img", "--output-root", root],
            ):
                args = bench.parse_args()
            with mock.patch.object(bench, "parse_args", return_value=args), mock.patch.dict(
                os.environ, {"AENV_API_KEY": "key"}
            ), mock.patch.object(
                bench, "ensure_service_active", side_effect=bench.TestFailure("gateway not active")
            ), mock.patch.object(
                bench.time, "sleep"
            ):
                self.assertEqual(bench.main(), 1)
            text = next(pathlib.Path(root).glob("*/migration-summary.log")).read_text()
            self.assertIn("gateway not active", text)
            self.assertIn("no migration stage events", text)


if __name__ == "__main__":
    unittest.main()
