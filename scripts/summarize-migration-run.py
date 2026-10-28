#!/usr/bin/env python3
"""Compatibility entry point: generate the compact log using the unified runner."""

import argparse
import pathlib
import runpy
import sys

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dir")
    parser.add_argument("--output")
    parser.add_argument("--transport", choices=("urma", "iroh", "composite", "disabled", "unknown"))
    args = parser.parse_args()
    sys.argv = [sys.argv[0], "--summarize", args.run_dir]
    for name in ("output", "transport"):
        if getattr(args, name):
            sys.argv.extend(["--" + name, getattr(args, name)])
    runpy.run_path(
        str(pathlib.Path(__file__).with_name("run-migration-test.py")), run_name="__main__"
    )
