"""Command line: `madsci-data-publisher run` and `madsci-data-publisher site`."""

from __future__ import annotations

import argparse
import json
import sys

from .mcp_tables import TableOptions
from .publisher import LabPublisher, PublisherOptions
from .store import DataStore


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="madsci-data-publisher",
                                 description="Publish a MADSci lab's live state and site description to a data store.")
    ap.add_argument("--lab-url", help="Lab Manager URL. Default: $LAB_SERVER_URL or http://localhost:8000/")
    ap.add_argument("--out", default="data", help="Data store directory. Default: ./data")
    ap.add_argument("--format", choices=["parquet", "json"], default="parquet")
    ap.add_argument("--location", default="", help="Free-text lab location for the labs table.")
    sub = ap.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run", help="Poll live data every --interval seconds.")
    run.add_argument("--interval", type=float, default=30.0)
    run.add_argument("--once", action="store_true", help="Poll once and exit.")
    run.add_argument("--describe-site", action="store_true", help="Also collect a site description at start.")
    sub.add_parser("site", help="Collect a site description now.")
    args = ap.parse_args(argv)

    options = PublisherOptions(interval_s=getattr(args, "interval", 30.0), tables=TableOptions(location=args.location))
    pub = LabPublisher(DataStore(args.out, args.format), lab_url=args.lab_url, options=options,
                       log=lambda m: print(m, flush=True))
    if args.command == "site" or getattr(args, "describe_site", False):
        summary = pub.describe_site()
        print(f"site description: {len(summary['nodes'])} nodes, {len(summary['locations'])} locations, "
              f"{len(summary['transfers'])} transfer paths -> {args.out}/site_description.json")
        if args.command == "site":
            return 0
    try:
        status = pub.run(cycles=1 if args.once else None)
    except KeyboardInterrupt:
        return 0
    if args.once:
        print(json.dumps({"counts": status["counts"], "sources": status["sources"]}, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
