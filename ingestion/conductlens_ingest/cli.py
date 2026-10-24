"""cl-ingest: the command line the Airflow DAGs call. See docs/ARCHITECTURE.md §4."""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date
from pathlib import Path

from .config import Settings
from .runner import MODES, Runner


def _date(s: str | None) -> date | None:
    return date.fromisoformat(s) if s else None


def _sinks(arg: str | None, settings: Settings) -> list[str]:
    return [s.strip() for s in arg.split(",") if s.strip()] if arg else list(settings.sinks)


def _global_options(parser: argparse.ArgumentParser, suppress: bool) -> None:
    """--run-id / --sinks / -v are accepted both before and after the subcommand
    (Airflow writes `cl-ingest run ... --run-id X`). SUPPRESS keeps a subcommand from
    overriding a value given at the top level with its default."""
    d = argparse.SUPPRESS if suppress else None
    parser.add_argument("--run-id", default=d, help="run identifier written to _load_runs (default: local-<utc>)")
    parser.add_argument("--sinks", default=d, help="comma list: bigquery,databricks,local (default: CL_SINKS)")
    parser.add_argument("-v", "--verbose", action="store_true", default=argparse.SUPPRESS if suppress else False)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="cl-ingest", description=__doc__)
    _global_options(p, suppress=False)
    common = argparse.ArgumentParser(add_help=False)
    _global_options(common, suppress=True)
    sub = p.add_subparsers(dest="cmd", required=True, parser_class=lambda **kw: argparse.ArgumentParser(parents=[common], **kw))

    run = sub.add_parser("run", help="load one or all sources")
    run.add_argument("--source", choices=["cfpb", "fdic", "fred", "all"], default="all")
    run.add_argument("--mode", choices=MODES, default="incremental")
    run.add_argument("--start", type=_date, default=None)
    run.add_argument("--end", type=_date, default=None)

    sub.add_parser("ensure-tables", help="create missing datasets/schemas/tables; rotate expiring BigQuery tables")
    rh = sub.add_parser("rehydrate", help="full load of the latest landed CFPB parquet into one sink")
    rh.add_argument("--sink", default="bigquery")
    sub.add_parser("status", help="print _load_state and recent _load_runs")
    sub.add_parser("parity-check", help="compare marts across warehouses; non-zero exit on mismatch")
    ev = sub.add_parser("evidence", help="write evidence/scheduled_runs.md")
    ev.add_argument("--out-dir", default=None, help="default: $CL_EVIDENCE_DIR or ./evidence")
    sub.add_parser("convert", help="dev: download the CFPB bulk file and convert to parquet only")
    return p


def main(argv: list[str] | None = None) -> int:
    try:
        return _main(argv)
    finally:
        for sink in _OPEN_SINKS:
            getattr(sink, "close", lambda: None)()


_OPEN_SINKS: list = []


def _main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s", stream=sys.stdout)
    for noisy in ("urllib3", "google", "databricks", "thrift"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    settings = Settings()
    if args.cmd == "convert":
        from .landing import Landing
        from .sources import cfpb

        landing = Landing(settings)
        zip_path, as_of, _ = cfpb.download_bulk(settings, landing)
        out = landing.path("cfpb", "complaints", as_of, "parquet")
        n = cfpb.bulk_zip_to_parquet(zip_path, out)
        print(f"{out} rows={n}")
        return 0

    runner = Runner(settings, sink_names=_sinks(args.sinks, settings), run_id=args.run_id)
    _OPEN_SINKS.extend(runner.sinks)
    if args.cmd == "ensure-tables":
        for n in runner.ensure_tables():
            print(n)
        return 0
    if args.cmd == "run":
        sources = ["cfpb", "fdic", "fred"] if args.source == "all" else [args.source]
        failures = []
        for src in sources:
            try:
                getattr(runner, f"run_{src}")(mode=args.mode, start=args.start, end=args.end)
            except Exception as exc:  # noqa: BLE001
                logging.getLogger("cl-ingest").error("%s failed: %s", src, exc)
                failures.append(src)
        return 1 if failures else 0
    if args.cmd == "rehydrate":
        target = [s for s in runner.sinks if s.name == args.sink]
        if not target:
            print(f"sink {args.sink} not configured", file=sys.stderr)
            return 2
        for n in runner.rehydrate(target[0]):
            print(n)
        return 0
    if args.cmd == "status":
        print(runner.status())
        return 0
    if args.cmd == "parity-check":
        from .parity import parity_check

        failures = parity_check(settings, runner.sinks, runner.run_id)
        for f in failures:
            print("PARITY FAIL", f)
        return 1 if failures else 0
    if args.cmd == "evidence":
        from .evidence import write_evidence

        out_dir = Path(args.out_dir or os.environ.get("CL_EVIDENCE_DIR") or "evidence")
        print(write_evidence(runner.sinks, out_dir))
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
