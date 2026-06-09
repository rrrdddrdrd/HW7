from __future__ import annotations

from datetime import date, timedelta

from app.aggregator import run_aggregation
from app.postgres_writer import write_aggregates
from app.s3_exporter import export_to_s3


def main() -> None:
    today = date.today()
    for i in range(0, 8):
        target = today - timedelta(days=i)
        try:
            summary = run_aggregation(target)
            write_aggregates(target, summary["metrics"])
            try:
                export_to_s3(target)
            except Exception as exc:
                print(f"{target} s3_export_skipped: {exc}")
            print(f"{target} dau={summary['metrics'].get('dau')} "
                  f"started={summary['metrics'].get('started')} "
                  f"finished={summary['metrics'].get('finished')}")
        except Exception as exc:
            print(f"{target} ERROR: {exc}")


if __name__ == "__main__":
    main()
