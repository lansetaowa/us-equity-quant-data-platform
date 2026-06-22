"""Legacy Week 3 demo pipeline.

This module is retained for historical reference and is not the current
production daily-price workflow.
"""

from __future__ import annotations

import traceback
import uuid
from datetime import date
from pathlib import Path

import yaml

from scripts.legacy.data_quality_checks import (
    run_price_quality_checks,
)
from scripts.legacy.ingest_tiingo_prices import (
    ingest_tiingo_prices,
)
from scripts.legacy.metadata_utils import (
    log_pipeline_failed,
    log_pipeline_started,
    log_pipeline_success,
)
from scripts.legacy.pipeline_state import (
    compute_refresh_window,
    get_last_successful_data_end_date,
)
from scripts.legacy.transform_tiingo_prices_to_dwd import (
    transform_tiingo_prices_to_dwd,
)
from scripts.sync_data_to_gcs import sync_data_to_gcs


UNIVERSE_CONFIG_PATH = Path("configs/legacy/universe.yml")
PIPELINE_CONFIG_PATH = Path("configs/legacy/pipeline.yml")
PIPELINE_NAME = "daily_price_pipeline"
PARQUET_GLOB = "data/dwd/equity_price_daily/**/part-*.parquet"


def load_yaml(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def main() -> None:
    run_id = str(uuid.uuid4())

    universe_config = load_yaml(UNIVERSE_CONFIG_PATH)
    pipeline_config = load_yaml(PIPELINE_CONFIG_PATH)["price_pipeline"]

    symbols = universe_config["symbols"]

    source = pipeline_config["source"]
    dataset = pipeline_config["dataset"]
    mode = pipeline_config["mode"]
    backfill_start_date = pipeline_config["backfill_start_date"]
    default_lookback_days = int(pipeline_config["default_lookback_days"])
    sync_to_gcs = bool(pipeline_config["sync_to_gcs"])

    last_successful_data_end_date = get_last_successful_data_end_date(
        pipeline_name=PIPELINE_NAME
    )

    data_start_date, data_end_date = compute_refresh_window(
        mode=mode,
        backfill_start_date=backfill_start_date,
        default_lookback_days=default_lookback_days,
        last_successful_data_end_date=last_successful_data_end_date,
        today=date.today(),
    )

    print("=" * 80)
    print(f"Pipeline run_id: {run_id}")
    print(f"Mode: {mode}")
    print(f"Source: {source}")
    print(f"Dataset: {dataset}")
    print(f"Symbols: {len(symbols)}")
    print(f"Window: {data_start_date} → {data_end_date}")
    print("=" * 80)

    log_pipeline_started(
        run_id=run_id,
        pipeline_name=PIPELINE_NAME,
        source=source,
        dataset=dataset,
        mode=mode,
        data_start_date=data_start_date,
        data_end_date=data_end_date,
        symbols_count=len(symbols),
        notes="Daily price pipeline started.",
    )

    try:
        ingestion_result = ingest_tiingo_prices(
            symbols=symbols,
            start_date=str(data_start_date),
            end_date=str(data_end_date),
            run_id=run_id,
        )

        transform_result = transform_tiingo_prices_to_dwd(
            symbols=symbols,
            load_id=run_id,
        )

        quality_result = run_price_quality_checks(
            parquet_glob=PARQUET_GLOB,
            expected_symbols=symbols,
        )

        if sync_to_gcs:
            sync_data_to_gcs()

        log_pipeline_success(
            run_id=run_id,
            row_count=transform_result["records_written"],
            ods_records=ingestion_result["records_written"],
            dwd_records=transform_result["records_written"],
            notes=(
                "Daily price pipeline completed. "
                f"Quality result: {quality_result}"
            ),
        )

        print("=" * 80)
        print("Pipeline completed successfully.")
        print(f"Ingestion result: {ingestion_result}")
        print(f"Transform result: {transform_result}")
        print(f"Quality result: {quality_result}")
        print("=" * 80)

    except Exception as exc:
        error_message = traceback.format_exc()

        log_pipeline_failed(
            run_id=run_id,
            error_message=error_message,
            notes=f"Daily price pipeline failed: {exc}",
        )

        print(error_message)
        raise


if __name__ == "__main__":
    main()