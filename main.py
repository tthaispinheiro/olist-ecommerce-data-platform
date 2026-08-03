from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone

from src.common.config import load_config
from src.common.spark import create_spark_session
from src.jobs.bronze_job import run_bronze
from src.jobs.gold_job import run_gold
from src.jobs.silver_job import run_silver
from src.jobs.sql_server_job import run_sql_server_load


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | %(levelname)s | "
            "%(name)s | %(message)s"
        ),
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(
                "logs/pipeline.log",
                encoding="utf-8",
            ),
        ],
    )


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Nexora Commerce Data Platform"
    )

    parser.add_argument(
        "--stage",
        choices=["bronze", "silver", "gold", "load", "all"],
        default="all",
        help="Etapa do pipeline que será executada.",
    )

    return parser.parse_args()


def main() -> None:
    configure_logging()
    logger = logging.getLogger("olist_pipeline")
    args = parse_arguments()
    config = load_config()

    batch_id = datetime.now(timezone.utc).strftime(
        "%Y%m%dT%H%M%SZ"
    )

    spark = create_spark_session(config)

    logger.info(
        "Pipeline iniciado: stage=%s batch_id=%s",
        args.stage,
        batch_id,
    )

    try:
        if args.stage in ["bronze", "all"]:
            run_bronze(spark, config, batch_id)

        if args.stage in ["silver", "all"]:
            run_silver(spark, config)

        if args.stage in ["gold", "all"]:
            run_gold(spark, config)

        if args.stage in ["load", "all"]:
            run_sql_server_load(spark, config)

        logger.info(
            "Pipeline concluído: stage=%s batch_id=%s",
            args.stage,
            batch_id,
        )

    except Exception:
        logger.exception(
            "Pipeline finalizado com erro: batch_id=%s",
            batch_id,
        )
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()