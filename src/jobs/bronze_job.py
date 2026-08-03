
from __future__ import annotations

import logging
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.config import get_project_path


logger = logging.getLogger(__name__)


def run_bronze(
    spark: SparkSession,
    config: dict[str, Any],
    batch_id: str,
) -> None:
    """Ingere os CSVs originais e grava a camada Bronze em Parquet."""
    raw_path = get_project_path(config, "raw")
    bronze_path = get_project_path(config, "bronze")

    for table_name, filename in config["sources"].items():
        source_path = raw_path / filename
        destination_path = bronze_path / table_name

        if not source_path.exists():
            raise FileNotFoundError(
                f"Arquivo de origem não encontrado: {source_path}"
            )

        logger.info("Ingerindo %s", source_path)

        dataframe = (
            spark.read
            .option("header", True)
            .option("encoding", "UTF-8")
            .option("mode", "PERMISSIVE")
            .csv(str(source_path))
            .withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_ingestion_timestamp", F.current_timestamp())
            .withColumn("_source_file", F.input_file_name())
            .withColumn("_source_table", F.lit(table_name))
        )

        (
            dataframe.write
            .mode("overwrite")
            .format("parquet")
            .save(str(destination_path))
        )

        logger.info(
            "Bronze concluída: tabela=%s registros=%s",
            table_name,
            dataframe.count(),
        )