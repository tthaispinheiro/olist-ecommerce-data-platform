
from __future__ import annotations

import logging
from typing import Any

from pyspark.sql import SparkSession

from src.common.config import (
    get_project_path,
    get_sql_server_options,
)


logger = logging.getLogger(__name__)


def run_sql_server_load(
    spark: SparkSession,
    config: dict[str, Any],
) -> None:
    """Carrega as tabelas Gold no SQL Server por JDBC."""
    gold_path = get_project_path(config, "gold")
    connection = get_sql_server_options()

    for gold_table, sql_table in config["sql_tables"].items():
        source_path = gold_path / gold_table

        dataframe = spark.read.parquet(str(source_path))

        logger.info(
            "Carregando %s em %s",
            gold_table,
            sql_table,
        )

        (
            dataframe
            .coalesce(2)
            .write
            .format("jdbc")
            .option("url", connection["url"])
            .option("dbtable", sql_table)
            .option("user", connection["user"])
            .option("password", connection["password"])
            .option("driver", connection["driver"])
            .option("batchsize", "5000")
            .option("isolationLevel", "READ_COMMITTED")
            .option("truncate", "true")
            .mode("overwrite")
            .save()
        )

        logger.info(
            "Carga SQL concluída: tabela=%s registros=%s",
            sql_table,
            dataframe.count(),
        )