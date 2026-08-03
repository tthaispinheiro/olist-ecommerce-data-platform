from __future__ import annotations

import os
from typing import Any

from pyspark.sql import SparkSession


def create_spark_session(
    config: dict[str, Any],
) -> SparkSession:
    """Cria a sessão Spark usada pelo pipeline local."""

    project_root = config["project_root"]

    # Configuração necessária para executar operações
    # de arquivos do Hadoop no Windows.
    if os.name == "nt":
        hadoop_home = project_root / "tools" / "hadoop"
        hadoop_bin = hadoop_home / "bin"
        winutils_path = hadoop_bin / "winutils.exe"

        if not winutils_path.exists():
            raise FileNotFoundError(
                "winutils.exe não encontrado. "
                f"Arquivo esperado em: {winutils_path}"
            )

        os.environ["HADOOP_HOME"] = str(hadoop_home.resolve())

        current_path = os.environ.get("PATH", "")
        os.environ["PATH"] = (
            f"{hadoop_bin.resolve()}{os.pathsep}{current_path}"
        )

    # Diretório temporário usado pelo Spark.
    spark_temp_path = project_root / "tmp" / "spark"
    spark_temp_path.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName(config["app"]["name"])
        .master(config["spark"]["master"])
        .config(
            "spark.sql.shuffle.partitions",
            str(config["spark"]["shuffle_partitions"]),
        )
        .config(
            "spark.local.dir",
            str(spark_temp_path.resolve()),
        )
        .config(
            "spark.sql.session.timeZone",
            "UTC",
        )
        .config(
            "spark.sql.parquet.compression.codec",
            "snappy",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark