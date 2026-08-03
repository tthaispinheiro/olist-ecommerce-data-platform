import os
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession


PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Permite importar módulos como src.jobs.gold_job nos testes
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture(scope="session")
def spark():
    if os.name == "nt":
        hadoop_home = PROJECT_ROOT / "tools" / "hadoop"
        hadoop_bin = hadoop_home / "bin"

        os.environ["HADOOP_HOME"] = str(hadoop_home)

        current_path = os.environ.get("PATH", "")
        os.environ["PATH"] = (
            f"{hadoop_bin}{os.pathsep}{current_path}"
        )

    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("OlistPipelineTests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    session.sparkContext.setLogLevel("ERROR")

    yield session

    session.stop()