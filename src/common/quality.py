from __future__ import annotations

from collections.abc import Iterable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def assert_not_null(
    dataframe: DataFrame,
    columns: Iterable[str],
    table_name: str,
) -> None:
    """Falha o pipeline quando chaves obrigatórias possuem valores nulos."""
    expressions = [
        F.sum(
            F.when(F.col(column).isNull(), 1).otherwise(0)
        ).alias(column)
        for column in columns
    ]

    result = dataframe.select(expressions).first().asDict()
    invalid = {column: count for column, count in result.items() if count}

    if invalid:
        raise ValueError(
            f"{table_name}: valores nulos encontrados: {invalid}"
        )


def assert_unique(
    dataframe: DataFrame,
    keys: list[str],
    table_name: str,
) -> None:
    """Verifica se a chave definida é única."""
    total_rows = dataframe.count()
    unique_rows = dataframe.dropDuplicates(keys).count()

    if total_rows != unique_rows:
        raise ValueError(
            f"{table_name}: duplicidades encontradas nas chaves {keys}. "
            f"Total={total_rows}, únicos={unique_rows}"
        )


def assert_non_negative(
    dataframe: DataFrame,
    columns: Iterable[str],
    table_name: str,
) -> None:
    """Verifica se campos monetários ou quantitativos são não negativos."""
    condition = None

    for column in columns:
        current_condition = F.col(column) < 0
        condition = (
            current_condition
            if condition is None
            else condition | current_condition
        )

    invalid_count = dataframe.filter(condition).count()

    if invalid_count:
        raise ValueError(
            f"{table_name}: encontrados {invalid_count} registros negativos."
        )