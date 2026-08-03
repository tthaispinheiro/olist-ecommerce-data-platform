from __future__ import annotations

import logging
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import get_project_path
from src.common.quality import (
    assert_non_negative,
    assert_not_null,
    assert_unique,
)


logger = logging.getLogger(__name__)


def clean_customers(dataframe: DataFrame) -> DataFrame:
    return (
        dataframe.select(
            F.trim("customer_id").alias("customer_id"),
            F.trim("customer_unique_id").alias("customer_unique_id"),
            F.col("customer_zip_code_prefix")
            .cast("integer")
            .alias("customer_zip_code_prefix"),
            F.initcap(F.trim("customer_city")).alias("customer_city"),
            F.upper(F.trim("customer_state")).alias("customer_state"),
        )
        .filter(F.col("customer_id").isNotNull())
        .dropDuplicates(["customer_id"])
    )


def clean_orders(dataframe: DataFrame) -> DataFrame:
    timestamp_columns = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]

    selected = dataframe.select(
        F.trim("order_id").alias("order_id"),
        F.trim("customer_id").alias("customer_id"),
        F.lower(F.trim("order_status")).alias("order_status"),
        *[
            F.to_timestamp(F.col(column)).alias(column)
            for column in timestamp_columns
        ],
    )

    valid_statuses = [
        "created",
        "approved",
        "invoiced",
        "processing",
        "shipped",
        "delivered",
        "unavailable",
        "canceled",
    ]

    return (
        selected
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("order_status").isin(valid_statuses))
        .dropDuplicates(["order_id"])
    )


def clean_order_items(dataframe: DataFrame) -> DataFrame:
    return (
        dataframe.select(
            F.trim("order_id").alias("order_id"),
            F.col("order_item_id").cast("integer").alias("order_item_id"),
            F.trim("product_id").alias("product_id"),
            F.trim("seller_id").alias("seller_id"),
            F.to_timestamp("shipping_limit_date")
            .alias("shipping_limit_date"),
            F.col("price").cast("decimal(18, 2)").alias("price"),
            F.col("freight_value")
            .cast("decimal(18, 2)")
            .alias("freight_value"),
        )
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("order_item_id").isNotNull())
        .dropDuplicates(["order_id", "order_item_id"])
    )


def clean_payments(dataframe: DataFrame) -> DataFrame:
    return (
        dataframe.select(
            F.trim("order_id").alias("order_id"),
            F.col("payment_sequential")
            .cast("integer")
            .alias("payment_sequential"),
            F.lower(F.trim("payment_type")).alias("payment_type"),
            F.col("payment_installments")
            .cast("integer")
            .alias("payment_installments"),
            F.col("payment_value")
            .cast("decimal(18, 2)")
            .alias("payment_value"),
        )
        .filter(F.col("order_id").isNotNull())
        .dropDuplicates(["order_id", "payment_sequential"])
    )


def clean_products(
    products: DataFrame,
    translations: DataFrame,
) -> DataFrame:
    translation_clean = translations.select(
        F.trim("product_category_name").alias("product_category_name"),
        F.trim("product_category_name_english")
        .alias("product_category_name_english"),
    )

    product_clean = products.select(
        F.trim("product_id").alias("product_id"),
        F.trim("product_category_name").alias("product_category_name"),
        F.col("product_name_lenght")
        .cast("integer")
        .alias("product_name_length"),
        F.col("product_description_lenght")
        .cast("integer")
        .alias("product_description_length"),
        F.col("product_photos_qty")
        .cast("integer")
        .alias("product_photos_quantity"),
        F.col("product_weight_g")
        .cast("decimal(18, 2)")
        .alias("product_weight_g"),
        F.col("product_length_cm")
        .cast("decimal(18, 2)")
        .alias("product_length_cm"),
        F.col("product_height_cm")
        .cast("decimal(18, 2)")
        .alias("product_height_cm"),
        F.col("product_width_cm")
        .cast("decimal(18, 2)")
        .alias("product_width_cm"),
    )

    return (
        product_clean
        .join(
            translation_clean,
            on="product_category_name",
            how="left",
        )
        .withColumn(
            "product_category",
            F.coalesce(
                F.col("product_category_name_english"),
                F.col("product_category_name"),
                F.lit("unknown"),
            ),
        )
        .drop("product_category_name_english")
        .filter(F.col("product_id").isNotNull())
        .dropDuplicates(["product_id"])
    )


def clean_sellers(dataframe: DataFrame) -> DataFrame:
    return (
        dataframe.select(
            F.trim("seller_id").alias("seller_id"),
            F.col("seller_zip_code_prefix")
            .cast("integer")
            .alias("seller_zip_code_prefix"),
            F.initcap(F.trim("seller_city")).alias("seller_city"),
            F.upper(F.trim("seller_state")).alias("seller_state"),
        )
        .filter(F.col("seller_id").isNotNull())
        .dropDuplicates(["seller_id"])
    )


def run_silver(
    spark: SparkSession,
    config: dict[str, Any],
) -> None:
    """Limpa, tipa e valida os dados da camada Bronze."""
    bronze_path = get_project_path(config, "bronze")
    silver_path = get_project_path(config, "silver")

    def read_bronze(table_name: str) -> DataFrame:
        return spark.read.parquet(str(bronze_path / table_name))

    customers = clean_customers(read_bronze("customers"))
    orders = clean_orders(read_bronze("orders"))
    order_items = clean_order_items(read_bronze("order_items"))
    payments = clean_payments(read_bronze("payments"))
    products = clean_products(
        read_bronze("products"),
        read_bronze("category_translation"),
    )
    sellers = clean_sellers(read_bronze("sellers"))

    assert_not_null(customers, ["customer_id"], "customers")
    assert_not_null(orders, ["order_id", "customer_id"], "orders")
    assert_not_null(
        order_items,
        ["order_id", "order_item_id", "product_id", "seller_id"],
        "order_items",
    )
    assert_not_null(products, ["product_id"], "products")
    assert_not_null(sellers, ["seller_id"], "sellers")

    assert_unique(customers, ["customer_id"], "customers")
    assert_unique(orders, ["order_id"], "orders")
    assert_unique(
        order_items,
        ["order_id", "order_item_id"],
        "order_items",
    )
    assert_unique(products, ["product_id"], "products")
    assert_unique(sellers, ["seller_id"], "sellers")

    assert_non_negative(
        order_items,
        ["price", "freight_value"],
        "order_items",
    )
    assert_non_negative(payments, ["payment_value"], "payments")

    tables = {
        "customers": customers,
        "orders": orders,
        "order_items": order_items,
        "payments": payments,
        "products": products,
        "sellers": sellers,
    }

    for table_name, dataframe in tables.items():
        destination = silver_path / table_name

        (
            dataframe.write
            .mode("overwrite")
            .parquet(str(destination))
        )

        logger.info(
            "Silver concluída: tabela=%s registros=%s",
            table_name,
            dataframe.count(),
        )