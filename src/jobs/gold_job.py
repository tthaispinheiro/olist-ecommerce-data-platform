from __future__ import annotations

import logging
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import get_project_path
from src.common.quality import assert_not_null, assert_unique


logger = logging.getLogger(__name__)


def hash_key(*columns: str):
    """Cria uma chave técnica determinística."""
    return F.sha2(
        F.concat_ws(
            "|",
            *[F.coalesce(F.col(column).cast("string"), F.lit("")) for column in columns],
        ),
        256,
    )


def build_dim_customer(customers: DataFrame) -> DataFrame:
    return customers.select(
        hash_key("customer_id").alias("customer_key"),
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
    )


def build_dim_product(products: DataFrame) -> DataFrame:
    return products.select(
        hash_key("product_id").alias("product_key"),
        "product_id",
        "product_category",
        "product_category_name",
        "product_name_length",
        "product_description_length",
        "product_photos_quantity",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    )


def build_dim_seller(sellers: DataFrame) -> DataFrame:
    return sellers.select(
        hash_key("seller_id").alias("seller_key"),
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state",
    )


def build_dim_date(
    spark: SparkSession,
    orders: DataFrame,
) -> DataFrame:
    bounds = (
        orders.select(
            F.min(F.to_date("order_purchase_timestamp")).alias("min_date"),
            F.max(F.to_date("order_purchase_timestamp")).alias("max_date"),
        )
        .first()
    )

    min_date = bounds["min_date"]
    max_date = bounds["max_date"]

    if min_date is None or max_date is None:
        raise ValueError("Não foi possível determinar o período dos pedidos.")

    date_dataframe = spark.sql(
        f"""
        SELECT explode(
            sequence(
                to_date('{min_date}'),
                to_date('{max_date}'),
                interval 1 day
            )
        ) AS full_date
        """
    )

    return date_dataframe.select(
        F.date_format("full_date", "yyyyMMdd")
        .cast("integer")
        .alias("date_key"),
        "full_date",
        F.year("full_date").alias("year"),
        F.quarter("full_date").alias("quarter"),
        F.month("full_date").alias("month"),
        F.date_format("full_date", "MMMM").alias("month_name"),
        F.weekofyear("full_date").alias("week_of_year"),
        F.dayofmonth("full_date").alias("day_of_month"),
        F.dayofweek("full_date").alias("day_of_week"),
        F.date_format("full_date", "EEEE").alias("day_name"),
    )


def build_fact_orders(
    orders: DataFrame,
    customers: DataFrame,
    order_items: DataFrame,
    payments: DataFrame,
) -> DataFrame:
    items_aggregated = order_items.groupBy("order_id").agg(
        F.count("*").alias("item_count"),
        F.countDistinct("product_id").alias("distinct_product_count"),
        F.countDistinct("seller_id").alias("distinct_seller_count"),
        F.round(F.sum("price"), 2).alias("gross_merchandise_value"),
        F.round(F.sum("freight_value"), 2).alias("freight_value"),
        F.round(
            F.sum(F.col("price") + F.col("freight_value")),
            2,
        ).alias("calculated_order_value"),
    )

    payments_aggregated = payments.groupBy("order_id").agg(
        F.round(F.sum("payment_value"), 2).alias("payment_value"),
        F.max("payment_installments").alias("maximum_installments"),
        F.concat_ws(
            ",",
            F.sort_array(F.collect_set("payment_type")),
        ).alias("payment_types"),
    )

    dataframe = (
        orders
        .join(customers, on="customer_id", how="left")
        .join(items_aggregated, on="order_id", how="left")
        .join(payments_aggregated, on="order_id", how="left")
        .withColumn("order_key", hash_key("order_id"))
        .withColumn("customer_key", hash_key("customer_id"))
        .withColumn(
            "purchase_date",
            F.to_date("order_purchase_timestamp"),
        )
        .withColumn(
            "purchase_date_key",
            F.date_format("order_purchase_timestamp", "yyyyMMdd")
            .cast("integer"),
        )
        .withColumn(
            "delivery_days",
            F.when(
                F.col("order_delivered_customer_date").isNotNull(),
                F.datediff(
                    "order_delivered_customer_date",
                    "order_purchase_timestamp",
                ),
            ),
        )
        .withColumn(
            "delivery_variance_days",
            F.when(
                F.col("order_delivered_customer_date").isNotNull(),
                F.datediff(
                    "order_delivered_customer_date",
                    "order_estimated_delivery_date",
                ),
            ),
        )
        .withColumn(
            "late_days",
            F.when(
                F.col("delivery_variance_days").isNotNull(),
                F.greatest(
                    F.col("delivery_variance_days"),
                    F.lit(0),
                ),
            ),
        )
        .withColumn(
            "is_late",
            F.when(
                F.col("delivery_variance_days").isNull(),
                F.lit(None).cast("integer"),
            )
            .when(F.col("delivery_variance_days") > 0, F.lit(1))
            .otherwise(F.lit(0)),
        )
        .withColumn(
            "order_value_difference",
            F.round(
                F.col("payment_value")
                - F.col("calculated_order_value"),
                2,
            ),
        )
    )

    return dataframe.select(
        "order_key",
        "order_id",
        "customer_key",
        "purchase_date_key",
        "purchase_date",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "item_count",
        "distinct_product_count",
        "distinct_seller_count",
        "gross_merchandise_value",
        "freight_value",
        "calculated_order_value",
        "payment_value",
        "maximum_installments",
        "payment_types",
        "delivery_days",
        "delivery_variance_days",
        "late_days",
        "is_late",
        "order_value_difference",
        "customer_city",
        "customer_state",
    )


def build_fact_order_items(
    order_items: DataFrame,
    orders: DataFrame,
) -> DataFrame:
    order_attributes = orders.select(
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
    )

    return (
        order_items
        .join(order_attributes, on="order_id", how="inner")
        .withColumn(
            "order_item_key",
            hash_key("order_id", "order_item_id"),
        )
        .withColumn("order_key", hash_key("order_id"))
        .withColumn("customer_key", hash_key("customer_id"))
        .withColumn("product_key", hash_key("product_id"))
        .withColumn("seller_key", hash_key("seller_id"))
        .withColumn(
            "purchase_date_key",
            F.date_format("order_purchase_timestamp", "yyyyMMdd")
            .cast("integer"),
        )
        .withColumn(
            "item_total_value",
            F.round(F.col("price") + F.col("freight_value"), 2),
        )
        .select(
            "order_item_key",
            "order_key",
            "order_id",
            "order_item_id",
            "customer_key",
            "product_key",
            "seller_key",
            "purchase_date_key",
            "order_status",
            "shipping_limit_date",
            "price",
            "freight_value",
            "item_total_value",
        )
    )


def build_daily_sales(fact_orders: DataFrame) -> DataFrame:
    valid_orders = fact_orders.filter(
        ~F.col("order_status").isin("canceled", "unavailable")
    )

    return (
        valid_orders
        .groupBy("purchase_date_key", "purchase_date")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.sum("item_count").alias("item_count"),
            F.round(
                F.sum("gross_merchandise_value"),
                2,
            ).alias("gross_merchandise_value"),
            F.round(F.sum("freight_value"), 2).alias("freight_value"),
            F.round(
                F.sum("calculated_order_value"),
                2,
            ).alias("total_order_value"),
            F.round(
                F.avg("calculated_order_value"),
                2,
            ).alias("average_order_value"),
            F.round(
                F.avg("delivery_days"),
                2,
            ).alias("average_delivery_days"),
            F.round(
                F.avg("is_late") * 100,
                2,
            ).alias("late_delivery_percentage"),
        )
    )


def build_seller_performance(
    fact_order_items: DataFrame,
    fact_orders: DataFrame,
    dim_seller: DataFrame,
) -> DataFrame:
    order_delivery = fact_orders.select(
        "order_key",
        "is_late",
        "delivery_days",
    )

    return (
        fact_order_items
        .join(order_delivery, on="order_key", how="left")
        .join(dim_seller, on="seller_key", how="left")
        .filter(
            ~F.col("order_status").isin("canceled", "unavailable")
        )
        .groupBy(
            "seller_key",
            "seller_id",
            "seller_city",
            "seller_state",
        )
        .agg(
            F.countDistinct("order_key").alias("order_count"),
            F.count("*").alias("item_count"),
            F.round(F.sum("price"), 2).alias("product_revenue"),
            F.round(
                F.sum("freight_value"),
                2,
            ).alias("freight_value"),
            F.round(
                F.avg("delivery_days"),
                2,
            ).alias("average_delivery_days"),
            F.round(
                F.avg("is_late") * 100,
                2,
            ).alias("late_delivery_percentage"),
        )
    )


def run_gold(
    spark: SparkSession,
    config: dict[str, Any],
) -> None:
    """Constrói dimensões, fatos e data marts."""
    silver_path = get_project_path(config, "silver")
    gold_path = get_project_path(config, "gold")

    def read_silver(table_name: str) -> DataFrame:
        return spark.read.parquet(str(silver_path / table_name))

    customers = read_silver("customers")
    orders = read_silver("orders")
    order_items = read_silver("order_items")
    payments = read_silver("payments")
    products = read_silver("products")
    sellers = read_silver("sellers")

    dim_customer = build_dim_customer(customers)
    dim_product = build_dim_product(products)
    dim_seller = build_dim_seller(sellers)
    dim_date = build_dim_date(spark, orders)

    fact_orders = build_fact_orders(
        orders,
        customers,
        order_items,
        payments,
    )

    fact_order_items = build_fact_order_items(
        order_items,
        orders,
    )

    daily_sales = build_daily_sales(fact_orders)

    seller_performance = build_seller_performance(
        fact_order_items,
        fact_orders,
        dim_seller,
    )

    assert_not_null(
        fact_orders,
        ["order_key", "order_id", "customer_key"],
        "fact_orders",
    )
    assert_unique(fact_orders, ["order_key"], "fact_orders")
    assert_unique(
        fact_order_items,
        ["order_item_key"],
        "fact_order_items",
    )

    tables = {
        "dim_customer": dim_customer,
        "dim_product": dim_product,
        "dim_seller": dim_seller,
        "dim_date": dim_date,
        "fact_orders": fact_orders,
        "fact_order_items": fact_order_items,
        "daily_sales": daily_sales,
        "seller_performance": seller_performance,
    }

    for table_name, dataframe in tables.items():
        destination = gold_path / table_name

        (
            dataframe.write
            .mode("overwrite")
            .parquet(str(destination))
        )

        logger.info(
            "Gold concluída: tabela=%s registros=%s",
            table_name,
            dataframe.count(),
        )