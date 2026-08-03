from datetime import datetime

from src.jobs.gold_job import build_fact_orders


def test_build_fact_orders_calculates_values_and_delay(spark):
    orders = spark.createDataFrame(
        [
            (
                "order-1",
                "customer-1",
                "delivered",
                datetime(2024, 1, 1, 10, 0),
                datetime(2024, 1, 1, 11, 0),
                datetime(2024, 1, 2, 10, 0),
                datetime(2024, 1, 12, 10, 0),
                datetime(2024, 1, 10, 10, 0),
            )
        ],
        [
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
    )

    customers = spark.createDataFrame(
        [
            (
                "customer-1",
                "unique-customer-1",
                10000,
                "São Paulo",
                "SP",
            )
        ],
        [
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        ],
    )

    order_items = spark.createDataFrame(
        [
            ("order-1", 1, "product-1", "seller-1", 100.0, 10.0),
            ("order-1", 2, "product-2", "seller-1", 50.0, 5.0),
        ],
        [
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "price",
            "freight_value",
        ],
    )

    payments = spark.createDataFrame(
        [
            ("order-1", 1, "credit_card", 2, 165.0)
        ],
        [
            "order_id",
            "payment_sequential",
            "payment_type",
            "payment_installments",
            "payment_value",
        ],
    )

    result = build_fact_orders(
        orders,
        customers,
        order_items,
        payments,
    ).first()

    assert result.item_count == 2
    assert float(result.gross_merchandise_value) == 150.0
    assert float(result.freight_value) == 15.0
    assert float(result.calculated_order_value) == 165.0
    assert result.delivery_variance_days == 2
    assert result.late_days == 2
    assert result.is_late == 1