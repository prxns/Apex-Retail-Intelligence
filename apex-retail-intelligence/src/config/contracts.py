"""Explicit source contracts and Silver type definitions."""
from pyspark.sql.types import StringType, StructField, StructType

SOURCE_COLUMNS = {
    "customer": [
        "customer_id", "age", "gender", "income_bracket", "loyalty_program",
        "membership_years", "churned", "marital_status", "number_of_children",
        "education_level", "occupation", "customer_zip_code", "customer_city", "customer_state",
    ],
    "product": [
        "product_id", "product_name", "product_brand", "product_category", "product_rating",
        "product_review_count", "product_stock", "product_return_rate", "product_size",
        "product_weight", "product_color", "product_material", "product_manufacture_date",
        "product_expiry_date", "product_shelf_life", "unit_price",
    ],
    "sales": [
        "transaction_id", "transaction_date", "customer_id", "product_id", "quantity", "unit_price",
        "discount_applied", "payment_method", "store_location", "transaction_hour", "day_of_week",
        "week_of_year", "month_of_year", "total_sales", "promotion_id", "promotion_type",
        "holiday_season", "season", "weekend",
    ],
}

# Customer incremental contains additional source-system SCD metadata. It is input only.
OPTIONAL_SOURCE_COLUMNS = {
    "customer": {"surrogate_key", "version", "effective_start_date", "effective_end_date", "is_current"},
    "product": {"last_updated"},
    "sales": set(),
}


def string_schema(entity: str) -> StructType:
    return StructType([StructField(c, StringType(), True) for c in SOURCE_COLUMNS[entity]])
