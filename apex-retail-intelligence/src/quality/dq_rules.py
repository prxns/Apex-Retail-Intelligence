"""Dataset-specific Silver cleansing rules."""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lower, lit, row_number, sha2, concat_ws, to_date, to_timestamp, trim, when
from pyspark.sql.window import Window


def _blank_to_null(df: DataFrame, columns):
    for c in columns:
        if c in df.columns:
            df = df.withColumn(c, when(trim(col(c)) == "", None).otherwise(col(c)))
    return df


def _fill_strings(df: DataFrame, columns):
    for c in columns:
        if c in df.columns:
            df = df.withColumn(c, when(col(c).isNull(), lit("Unknown")).otherwise(col(c)))
    return df


def _boolean(df: DataFrame, c: str):
    if c not in df.columns:
        return df
    return df.withColumn(
        c,
        when(lower(trim(col(c))).isin("true", "yes", "y", "1"), lit(True))
        .when(lower(trim(col(c))).isin("false", "no", "n", "0"), lit(False))
        .otherwise(None),
    )


def clean_customer(df: DataFrame) -> DataFrame:
    df = df.filter(col("customer_id").isNotNull())
    df = _blank_to_null(df, [c for c in df.columns if c != "customer_id"])
    # Incremental customer data contains two legitimate versions for changed keys;
    # version is used only to distinguish source change events, not copied into Silver.
    if "version" in df.columns:
        w = Window.partitionBy("customer_id", "version").orderBy(col("ingested_at").desc())
        df = df.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).drop("_rn")
    else:
        payload = [c for c in df.columns if c not in {"customer_id", "ingested_at", "load_type", "source_file", "batch_id"}]
        w = Window.partitionBy("customer_id").orderBy(sha2(concat_ws("|", *[col(c).cast("string") for c in payload]), 256).desc())
        df = df.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).drop("_rn")

    for c in ["age", "membership_years", "number_of_children"]:
        df = df.withColumn(c, col(c).cast("int"))
    df = _boolean(df, "churned")
    return _fill_strings(
        df,
        ["gender", "income_bracket", "loyalty_program", "marital_status", "education_level", "occupation", "customer_zip_code", "customer_city", "customer_state"],
    ).fillna({"age": 0, "membership_years": 0, "number_of_children": 0})


def clean_product(df: DataFrame) -> DataFrame:
    df = df.filter(col("product_id").isNotNull())
    df = _blank_to_null(df, [c for c in df.columns if c != "product_id"])
    order_col = to_date(col("last_updated")) if "last_updated" in df.columns else col("ingested_at")
    w = Window.partitionBy("product_id").orderBy(order_col.desc_nulls_last(), col("ingested_at").desc())
    df = df.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).drop("_rn")
    for c in ["product_rating", "product_review_count", "product_stock", "product_return_rate", "product_weight", "unit_price"]:
        df = df.withColumn(c, col(c).cast("double"))
    df = df.withColumn("product_shelf_life", col("product_shelf_life").cast("int"))
    for c in ["product_manufacture_date", "product_expiry_date"]:
        df = df.withColumn(c, to_timestamp(col(c)))
    return _fill_strings(df, ["product_name", "product_brand", "product_category", "product_size", "product_color", "product_material"]).fillna(
        {"product_rating": 0.0, "product_review_count": 0.0, "product_stock": 0.0, "product_return_rate": 0.0, "product_weight": 0.0, "unit_price": 0.0, "product_shelf_life": 0}
    )


def clean_sales(df: DataFrame) -> DataFrame:
    df = df.filter(col("transaction_id").isNotNull())
    df = _blank_to_null(df, [c for c in df.columns if c != "transaction_id"])
    payload = [c for c in df.columns if c not in {"transaction_id", "ingested_at", "load_type", "source_file", "batch_id"}]
    # Latest source instance wins; ingestion timestamp is the deterministic tie-breaker.
    w = Window.partitionBy("transaction_id").orderBy(col("ingested_at").desc())
    df = df.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).drop("_rn")
    df = df.withColumn("transaction_date", to_timestamp(col("transaction_date")))
    for c in ["quantity", "transaction_hour", "week_of_year", "month_of_year"]:
        df = df.withColumn(c, col(c).cast("int"))
    for c in ["unit_price", "discount_applied", "total_sales", "promotion_id"]:
        df = df.withColumn(c, col(c).cast("double"))
    for c in ["holiday_season", "weekend"]:
        df = _boolean(df, c)
    df = _fill_strings(df, ["payment_method", "store_location", "day_of_week", "season", "promotion_type"])
    return df.fillna({"quantity": 0, "transaction_hour": 0, "week_of_year": 0, "month_of_year": 0, "unit_price": 0.0, "discount_applied": 0.0, "total_sales": 0.0, "promotion_id": 0.0})
