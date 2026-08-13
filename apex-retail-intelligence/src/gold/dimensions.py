"""Gold star-schema builders and Unity Catalog registration."""
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, date_format, dayofmonth, dayofweek, lit, month, quarter, row_number, to_date, weekofyear, year, when
from pyspark.sql.window import Window

UNKNOWN_SK = 0


def build_dim_customer(spark, silver_path: str, gold_path: str) -> DataFrame:
    df = spark.read.format("delta").load(silver_path)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{gold_path}/dim_customer")
    return df


def build_dim_product(spark, silver_path: str, gold_path: str) -> DataFrame:
    df = spark.read.format("delta").load(silver_path)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{gold_path}/dim_product")
    return df


def build_dim_promotion(spark, silver_sales_path: str, gold_path: str) -> DataFrame:
    sales = spark.read.format("delta").load(silver_sales_path)
    known = (
        sales.select("promotion_id", "promotion_type")
        .withColumn("promotion_id", col("promotion_id").cast("long"))
        .dropDuplicates(["promotion_id", "promotion_type"])
        .filter(col("promotion_id").isNotNull() & (col("promotion_id") != 0))
        .withColumn("promotion_sk", row_number().over(Window.orderBy(col("promotion_id"))).cast("long"))
    )
    unknown = spark.createDataFrame([(0, "Unknown", UNKNOWN_SK)], ["promotion_id", "promotion_type", "promotion_sk"])
    promos = known.unionByName(unknown)
    promos.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{gold_path}/dim_promotion")
    return promos


def build_dim_date(spark, silver_sales_path: str, gold_path: str) -> DataFrame:
    sales = spark.read.format("delta").load(silver_sales_path)
    dates = sales.select(to_date(col("transaction_date")).alias("date")).where(col("date").isNotNull()).distinct()
    dates = dates.withColumn("date_sk", date_format(col("date"), "yyyyMMdd").cast("int"))
    dates = dates.withColumn("day", dayofmonth(col("date"))) \
        .withColumn("day_of_week", dayofweek(col("date"))) \
        .withColumn("week_of_year", weekofyear(col("date"))) \
        .withColumn("month", month(col("date"))) \
        .withColumn("month_name", date_format(col("date"), "MMMM")) \
        .withColumn("quarter", quarter(col("date"))) \
        .withColumn("year", year(col("date"))) \
        .withColumn("weekend", dayofweek(col("date")).isin([1, 7]))
    unknown = spark.createDataFrame([(None, 0, 0, "Unknown", 0, 0, "Unknown", 0, 0, False)], ["date", "date_sk", "day", "day_name", "day_of_week", "week_of_year", "month_name", "month", "quarter", "weekend"])
    dates = dates.withColumn("day_name", date_format(col("date"), "EEEE")).select(unknown.columns).unionByName(unknown)
    dates.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{gold_path}/dim_date")
    return dates


def build_fact_sales(spark, silver_sales_path: str, gold_path: str) -> DataFrame:
    sales = spark.read.format("delta").load(silver_sales_path).alias("s")
    cust = spark.read.format("delta").load(f"{gold_path}/dim_customer").alias("c")
    prod = spark.read.format("delta").load(f"{gold_path}/dim_product").alias("p")
    promo = spark.read.format("delta").load(f"{gold_path}/dim_promotion").alias("pr")

    # Point-in-time customer lookup. Historical customers begin at a sentinel date;
    # incremental source effective_start_date is used only as the change-event date by Silver.
    fact = (
        sales.join(cust, (col("s.customer_id") == col("c.customer_id")) &
                   (to_date(col("s.transaction_date")) >= col("c.effective_start_date")) &
                   (to_date(col("s.transaction_date")) <= col("c.effective_end_date") | col("c.effective_end_date").isNull()), "left")
        .join(prod, col("s.product_id") == col("p.product_id"), "left")
        .join(promo, (col("s.promotion_id") == col("pr.promotion_id")) & (col("s.promotion_id").isNotNull()), "left")
    )
    fact = fact.select(
        col("s.sales_sk"), col("s.transaction_id"),
        when(col("c.customer_sk").isNull(), lit(UNKNOWN_SK)).otherwise(col("c.customer_sk")).alias("customer_sk"),
        when(col("p.product_sk").isNull(), lit(UNKNOWN_SK)).otherwise(col("p.product_sk")).alias("product_sk"),
        when(col("pr.promotion_sk").isNull(), lit(UNKNOWN_SK)).otherwise(col("pr.promotion_sk")).alias("promotion_sk"),
        when(col("s.transaction_date").isNull(), lit(0)).otherwise(date_format(col("s.transaction_date"), "yyyyMMdd").cast("int")).alias("date_sk"),
        col("s.quantity"), col("s.unit_price"), col("s.discount_applied"), col("s.total_sales"),
        col("s.store_location"), col("s.transaction_hour"), col("s.day_of_week"), col("s.promotion_type"),
    )
    fact.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{gold_path}/fact_sales")
    return fact


def register_gold_tables(spark, catalog: str, schema: str, gold_path: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    for table_name in ["dim_customer", "dim_product", "dim_promotion", "dim_date", "fact_sales"]:
        location = f"{gold_path}/{table_name}"
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS `{catalog}`.`{schema}`.`{table_name}` USING DELTA LOCATION '{location}'"
        )
