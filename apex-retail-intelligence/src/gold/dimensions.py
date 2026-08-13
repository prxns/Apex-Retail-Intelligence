"""Gold star-schema builders and Unity Catalog registration."""
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, date_format, dayofmonth, dayofweek, lit, month, quarter, row_number, to_date, weekofyear, year, when
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    DateType,
    IntegerType,
    StringType,
    BooleanType,
)

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

    dates = (
        sales.select(to_date(col("transaction_date")).alias("date"))
        .where(col("date").isNotNull())
        .distinct()
    )

    dates = (
        dates
        .withColumn("date_sk", date_format(col("date"), "yyyyMMdd").cast("int"))
        .withColumn("day", dayofmonth(col("date")))
        .withColumn("day_name", date_format(col("date"), "EEEE"))
        .withColumn("day_of_week", dayofweek(col("date")))
        .withColumn("week_of_year", weekofyear(col("date")))
        .withColumn("month", month(col("date")))
        .withColumn("month_name", date_format(col("date"), "MMMM"))
        .withColumn("quarter", quarter(col("date")))
        .withColumn("year", year(col("date")))
        .withColumn("weekend", dayofweek(col("date")).isin([1, 7]))
    )

    unknown_schema = StructType([
        StructField("date", DateType(), True),
        StructField("date_sk", IntegerType(), True),
        StructField("day", IntegerType(), True),
        StructField("day_name", StringType(), True),
        StructField("day_of_week", IntegerType(), True),
        StructField("week_of_year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("month_name", StringType(), True),
        StructField("quarter", IntegerType(), True),
        StructField("year", IntegerType(), True),
        StructField("weekend", BooleanType(), True),
    ])

    unknown = spark.createDataFrame(
        [(None, 0, 0, "Unknown", 0, 0, 0, "Unknown", 0, 0, False)],
        schema=unknown_schema,
    )

    dates = dates.unionByName(unknown)

    dates.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(f"{gold_path}/dim_date")

    return dates

def build_fact_sales(spark, silver_sales_path: str, gold_path: str) -> DataFrame:
    sales = spark.read.format("delta").load(silver_sales_path)

    cust = (
        spark.read.format("delta")
        .load(f"{gold_path}/dim_customer")
        .select(
            "customer_id",
            "customer_sk",
            "effective_start_date",
            "effective_end_date",
        )
    )

    prod = (
        spark.read.format("delta")
        .load(f"{gold_path}/dim_product")
        .select(
            "product_id",
            "product_sk",
        )
    )

    promo = (
        spark.read.format("delta")
        .load(f"{gold_path}/dim_promotion")
        .select(
            "promotion_id",
            "promotion_sk",
        )
    )

    fact = (
        sales
        .join(
            cust,
            (sales.customer_id == cust.customer_id)
            & (
                to_date(sales.transaction_date)
                >= cust.effective_start_date
            )
            & (
                (
                    to_date(sales.transaction_date)
                    <= cust.effective_end_date
                )
                | cust.effective_end_date.isNull()
            ),
            "left",
        )
        .join(
            prod,
            sales.product_id == prod.product_id,
            "left",
        )
        .join(
            promo,
            sales.promotion_id == promo.promotion_id,
            "left",
        )
    )

    fact = fact.select(
        sales.sales_sk,
        sales.transaction_id,

        when(
            cust.customer_sk.isNull(),
            lit(UNKNOWN_SK)
        ).otherwise(
            cust.customer_sk
        ).alias("customer_sk"),

        when(
            prod.product_sk.isNull(),
            lit(UNKNOWN_SK)
        ).otherwise(
            prod.product_sk
        ).alias("product_sk"),

        when(
            promo.promotion_sk.isNull(),
            lit(UNKNOWN_SK)
        ).otherwise(
            promo.promotion_sk
        ).alias("promotion_sk"),

        when(
            sales.transaction_date.isNull(),
            lit(0)
        ).otherwise(
            date_format(
                sales.transaction_date,
                "yyyyMMdd"
            ).cast("int")
        ).alias("date_sk"),

        sales.quantity,
        sales.unit_price,
        sales.discount_applied,
        sales.total_sales,
        sales.store_location,
        sales.transaction_hour,
        sales.day_of_week,
        sales.promotion_type,
    )

    fact.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(f"{gold_path}/fact_sales")

    return fact

def register_gold_tables(spark, catalog: str, schema: str, gold_path: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    for table_name in ["dim_customer", "dim_product", "dim_promotion", "dim_date", "fact_sales"]:
        location = f"dbfs:{gold_path}/{table_name}"
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS `{catalog}`.`{schema}`.`{table_name}` USING DELTA LOCATION '{location}'"
        )
