"""Five mandatory Apex Retail Intelligence KPIs."""
from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, count, max as spark_max, round, sum as spark_sum, when


def net_margin_by_region(fact: DataFrame) -> DataFrame:
    return (
        fact.groupBy("store_location")
        .agg(spark_sum("total_sales").alias("gross_revenue"), spark_sum("discount_applied").alias("discount_amount"), count("transaction_id").alias("transaction_count"))
        .withColumn("net_margin", col("gross_revenue") - col("discount_amount"))
        .orderBy(col("net_margin").desc())
    )


def aov_by_promotion(fact: DataFrame, promo: DataFrame) -> DataFrame:
    return (
        fact.join(promo.select("promotion_sk", "promotion_type"), "promotion_sk", "left")
        .groupBy("promotion_type")
        .agg(count("transaction_id").alias("transaction_count"), spark_sum("total_sales").alias("total_sales"))
        .withColumn("average_order_value", round(when(col("transaction_count") > 0, col("total_sales") / col("transaction_count")).otherwise(0.0), 2))
        .orderBy(col("average_order_value").desc())
    )


def churn_heatmap(customers: DataFrame) -> DataFrame:
    current = customers.filter(col("is_current") == True)
    return (
        current.groupBy("customer_state", "loyalty_program")
        .agg(count("customer_sk").alias("total_customers"), spark_sum(when(col("churned") == True, 1).otherwise(0)).alias("churned_customers"))
        .withColumn("churn_rate", round(when(col("total_customers") > 0, col("churned_customers") / col("total_customers") * 100).otherwise(0.0), 2))
        .orderBy("customer_state", "loyalty_program")
    )


def product_quality(products: DataFrame) -> DataFrame:
    return (
        products.groupBy("product_category")
        .agg(count("product_sk").alias("number_of_products"), round(avg("product_return_rate"), 4).alias("average_return_rate"), spark_max("product_return_rate").alias("highest_return_rate"))
        .orderBy(col("average_return_rate").desc())
    )


def store_traffic_proxy(fact: DataFrame) -> DataFrame:
    return (
        fact.groupBy("store_location", "day_of_week", "transaction_hour")
        .agg(count("transaction_id").alias("transaction_count"), round(avg("total_sales"), 2).alias("average_transaction_value"))
        .orderBy(col("transaction_count").desc())
    )
