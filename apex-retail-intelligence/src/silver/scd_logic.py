from delta.tables import DeltaTable
from pyspark.sql.functions import col, coalesce, concat_ws, current_date, lit, md5


def process_silver_product(spark, df_updates, silver_path):
    """SCD Type 1 product MERGE with a deterministic surrogate key."""
    df_updates = df_updates.withColumn("product_sk", md5(col("product_id").cast("string")))
    if not DeltaTable.isDeltaTable(spark, silver_path):
        df_updates.write.format("delta").mode("overwrite").save(silver_path)
        return
    DeltaTable.forPath(spark, silver_path).alias("target").merge(
        df_updates.alias("source"), "target.product_id = source.product_id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


def process_silver_customer(spark, df_updates, silver_path):
    """SCD Type 2 customer processing without watermarking.

    The source hash makes no-change reruns a no-op; changed records close only
    the current version and append a new deterministic version key.
    """
    ignored = {
        "customer_id", "ingested_at", "load_type", "source_file", "customer_sk",
        "effective_start_date", "effective_end_date", "is_current", "surrogate_key", "version",
    }
    hash_cols = [name for name in df_updates.columns if name not in ignored]
    if not hash_cols:
        raise ValueError("Customer SCD Type 2 requires at least one tracked attribute.")
    source = df_updates.withColumn(
        "row_hash", md5(concat_ws("|", *[coalesce(col(name).cast("string"), lit("<NULL>")) for name in hash_cols]))
    )

    if not DeltaTable.isDeltaTable(spark, silver_path):
        initial = source.withColumn("effective_start_date", current_date()) \
            .withColumn("effective_end_date", lit(None).cast("date")) \
            .withColumn("is_current", lit(True)) \
            .withColumn("customer_sk", md5(concat_ws("|", col("customer_id").cast("string"), col("row_hash")))) \
            .drop("row_hash")
        initial.write.format("delta").mode("overwrite").save(silver_path)
        return

    table = DeltaTable.forPath(spark, silver_path)
    target = table.toDF().filter(col("is_current") == True)
    target_hash = target.withColumn(
        "row_hash", md5(concat_ws("|", *[coalesce(col(name).cast("string"), lit("<NULL>")) for name in hash_cols]))
    ).select("customer_id", col("row_hash").alias("target_row_hash"))
    classified = source.join(target_hash, "customer_id", "left")
    changes = classified.filter(col("target_row_hash").isNull() | (col("row_hash") != col("target_row_hash")))
    changed_existing = changes.filter(col("target_row_hash").isNotNull()).select("customer_id").distinct()

    table.alias("target").merge(
        changed_existing.alias("source"), "target.customer_id = source.customer_id AND target.is_current = true"
    ).whenMatchedUpdate(set={"is_current": lit(False), "effective_end_date": current_date()}).execute()

    inserts = changes.drop("target_row_hash").withColumn("effective_start_date", current_date()) \
        .withColumn("effective_end_date", lit(None).cast("date")) \
        .withColumn("is_current", lit(True)) \
        .withColumn("customer_sk", md5(concat_ws("|", col("customer_id").cast("string"), col("row_hash")))) \
        .drop("row_hash")
    inserts.write.format("delta").mode("append").save(silver_path)
