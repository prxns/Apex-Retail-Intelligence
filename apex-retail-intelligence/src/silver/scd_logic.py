"""Delta MERGE implementations for Customer SCD2 and Product SCD1."""
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, coalesce, concat_ws, current_date, date_sub, lit, md5, row_number, sha2, to_date
from pyspark.sql.window import Window

_CUSTOMER_META = {"customer_id", "ingested_at", "load_type", "source_file", "batch_id", "version", "surrogate_key", "effective_start_date", "effective_end_date", "is_current"}


def _customer_hash(df: DataFrame) -> DataFrame:
    tracked = [c for c in df.columns if c not in _CUSTOMER_META]
    if not tracked:
        raise ValueError("No customer attributes available for SCD2 hash.")
    return df.withColumn("_row_hash", sha2(concat_ws("||", *[coalesce(col(c).cast("string"), lit("<NULL>")) for c in tracked]), 256))


def initialize_customer_scd2(spark, historical: DataFrame, path: str) -> None:
    """Create deterministic sequential keys for the initial Silver customer dimension."""
    src = _customer_hash(historical).drop("surrogate_key", "version", "effective_end_date", "is_current", "effective_start_date")
    w = Window.orderBy(col("customer_id").cast("long"), col("_row_hash"))
    out = (
        src.withColumn("customer_sk", row_number().over(w).cast("long"))
        .withColumn("effective_start_date", lit("1900-01-01").cast("date"))
        .withColumn("effective_end_date", lit(None).cast("date"))
        .withColumn("is_current", lit(True))
        .drop("_row_hash")
    )
    out.write.format("delta").mode("overwrite").save(path)


def merge_customer_scd2(spark, incremental: DataFrame, path: str) -> None:
    """Close changed current versions with MERGE and append only changed/new versions."""
    table = DeltaTable.forPath(spark, path)
    target = table.toDF()
    source = _customer_hash(incremental)

    tracked = [c for c in source.columns if c not in _CUSTOMER_META and c != "_row_hash"]
    current = target.filter(col("is_current") == True).select("customer_id", "customer_sk", "effective_start_date", "_row_hash") if "_row_hash" in target.columns else None
    if current is None:
        target_hash = target.withColumn("_row_hash", sha2(concat_ws("||", *[coalesce(col(c).cast("string"), lit("<NULL>")) for c in tracked]), 256))
        current = target_hash.filter(col("is_current") == True).select("customer_id", "customer_sk", "effective_start_date", "_row_hash")

    classified = source.join(current.select("customer_id", col("_row_hash").alias("target_hash")), "customer_id", "left")
    changes = classified.filter(col("target_hash").isNull() | (col("_row_hash") != col("target_hash")))
    changed_existing = changes.filter(col("target_hash").isNotNull()).select("customer_id").distinct()

    if changed_existing.limit(1).count() > 0:
        # Close a changed version one day before the incoming version starts.
        change_dates = changes.filter(col("target_hash").isNotNull()).select("customer_id", to_date(col("effective_start_date")).alias("new_start")) if "effective_start_date" in changes.columns else changed_existing.withColumn("new_start", current_date())
        table.alias("t").merge(
            change_dates.alias("s"),
            "t.customer_id = s.customer_id AND t.is_current = true",
        ).whenMatchedUpdate(
            set={"is_current": lit(False), "effective_end_date": date_sub(col("s.new_start"), 1)}
        ).execute()

    if changes.limit(1).count() == 0:
        return

    max_sk = target.agg({"customer_sk": "max"}).collect()[0][0] or 0
    start_col = to_date(col("effective_start_date")) if "effective_start_date" in changes.columns else current_date()
    inserts = (
        changes.drop("target_hash", "surrogate_key", "version", "effective_end_date", "is_current")
        .withColumn("effective_start_date", coalesce(start_col, current_date()))
        .withColumn("effective_end_date", lit(None).cast("date"))
        .withColumn("is_current", lit(True))
    )
    w = Window.orderBy(col("customer_id").cast("long"), col("_row_hash"))
    inserts = inserts.withColumn("customer_sk", (lit(int(max_sk)) + row_number().over(w)).cast("long")).drop("_row_hash")
    inserts.write.format("delta").mode("append").save(path)

    current_count = (
        spark.read.format("delta").load(path).filter(col("is_current") == True)
        .groupBy("customer_id").count().filter(col("count") != 1).limit(1).count()
    )
    if current_count:
        raise AssertionError("Customer SCD2 invariant failed: a customer does not have exactly one current row.")


def process_silver_product(spark, df_updates: DataFrame, silver_path: str) -> None:
    """SCD1 product MERGE with stable sequential surrogate keys."""
    if not DeltaTable.isDeltaTable(spark, silver_path):
        w = Window.orderBy(col("product_id").cast("long"))
        out = df_updates.withColumn("product_sk", row_number().over(w).cast("long"))
        out.write.format("delta").mode("overwrite").save(silver_path)
        return

    table = DeltaTable.forPath(spark, silver_path)
    existing = table.toDF().select("product_id", "product_sk")
    incoming = df_updates.drop("product_sk") if "product_sk" in df_updates.columns else df_updates
    new_rows = incoming.join(existing, "product_id", "left_anti")
    max_sk = existing.agg({"product_sk": "max"}).collect()[0][0] or 0
    if new_rows.limit(1).count() > 0:
        w = Window.orderBy(col("product_id").cast("long"))
        new_rows = new_rows.withColumn("product_sk", (lit(int(max_sk)) + row_number().over(w)).cast("long"))
    else:
        new_rows = incoming.limit(0).withColumn("product_sk", lit(0).cast("long"))

    matched = incoming.join(existing, "product_id", "inner").select(incoming["*"], existing["product_sk"])
    merged_source = matched.unionByName(new_rows, allowMissingColumns=True)
    table.alias("t").merge(merged_source.alias("s"), "t.product_id = s.product_id") \
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
