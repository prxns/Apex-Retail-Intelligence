"""Immutable sales ledger using Delta MERGE and stable sequential keys."""
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, row_number
from pyspark.sql.window import Window


def process_silver_sales(spark, df_updates: DataFrame, silver_path: str) -> None:
    if not DeltaTable.isDeltaTable(spark, silver_path):
        w = Window.orderBy(col("transaction_id").cast("long"))
        out = df_updates.withColumn("sales_sk", row_number().over(w).cast("long"))
        out.write.format("delta").mode("overwrite").save(silver_path)
        return

    table = DeltaTable.forPath(spark, silver_path)
    existing = table.toDF().select("transaction_id", "sales_sk")
    incoming = df_updates.drop("sales_sk") if "sales_sk" in df_updates.columns else df_updates
    new_rows = incoming.join(existing, "transaction_id", "left_anti")
    max_sk = existing.agg({"sales_sk": "max"}).collect()[0][0] or 0
    if new_rows.limit(1).count() > 0:
        w = Window.orderBy(col("transaction_id").cast("long"))
        new_rows = new_rows.withColumn("sales_sk", (lit(int(max_sk)) + row_number().over(w)).cast("long"))

    # Existing transaction IDs are not overwritten: the ledger is immutable.
    # New IDs only are inserted, making reruns idempotent.
    if new_rows.limit(1).count() > 0:
        table.alias("t").merge(new_rows.alias("s"), "t.transaction_id = s.transaction_id") \
            .whenNotMatchedInsertAll().execute()
