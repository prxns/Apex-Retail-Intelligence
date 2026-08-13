from delta.tables import DeltaTable
from pyspark.sql.functions import col, md5

def process_silver_sales(spark, df_updates, silver_path):
    """
    Immutable ledger for Sales. Deduplicates and merges new transactions only.
    """
    df_updates = df_updates.withColumn("sales_sk", md5(col("transaction_id")))
    
    if not DeltaTable.isDeltaTable(spark, silver_path):
        df_updates.write.format("delta").mode("overwrite").save(silver_path)
    else:
        deltaTable = DeltaTable.forPath(spark, silver_path)
        deltaTable.alias("target").merge(
            df_updates.alias("source"),
            "target.transaction_id = source.transaction_id"
        ).whenNotMatchedInsertAll().execute()
