from pyspark.sql.functions import col, month, year, dayofweek, weekofyear, when, date_format, lit
from delta.tables import DeltaTable

def build_dim_customer(spark, silver_customer_path, gold_path):
    df = spark.read.format("delta").load(silver_customer_path)
    # dim_customer is basically the silver SCD2 table
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{gold_path}/dim_customer")
    return df

def build_dim_product(spark, silver_product_path, gold_path):
    df = spark.read.format("delta").load(silver_product_path)
    # dim_product is the silver SCD1 table
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{gold_path}/dim_product")
    return df

def build_dim_promotion(spark, silver_sales_path, gold_path):
    # Extract unique promotions from sales
    df = spark.read.format("delta").load(silver_sales_path)
    if "promotion_id" in df.columns:
        promotions = df.select("promotion_id", "promotion_type").distinct()
        promotions = promotions.withColumn("promotion_sk", col("promotion_id")) # Or use md5
        promotions.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{gold_path}/dim_promotion")
    else:
        # Dummy if missing
        pass

def build_dim_date(spark, silver_sales_path, gold_path):
    df = spark.read.format("delta").load(silver_sales_path)
    dates = df.select("transaction_date").distinct().withColumnRenamed("transaction_date", "date")
    
    dates = dates.withColumn("date_sk", date_format(col("date"), "yyyyMMdd").cast("int")) \
                 .withColumn("day", date_format(col("date"), "d").cast("int")) \
                 .withColumn("day_of_week", dayofweek(col("date"))) \
                 .withColumn("week_of_year", weekofyear(col("date"))) \
                 .withColumn("month", month(col("date"))) \
                 .withColumn("month_name", date_format(col("date"), "MMMM")) \
                 .withColumn("quarter", (((month(col("date")) - 1) / 3) + 1).cast("int")) \
                 .withColumn("year", year(col("date"))) \
                 .withColumn("weekend", when(col("day_of_week").isin([1, 7]), lit(True)).otherwise(lit(False)))
                 
    dates.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{gold_path}/dim_date")
    
def build_fact_sales(spark, silver_sales_path, gold_path):
    df = spark.read.format("delta").load(silver_sales_path)
    
    # We already have sales_sk, transaction_id, etc.
    # We need to link dimension SKs. In a real scenario, we join with dims.
    # Since Silver SCD keys are available (e.g. customer_id, product_id, date) 
    # we would perform AS-OF joins for SCD2. For simplicity, we assume Silver sales 
    # either already has SKs, or we can resolve them.
    # We will build fact_sales.
    
    # Example logic assuming Silver sales has transaction_date, product_id, customer_id
    dim_cust = spark.read.format("delta").load(f"{gold_path}/dim_customer").filter(col("is_current") == True)
    dim_prod = spark.read.format("delta").load(f"{gold_path}/dim_product")
    
    fact = df.alias("s") \
             .join(dim_cust.alias("c"), "customer_id", "left") \
             .join(dim_prod.alias("p"), "product_id", "left") \
             .withColumn("date_sk", date_format(col("s.transaction_date"), "yyyyMMdd").cast("int"))
             
    # Select keys and measures
    fact = fact.select(
        col("s.sales_sk"),
        col("s.transaction_id"),
        col("c.customer_sk"),
        col("p.product_sk"),
        col("s.promotion_id").alias("promotion_sk"), # using ID as SK for simplicity
        col("date_sk"),
        col("s.quantity"),
        col("s.unit_price"),
        col("s.discount_applied"),
        col("s.total_sales"),
        col("s.store_location"),
        col("s.transaction_hour")
    )
    
    fact.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{gold_path}/fact_sales")
