from pyspark.sql.functions import col, when, lit, row_number, to_date
from pyspark.sql.window import Window

def clean_customer(df):
    df = df.filter(col("customer_id").isNotNull())
    # Deduplicate keeping the latest ingested record
    if "ingested_at" in df.columns:
        # Incremental customer files may legitimately contain several supplied
        # SCD source versions for one business key.
        dedup_keys = ["customer_id", "version"] if "version" in df.columns else ["customer_id"]
        w = Window.partitionBy(*dedup_keys).orderBy(col("ingested_at").desc())
        df = df.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn")
    
    # Castings & Nulls
    for c in ["age", "membership_years", "number_of_children"]:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast("int")).fillna({c: 0})
           
    string_cols = ["gender", "income_bracket", "loyalty_program", "churned", "marital_status", "education_level", "occupation", "customer_zip_code", "customer_city", "customer_state"]
    for c in string_cols:
        if c in df.columns:
            df = df.withColumn(c, when(col(c).isNull() | (col(c) == ""), lit("Unknown")).otherwise(col(c)))
    return df

def clean_product(df):
    df = df.filter(col("product_id").isNotNull())
    if "ingested_at" in df.columns:
        w = Window.partitionBy("product_id").orderBy(col("ingested_at").desc())
        df = df.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn")
        
    num_cols = ["product_rating", "product_review_count", "product_stock", "product_return_rate", "product_weight", "unit_price"]
    for c in num_cols:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast("double")).fillna({c: 0.0})
            
    string_cols = ["product_name", "product_brand", "product_category", "product_size", "product_color", "product_material"]
    for c in string_cols:
        if c in df.columns:
            df = df.withColumn(c, when(col(c).isNull() | (col(c) == ""), lit("Unknown")).otherwise(col(c)))
            
    # Parse dates if they exist, else handle appropriately (e.g., leave as string or cast to date)
    date_cols = ["product_manufacture_date", "product_expiry_date"]
    for c in date_cols:
        if c in df.columns:
            # Cast using to_date assuming standard format YYYY-MM-DD
            df = df.withColumn(c, to_date(col(c)))
            
    return df

def clean_sales(df):
    df = df.filter(col("transaction_id").isNotNull())
    # Sales deduplication by transaction_id
    if "ingested_at" in df.columns:
        w = Window.partitionBy("transaction_id").orderBy(col("ingested_at").desc())
        df = df.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn")
        
    df = df.withColumn("transaction_date", to_date(col("transaction_date"))) \
           .withColumn("quantity", col("quantity").cast("int")) \
           .withColumn("unit_price", col("unit_price").cast("double")) \
           .withColumn("discount_applied", col("discount_applied").cast("double")) \
           .withColumn("total_sales", col("total_sales").cast("double")) \
           .withColumn("transaction_hour", col("transaction_hour").cast("int")) \
           .fillna({"quantity": 0, "unit_price": 0.0, "discount_applied": 0.0, "total_sales": 0.0, "transaction_hour": 0})
    
    # Fill null dimensions with default string if any
    string_cols = ["payment_method", "store_location", "promotion_type"]
    for c in string_cols:
        if c in df.columns:
            df = df.withColumn(c, when(col(c).isNull() | (col(c) == ""), lit("Unknown")).otherwise(col(c)))
            
    return df
