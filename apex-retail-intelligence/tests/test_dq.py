import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession

from src.quality.dq_rules import clean_customer, clean_product, clean_sales


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[2]").appName("apex-tests").getOrCreate()


def test_customer_pk_and_duplicate_handling(spark):
    df = spark.createDataFrame([(1, "A"), (1, "A"), (None, "B")], ["customer_id", "gender"])
    cleaned = clean_customer(df)
    assert cleaned.count() == 1
    assert cleaned.first()["gender"] == "A"


def test_product_numeric_cast(spark):
    df = spark.createDataFrame([("10", "4.5", None)], ["product_id", "product_rating", "product_name"])
    cleaned = clean_product(df)
    assert str(cleaned.schema["product_rating"].dataType) == "DoubleType()"
    assert cleaned.first()["product_name"] == "Unknown"


def test_sales_numeric_and_pk_rules(spark):
    df = spark.createDataFrame([("1", "2", None), (None, "3", "x")], ["transaction_id", "quantity", "payment_method"])
    cleaned = clean_sales(df)
    assert cleaned.count() == 1
    assert cleaned.first()["quantity"] == 2
