
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

@dp.table(
    name="ledgerlink_catalog.silver.customers",
    comment="Customers cleaned data"
)
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
def transactions_silver() -> DataFrame:
    bronze_df = (
        spark.readStream.table("ledgerlink_catalog.bronze.customers")
    )

    transformed_df = (
        bronze_df
        .withColumn("account_open_date", F.to_date(F.col("account_open_date"), "yyyy-MM-dd").alias("account_open_date"))
        .withColumn("silver_ingestion_time", F.current_timestamp())
    )

    return transformed_df