from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F



@dp.table(
    name="ledgerlink_catalog.gold.transaction_count_minute_sliding_window",
    comment="Alert details where transaction has been performed with value higher that what is configured by customer"
)
def transaction_count_by_minute_sliding_window() -> DataFrame:

    transactions = spark.readStream.table("ledgerlink_catalog.silver.transactions")
    fraud_watchlist = spark.readStream.table("ledgerlink_catalog.silver.fraud_watches")

    transactions_with_watermark = transactions.withWatermark("transaction_timestamp", "5 minutes")
    
    transaction_count_df = transactions_with_watermark.groupBy(
        F.window("transaction_timestamp", "5 minutes" ,"1 minute")
    ).agg(
        F.count("*").alias("transaction_count")
    ).select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("transaction_count")
    )

    return transaction_count_df


