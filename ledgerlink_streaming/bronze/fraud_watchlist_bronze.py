
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

import json


@dp.table(
    name="ledgerlink_catalog.bronze.fraud_watches",
    comment="Fraw watchlist raw stream data ingested from auto loader"
)
def transactions_bronze() -> DataFrame:

    source_path = "/Volumes/ledgerlink_catalog/source/fraud_watchlist/source_data/"

    input_stream = (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "True")
            .load(source_path)
    )

    transformed_df = input_stream.select(
        F.col("watchlist_id"),
        F.col("watch_type"),
        F.col("entity_id"),
        F.col("risk_level"),
        F.col("action"),
        F.col("reason_code"),
        F.col("reason_description"),
        F.col("status"),
        F.col("effective_from"),
        F.col("reported_by"),
        F.col("reported_source"),
        F.col("country"),
        F.col("city"),
        F.col("_rescued_data"),
        F.col("_metadata.file_path").alias("source_file"),
        F.current_timestamp().alias("ingestion_timestamp")
    )

    return transformed_df


