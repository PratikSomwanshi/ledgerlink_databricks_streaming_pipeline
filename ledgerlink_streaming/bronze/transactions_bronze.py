
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

import json


@dp.table(
    name="ledgerlink_catalog.bronze.transactions",
    comment="Transaction raw stream data ingested from Kafka"
)
def transactions_bronze() -> DataFrame:

    kafka_connection_json = dbutils.secrets.get(
        scope="ledgerlink-scope",
        key="kafka_connection_details"
    )

    kafka_config = json.loads(kafka_connection_json)

    bootstrap_server = kafka_config["bootstrap_servers"]
    api_key = kafka_config["api_key"]
    api_secret = kafka_config["api_secret"]
    topic = kafka_config["topic"]

    ca_file = (
        "/Volumes/ledgerlink_catalog/ledgerlink_schema/"
        "ledgerlink_volume/ca (2).pem"
    )

    with open(ca_file, "r", encoding="utf-8") as f:
        ca_certificate = f.read()

    jaas_config = (
        "kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required "
        f'username="{api_key}" '
        f'password="{api_secret}";'
    )

    streaming_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_server)
        .option("subscribe", topic)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "SCRAM-SHA-256")
        .option("kafka.sasl.jaas.config", jaas_config)
        .option("kafka.ssl.truststore.type", "PEM")
        .option("kafka.ssl.truststore.certificates", ca_certificate)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed_streaming_df = streaming_df.select(
        F.col("key").cast("string").alias("key"),
        F.col("value").cast("string").alias("value"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp"),
        F.col("timestampType"),
        F.current_timestamp().alias("ingestion_timestamp")
    )

    return parsed_streaming_df
