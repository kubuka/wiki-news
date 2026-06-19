from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, BooleanType, LongType


def main():
    spark = (
        SparkSession.builder.appName("WikiStreamProcessor")
        .config(
            "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2"
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    wiki_schema = (
        StructType()
        .add("server_name", StringType())
        .add("type", StringType())
        .add("bot", BooleanType())
        .add("title", StringType())
        .add("user", StringType())
        .add("timestamp", LongType())
    )

    raw_kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "wiki_raw_stream")
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = (
        raw_kafka_df.selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), wiki_schema).alias("data"))
        .select("data.*")
    )

    query = (
        parsed_df.writeStream.outputMode("append")
        .option("truncate", "false")
        .format("console")
        .start()
    )

    query.awaitTermination()


main()
