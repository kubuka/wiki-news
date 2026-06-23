from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, when
from pyspark.sql.types import StructType, StringType, BooleanType, LongType


def main():

    GCP_PROJECT_ID = "wiki-news-499909"
    GCP_TEMP_BUCKET = "wiki-news-temp-bucket"

    spark = (
        SparkSession.builder.appName("WikiStreamProcessor")
        .config(
            "spark.jars.packages",
            ",".join(
                [
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
                    "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1",
                    # "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.22",
                ]
            ),
        )
        .config("parentProject", GCP_PROJECT_ID)
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config(
            "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
            "/Users/kuba/wiki-spark-key.json",
        )
        # .config(
        #     "spark.hadoop.fs.gs.impl",
        #     "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
        # )
        # .config(
        #     "spark.hadoop.fs.AbstractFileSystem.gs.impl",
        #     "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
        # )
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

    with_time_df = parsed_df.withColumn(
        "event_time", (col("timestamp")).cast("timestamp")
    )

    # --- agregacja ----

    global_stats_df = (
        with_time_df.withWatermark("event_time", "1 minute")
        .groupBy(window(col("event_time"), "1 minute"), col("server_name"))
        .agg(
            count("*").alias("total_edits"),
            count(when(col("bot") == True, True)).alias("bot_edits"),
            count(when(col("bot") == False, False)).alias("human_edits"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("server_name"),
            col("total_edits"),
            col("bot_edits"),
            col("human_edits"),
        )
    )

    hot_topic_df = (
        with_time_df.withWatermark("event_time", "1 minute")
        .groupBy(
            window(col("event_time"), "1 minute"), col("server_name"), col("title")
        )
        .agg(count("*").alias("edit_count"))
        .filter(col("edit_count") > 1)
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("server_name"),
            col("title").alias("page_title"),
            col("edit_count"),
        )
    )

    # ----big query save------

    def save_global_stats(batch_df, batch_id):
        if not batch_df.isEmpty():
            (
                batch_df.write.format("bigquery")
                .option(
                    "table", f"{GCP_PROJECT_ID}.wikipedia_streaming.wiki_global_stats"
                )
                .option(
                    "writeMethod", "direct"
                )  # xddd wystarczylo zmienic na direct zabije sie a nie jakis bucket
                .mode("append")
                .save()
            )

    def save_hot_topics(batch_df, batch_id):
        if not batch_df.isEmpty():
            (
                batch_df.write.format("bigquery")
                .option(
                    "table", f"{GCP_PROJECT_ID}.wikipedia_streaming.wiki_hot_topics"
                )
                .option("writeMethod", "direct")
                .mode("append")
                .save()
            )

    query_global = (
        global_stats_df.writeStream.foreachBatch(save_global_stats)
        .option("checkpointLocation", "/tmp/spark_checkpoint_global")
        .outputMode("append")
        .start()
    )

    query_hot = (
        hot_topic_df.writeStream.foreachBatch(save_hot_topics)
        .option("checkpointLocation", "/tmp/spark_checkpoint_hot")
        .outputMode("append")
        .start()
    )

    print("maszyna ruszyła")
    spark.streams.awaitAnyTermination()


main()
