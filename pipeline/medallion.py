"""Bronze -> Silver -> Gold medallion pipeline on PySpark + Delta Lake.

bronze: raw JSON as-is plus ingest metadata
silver: typed, validated, de-duplicated events
gold:   per-player stats, per-genre stats, daily active players (served by the API)
"""
import argparse
import time
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F


def get_spark(app="game-telemetry-lakehouse"):
    builder = (
        SparkSession.builder.appName(app)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "16")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def to_bronze(spark, raw, base):
    df = spark.read.json(str(raw)).withColumn("_ingested_at", F.current_timestamp())
    df.write.format("delta").mode("overwrite").save(f"{base}/bronze/events")
    return df.count()


def to_silver(spark, base):
    bronze = spark.read.format("delta").load(f"{base}/bronze/events")
    silver = (
        bronze.withColumn("event_ts", F.to_timestamp("event_ts"))
        .filter(F.col("player_id").isNotNull() & (F.col("playtime_seconds") > 0))
        .dropDuplicates(["event_id"])
        .withColumn("event_date", F.to_date("event_ts"))
        .drop("_ingested_at")
    )
    (silver.write.format("delta").mode("overwrite")
        .partitionBy("event_date").save(f"{base}/silver/events"))
    return silver.count()


def to_gold(spark, base):
    s = spark.read.format("delta").load(f"{base}/silver/events")

    player = s.groupBy("player_id").agg(
        F.sum("playtime_seconds").alias("total_playtime_seconds"),
        F.countDistinct("title_id").alias("distinct_titles"),
        F.count("*").alias("events"),
    )
    player.write.format("delta").mode("overwrite").save(f"{base}/gold/player_stats")

    genre = s.groupBy("genre").agg(
        F.sum("playtime_seconds").alias("total_playtime_seconds"),
        F.countDistinct("player_id").alias("players"),
    )
    genre.write.format("delta").mode("overwrite").save(f"{base}/gold/genre_stats")

    top = (s.groupBy("player_id", "title_id").agg(F.sum("playtime_seconds").alias("playtime"))
           .withColumn("rn", F.row_number().over(
               __import__("pyspark.sql.window", fromlist=["Window"]).Window
               .partitionBy("player_id").orderBy(F.desc("playtime"))))
           .filter("rn <= 5").drop("rn"))
    top.write.format("delta").mode("overwrite").save(f"{base}/gold/player_top_titles")

    dau = s.groupBy("event_date").agg(F.countDistinct("player_id").alias("active_players"))
    dau.write.format("delta").mode("overwrite").save(f"{base}/gold/daily_active")

    # Parquet export of the gold tables so the API doesn't need a Spark runtime
    for name in ("player_stats", "genre_stats", "player_top_titles", "daily_active"):
        (spark.read.format("delta").load(f"{base}/gold/{name}")
            .coalesce(1).write.mode("overwrite").parquet(f"{base}/serving/{name}"))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--raw", default="data/raw")
    p.add_argument("--base", default="data/lake")
    a = p.parse_args()
    Path(a.base).mkdir(parents=True, exist_ok=True)
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    t0 = time.time()
    n_bronze = to_bronze(spark, a.raw, a.base)
    t1 = time.time()
    n_silver = to_silver(spark, a.base)
    t2 = time.time()
    to_gold(spark, a.base)
    t3 = time.time()

    print(f"bronze rows : {n_bronze:,}  ({t1 - t0:.1f}s)")
    print(f"silver rows : {n_silver:,}  ({t2 - t1:.1f}s)  dropped {n_bronze - n_silver:,}")
    print(f"gold built  : ({t3 - t2:.1f}s)")
    print(f"total       : {t3 - t0:.1f}s")
    spark.stop()


if __name__ == "__main__":
    main()
