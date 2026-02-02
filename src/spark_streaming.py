import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count, avg,
    current_timestamp, to_timestamp, sum, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, BooleanType, LongType
)

# Kafka 설정
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "wiki-events"

def create_spark_session() -> SparkSession:
    """Streaming용 SparkSession 생성"""
    spark = SparkSession.builder \
        .appName("WikiStreaming") \
        .master("local[*]") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark

def get_message_schema():
    """Wikimedia recentchange Kafka 메시지 스키마"""
    return StructType([

        # meta
        StructField("domain", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("partition", LongType(), True),
        StructField("offset", LongType(), True),

        # page info
        StructField("title", StringType(), True),
        StructField("title_url", StringType(), True),
        StructField("namespace", LongType(), True),

        # edit info
        StructField("type", StringType(), True),
        StructField("comment", StringType(), True),
        StructField("parsedcomment", StringType(), True),
        StructField("minor", BooleanType(), True),
        StructField("bot", BooleanType(), True),

        # user
        StructField("user", StringType(), True),

        # length diff
        StructField("old_length", LongType(), True),
        StructField("new_length", LongType(), True),

        # revision
        StructField("old_revision", LongType(), True),
        StructField("new_revision", LongType(), True),

        # 수집 시간
        StructField("collected_at", StringType(), True),
    ])

def read_from_kafka(spark: SparkSession):
    """Kafka에서 스트림 읽기"""

    # format: "kafka"
    # option: kafka.bootstrap.servers, subscribe
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    return kafka_df

from pyspark.sql.functions import col, from_json, to_timestamp, current_timestamp
from pyspark.sql.types import IntegerType

def parse_messages(kafka_df, schema):
    """Kafka 메시지 파싱 (Wikimedia recentchange)"""

    # Kafka value는 bytes -> string
    string_df = kafka_df.selectExpr("CAST(value AS STRING) as value")

    # JSON -> struct
    parsed_df = string_df.select(from_json(col("value"), schema).alias("data"))

    # struct 펼치기
    flat_df = parsed_df.select(
        # meta
        col("data.domain").alias("domain"),
        to_timestamp(col("data.event_time")).alias("event_time"),  # event_time이 ISO8601이면 잘 파싱됨
        col("data.partition").alias("partition"),
        col("data.offset").alias("offset"),

        # page info
        col("data.title").alias("title"),
        col("data.title_url").alias("title_url"),
        col("data.namespace").alias("namespace"),

        # edit info
        col("data.type").alias("type"),
        col("data.comment").alias("comment"),
        col("data.parsedcomment").alias("parsedcomment"),
        col("data.minor").alias("minor"),
        col("data.bot").alias("bot"),

        # user
        col("data.user").alias("user"),

        # length diff
        col("data.old_length").alias("old_length"),
        col("data.new_length").alias("new_length"),

        # revision
        col("data.old_revision").alias("old_revision"),
        col("data.new_revision").alias("new_revision"),

        # collected_at (수집시간)
        to_timestamp(col("data.collected_at")).alias("collected_at"),
    ).withColumn("processing_time", current_timestamp())

    return flat_df

def aggregate_by_wiki(df):
    """위키 실시간 집계: 도메인별 1분 윈도우"""

    aggregated = (
        df
        .withWatermark("event_time", "2 minutes")   # 1분 윈도우보다 조금 크게
        .groupBy(
            window(col("event_time"), "1 minute"),
            col("domain")
        )
        .agg(
            count("*").alias("change_count"),
            sum(when(col("bot") == True, 1).otherwise(0)).alias("bot_change_count"),
            sum(when(col("minor") == True, 1).otherwise(0)).alias("minor_change_count"),
        )
    )

    return aggregated

def write_to_console(df, output_mode="complete"):
    """콘솔로 출력 (디버깅용)"""

    query = df.writeStream \
        .format("console") \
        .outputMode(output_mode) \
        .option("truncate", False) \
        .start()

    return query


def write_to_parquet(df, path: str):
    """Parquet 파일로 저장 (append 모드)"""

    query = df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", path) \
        .option("checkpointLocation", f"{path}/_checkpoint") \
        .start()

    return query

if __name__ == "__main__":
    print("=" * 60)
    print("Spark Structured Streaming 시작")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {KAFKA_TOPIC}")
    print("=" * 60)

    # SparkSession 생성
    spark = create_spark_session()

    # 스키마 정의
    schema = get_message_schema()

    # Kafka에서 읽기
    kafka_df = read_from_kafka(spark)

    # 메시지 파싱
    parsed_df = parse_messages(kafka_df, schema)

    # 집계
    station_agg = aggregate_by_wiki(parsed_df)

    # 콘솔 출력 (디버깅)
    console_query = write_to_console(station_agg, "complete")

    # Parquet 저장 (원본 데이터)
    # parquet_query = write_to_parquet(parsed_df, "output/streaming_data")

    print("\nStreaming 시작... (Ctrl+C로 종료)")

    # 스트림 대기
    console_query.awaitTermination()