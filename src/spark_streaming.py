# src/spark_streaming.py
"""
Wikimedia Kafka 스트림 처리 with Spark Structured Streaming
- Kafka topic 'wiki-events'에서 Wikimedia recentchange 이벤트 읽기
- Raw 데이터를 Parquet로 저장
- 4가지 집계 메트릭 계산 및 JSON + Redis 저장
"""
import os
import json
import redis
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count, sum as spark_sum, when,
    to_timestamp, coalesce, lit, year, month, dayofmonth, hour
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    BooleanType, LongType
)

# 환경 변수
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "wiki-events")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# 경로 설정
RAW_DATA_PATH = "/app/data/raw"
PROCESSED_DATA_PATH = "/app/output/processed"
CHECKPOINT_BASE = "/app/checkpoints"


def create_spark_session():
    """SparkSession 생성 (Kafka 패키지 포함)"""
    spark = SparkSession.builder \
        .appName("WikimediaStreaming") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_wikimedia_schema():
    """
    Wikimedia recentchange 이벤트 스키마
    실제 이벤트 구조에 맞게 정의
    """
    return StructType([
        # meta 정보
        StructField("meta", StructType([
            StructField("dt", StringType(), True),
            StructField("domain", StringType(), True),
            StructField("stream", StringType(), True),
            StructField("uri", StringType(), True),
            StructField("id", StringType(), True),
        ]), True),

        # 이벤트 기본 정보
        StructField("type", StringType(), True),
        StructField("wiki", StringType(), True),
        StructField("title", StringType(), True),
        StructField("comment", StringType(), True),
        StructField("user", StringType(), True),
        StructField("bot", BooleanType(), True),
        StructField("minor", BooleanType(), True),

        # namespace
        StructField("namespace", IntegerType(), True),

        # revision 정보
        StructField("revision", StructType([
            StructField("new", LongType(), True),
            StructField("old", LongType(), True),
        ]), True),

        # 길이 정보
        StructField("length", StructType([
            StructField("new", LongType(), True),
            StructField("old", LongType(), True),
        ]), True),

        # 기타
        StructField("patrolled", BooleanType(), True),
        StructField("timestamp", LongType(), True),
    ])


def read_from_kafka(spark):
    """Kafka에서 wiki-events 토픽 읽기"""
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    return kafka_df


def parse_events(kafka_df, schema):
    """
    Kafka 메시지 파싱 및 표준 컬럼 생성
    - event_time: meta.dt를 timestamp로 변환
    - domain: meta.domain
    - stream: meta.stream
    - event_type: type
    - bot: coalesce(bot, false)
    - wiki: wiki
    """
    # JSON 파싱
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(
            col("json_str"),
            from_json(col("json_str"), schema).alias("data")
        )

    # 표준 컬럼 생성
    standard_df = parsed_df.select(
        # 이벤트 시간 (ISO8601 → timestamp)
        to_timestamp(col("data.meta.dt"),"yyyy-MM-dd'T'HH:mm:ss.SSSX").alias("event_time"),

        # 표준 컬럼
        col("data.meta.domain").alias("domain"),
        col("data.meta.stream").alias("stream"),
        col("data.type").alias("event_type"),
        coalesce(col("data.bot"), lit(False)).alias("bot"),
        col("data.wiki").alias("wiki"),

        # 추가 정보
        col("data.title").alias("title"),
        col("data.user").alias("user"),
        col("data.comment").alias("comment"),
        col("data.namespace").alias("namespace"),
        coalesce(col("data.minor"), lit(False)).alias("minor"),

        # 원본 JSON도 보관
        col("json_str").alias("raw_json")
    )

    return standard_df


def save_raw_data(df):
    """
    Raw 데이터를 Parquet 형식으로 저장
    파티션: year=YYYY/month=MM/day=DD/hour=HH
    """
    partitioned_df = df \
        .withColumn("year", year(col("event_time"))) \
        .withColumn("month", month(col("event_time"))) \
        .withColumn("day", dayofmonth(col("event_time"))) \
        .withColumn("hour", hour(col("event_time")))

    query = partitioned_df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", RAW_DATA_PATH) \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/raw") \
        .partitionBy("year", "month", "day", "hour") \
        .start()

    return query


def metric1_total_events(df):
    """
    메트릭 1: 전체 이벤트량 라인차트
    - 범위: 최근 10분
    - 버킷: 10초 tumble window
    - 출력: [{ "ts": "...", "count": 123 }, ...]
    """
    aggregated = df \
        .withWatermark("event_time", "2 minutes") \
        .groupBy(window(col("event_time"), "10 seconds")) \
        .agg(count("*").alias("event_count")) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("event_count")
        )

    return aggregated


def metric2_events_by_type(df):
    """
    메트릭 2: 이벤트 타입별 스택 차트
    - 스트리밍에서는 window+type count까지만 수행 (Top5+others는 foreachBatch에서 처리)
    """
    type_counts = df \
        .withWatermark("event_time", "2 minutes") \
        .groupBy(
            window(col("event_time"), "10 seconds").alias("w"),
            col("event_type")
        ) \
        .agg(count("*").alias("cnt")) \
        .select(
            col("w.start").alias("window_start"),
            col("w.end").alias("window_end"),
            col("event_type"),
            col("cnt")
        )

    return type_counts



def metric3_bot_ratio(df):
    """
    메트릭 3: bot 비율 게이지
    - 윈도우: 최근 1분
    - 출력: { "ts": "...", "bot_ratio":0.27, "bot_count":27, "total_count":100 }
    """
    aggregated = df \
        .withWatermark("event_time", "2 minutes") \
        .groupBy(window(col("event_time"), "1 minute")) \
        .agg(
            count("*").alias("total_count"),
            spark_sum(when(col("bot") == True, 1).otherwise(0)).alias("bot_count")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("total_count"),
            col("bot_count"),
            (col("bot_count") / col("total_count")).alias("bot_ratio")
        )

    return aggregated


def metric4_top_domains(df):
    """
    메트릭 4: top wiki 바차트
    - 스트리밍에서는 window+domain count까지만 수행 (Top10은 foreachBatch에서 처리)
    """
    domain_counts = df \
        .withWatermark("event_time", "2 minutes") \
        .groupBy(
            window(col("event_time"), "5 minutes").alias("w"),
            col("domain")
        ) \
        .agg(count("*").alias("cnt")) \
        .select(
            col("w.start").alias("window_start"),
            col("w.end").alias("window_end"),
            col("domain"),
            col("cnt")
        )

    return domain_counts



def get_redis_client():
    """Redis 클라이언트 생성"""
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True
    )


def write_metric1_to_outputs(batch_df, batch_id):
    """
    메트릭 1 결과를 JSON 파일과 Redis에 저장
    Redis key: metrics:events_total_10m_10s
    """
    if batch_df.isEmpty():
        return

    # JSON 파일로 저장
    batch_df.coalesce(1).write \
        .mode("append") \
        .json(f"{PROCESSED_DATA_PATH}/events_total")

    # Redis에 저장 (최신 결과만)
    records = batch_df.orderBy(col("window_start").desc()).limit(60).collect()
    result = [
        {
            "ts": row.window_start.isoformat(),
            "count": int(row.event_count)
        }
        for row in records
    ]

    try:
        r = get_redis_client()
        r.set("metrics:events_total_10m_10s", json.dumps(result))
        print(f"[Metric 1] Batch {batch_id}: {len(result)} records saved to Redis")
    except Exception as e:
        print(f"[Metric 1] Redis error: {e}")


def write_metric2_to_outputs(batch_df, batch_id):
    """
    메트릭 2 결과를 JSON 파일과 Redis에 저장
    - 배치(정적 DF)이므로 row_number 사용 가능
    - window별 top5 + others로 묶는다
    """
    if batch_df.isEmpty():
        return

    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, sum as _sum

    # 1) window별 cnt 기준 rank
    w = Window.partitionBy("window_start", "window_end").orderBy(col("cnt").desc())
    ranked = batch_df.withColumn("rank", row_number().over(w))

    # 2) top5 유지, 나머지는 others
    grouped = ranked.withColumn(
        "final_type",
        when(col("rank") <= 5, col("event_type")).otherwise(lit("others"))
    ).groupBy("window_start", "window_end", "final_type") \
    .agg(_sum("cnt").alias("cnt"))

    # 3) JSON 파일 저장
    grouped.coalesce(1).write.mode("append").json(f"{PROCESSED_DATA_PATH}/events_by_type")

    # 4) Redis 저장 (최근 60개 윈도우)
    records = grouped.orderBy(col("window_start").desc()).collect()

    window_dict = {}
    for row in records:
        ts = row.window_start.isoformat()
        if ts not in window_dict:
            window_dict[ts] = {}
        window_dict[ts][row.final_type] = int(row.cnt)

    result = [{"ts": ts, "type_counts": counts} for ts, counts in sorted(window_dict.items(), reverse=True)[:60]]

    try:
        r = get_redis_client()
        r.set("metrics:events_by_type_10m_10s", json.dumps(result))
        print(f"[Metric 2] Batch {batch_id}: {len(result)} windows saved to Redis")
    except Exception as e:
        print(f"[Metric 2] Redis error: {e}")



def write_metric3_to_outputs(batch_df, batch_id):
    """
    메트릭 3 결과를 JSON 파일과 Redis에 저장
    Redis key: metrics:bot_ratio_1m
    """
    if batch_df.isEmpty():
        return

    # JSON 파일로 저장
    batch_df.coalesce(1).write \
        .mode("append") \
        .json(f"{PROCESSED_DATA_PATH}/bot_ratio")

    # Redis에 저장 (최신 1개만)
    latest = batch_df.orderBy(col("window_start").desc()).first()
    if latest:
        result = {
            "ts": latest.window_start.isoformat(),
            "bot_ratio": float(latest.bot_ratio),
            "bot_count": int(latest.bot_count),
            "total_count": int(latest.total_count)
        }

        try:
            r = get_redis_client()
            r.set("metrics:bot_ratio_1m", json.dumps(result))
            print(f"[Metric 3] Batch {batch_id}: bot_ratio={result['bot_ratio']:.2%} saved to Redis")
        except Exception as e:
            print(f"[Metric 3] Redis error: {e}")


def write_metric4_to_outputs(batch_df, batch_id):
    """
    메트릭 4 결과를 JSON 파일과 Redis에 저장
    - 배치 DF에서 최신 window를 고르고 그 window에서 top10만 추출
    """
    if batch_df.isEmpty():
        return

    # 1) 최신 window 하나 선택
    latest_window_row = batch_df.orderBy(col("window_start").desc()).select("window_start", "window_end").first()
    if not latest_window_row:
        return

    ws, we = latest_window_row.window_start, latest_window_row.window_end

    latest_window_df = batch_df.filter((col("window_start") == ws) & (col("window_end") == we))

    # 2) top10
    top10 = latest_window_df.orderBy(col("cnt").desc()).limit(10)

    # 3) JSON 파일 저장
    top10.coalesce(1).write.mode("append").json(f"{PROCESSED_DATA_PATH}/top_domains")

    # 4) Redis 저장
    rows = top10.collect()
    result = [{"domain": r.domain, "count": int(r.cnt)} for r in rows]

    try:
        r = get_redis_client()
        r.set("metrics:top_domain_5m_top10", json.dumps(result))
        print(f"[Metric 4] Batch {batch_id}: {len(result)} domains saved to Redis")
    except Exception as e:
        print(f"[Metric 4] Redis error: {e}")



def main():
    print("=" * 60)
    print("Wikimedia Spark Structured Streaming")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {KAFKA_TOPIC}")
    print(f"Redis: {REDIS_HOST}:{REDIS_PORT}")
    print("=" * 60)

    # SparkSession 생성
    spark = create_spark_session()

    # Kafka에서 읽기
    kafka_df = read_from_kafka(spark)

    # 메시지 파싱
    schema = get_wikimedia_schema()
    events_df = parse_events(kafka_df, schema)

    # Raw 데이터 저장
    raw_query = save_raw_data(events_df)
    print("[Raw] Started saving to Parquet")

    # 메트릭 1: 전체 이벤트량
    metric1_df = metric1_total_events(events_df)
    metric1_query = metric1_df.writeStream \
        .foreachBatch(write_metric1_to_outputs) \
        .outputMode("update") \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/metric1") \
        .start()
    print("[Metric 1] Started: Total events (10min, 10sec buckets)")

    # 메트릭 2: 이벤트 타입별
    metric2_df = metric2_events_by_type(events_df)
    metric2_query = metric2_df.writeStream \
        .foreachBatch(write_metric2_to_outputs) \
        .outputMode("update") \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/metric2") \
        .start()
    print("[Metric 2] Started: Events by type (10min, 10sec buckets, top 5 + others)")

    # 메트릭 3: bot 비율
    metric3_df = metric3_bot_ratio(events_df)
    metric3_query = metric3_df.writeStream \
        .foreachBatch(write_metric3_to_outputs) \
        .outputMode("update") \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/metric3") \
        .start()
    print("[Metric 3] Started: Bot ratio (1min window)")

    # 메트릭 4: top domains
    metric4_df = metric4_top_domains(events_df)
    metric4_query = metric4_df.writeStream \
        .foreachBatch(write_metric4_to_outputs) \
        .outputMode("update") \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/metric4") \
        .start()
    print("[Metric 4] Started: Top domains (5min window, top 10)")

    print("\nAll streaming queries started. Waiting for termination...")
    print("Press Ctrl+C to stop")

    # 모든 쿼리 대기
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
