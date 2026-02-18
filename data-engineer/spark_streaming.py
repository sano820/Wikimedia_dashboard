# src/spark_streaming.py
"""
Wikimedia Kafka 스트림 처리 with Spark Structured Streaming
- Kafka topics 'wiki-recentchange', 'wiki-revision-create'에서 이벤트 읽기
- Raw 데이터를 Parquet로 저장
- 5가지 집계 메트릭 계산 및 JSON + Redis 저장
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
KAFKA_TOPIC_RECENTCHANGE = os.getenv("KAFKA_TOPIC_RECENTCHANGE", "wiki-recentchange")
KAFKA_TOPIC_REVISION_CREATE = os.getenv("KAFKA_TOPIC_REVISION_CREATE", "wiki-revision-create")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# 경로 설정
RAW_DATA_PATH = "/app/data/raw"
PROCESSED_DATA_PATH = "/app/output/processed"
CHECKPOINT_BASE = "/app/checkpoints"


def create_spark_session():
    """
    SparkSession 생성 (Kafka 패키지 포함)
    최적화 설정:
    - shuffle.partitions: 2 (두 토픽이므로 적절한 병렬도)
    - adaptive.enabled: true (동적 최적화)
    - streaming.statefulOperator.checkCorrectness.enabled: false (성능 향상)
    """
    spark = SparkSession.builder \
        .appName("WikimediaStreaming") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_wikimedia_schema():
    """
    Wikimedia recentchange + revision-create 통합 스키마
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

        # revision-create 전용 필드
        StructField("rev_content_changed", BooleanType(), True),
    ])


def read_from_kafka(spark):
    """
    Kafka에서 두 토픽 읽기
    - wiki-recentchange: recentchange 이벤트
    - wiki-revision-create: revision-create 이벤트

    최적화 설정:
    - maxOffsetsPerTrigger: 5000 (트리거당 최대 메시지 수 제한, 메모리 관리)
    - minPartitions: 2 (두 토픽에 적절한 파티션 수)
    """
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", f"{KAFKA_TOPIC_RECENTCHANGE},{KAFKA_TOPIC_REVISION_CREATE}") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", "5000") \
        .option("minPartitions", "2") \
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
    - rev_content_changed: revision-create 이벤트에서 실제 content 변경 여부
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

        # revision-create 전용 필드
        coalesce(col("data.rev_content_changed"), lit(False)).alias("rev_content_changed"),

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


def metric5_content_change_ratio(df):
    """
    메트릭 5: 실제 content 변경 비율
    - revision-create 이벤트에서 rev_content_changed=true인 비율 계산
    - 윈도우: 최근 1분
    - 출력: { "ts": "...", "content_change_ratio": 0.85, "content_changed_count": 85, "total_revision_count": 100 }
    """
    # revision-create 이벤트만 필터링 (stream = "mediawiki.revision-create")
    revision_df = df.filter(col("stream") == "mediawiki.revision-create")

    aggregated = revision_df \
        .withWatermark("event_time", "2 minutes") \
        .groupBy(window(col("event_time"), "1 minute")) \
        .agg(
            count("*").alias("total_revision_count"),
            spark_sum(when(col("rev_content_changed") == True, 1).otherwise(0)).alias("content_changed_count")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("total_revision_count"),
            col("content_changed_count"),
            (col("content_changed_count") / col("total_revision_count")).alias("content_change_ratio")
        )

    return aggregated



def get_redis_client():
    """
    Redis 클라이언트 생성 (연결 풀 사용)
    최적화: ConnectionPool 사용으로 연결 재사용
    """
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
        max_connections=10,
        socket_keepalive=True,
        socket_timeout=5,
        retry_on_timeout=True
    )


def write_metric1_to_outputs(batch_df, batch_id):
    """
    메트릭 1 결과를 JSON 파일과 Redis에 누적 저장
    Redis key: metrics:events_total_10m_10s
    최적화: coalesce(1)로 파일 수 최소화, Redis 파이프라인 사용
    """
    if batch_df.isEmpty():
        return

    # 데이터 수집 (한 번만)
    rows = batch_df.collect()

    # JSON 파일로 저장 (최적화: coalesce로 단일 파일)
    batch_df.coalesce(1).write \
        .mode("append") \
        .json(f"{PROCESSED_DATA_PATH}/events_total")

    # 새로운 레코드 생성
    new_records = [
        {
            "ts": row.window_start.isoformat(),
            "count": int(row.event_count)
        }
        for row in rows
    ]

    try:
        r = get_redis_client()

        # 기존 데이터 읽기
        existing_data = r.get("metrics:events_total_10m_10s")
        if existing_data:
            try:
                existing_records = json.loads(existing_data)
            except json.JSONDecodeError:
                existing_records = []
        else:
            existing_records = []

        # 중복 제거를 위해 timestamp를 키로 하는 딕셔너리 생성
        records_dict = {rec["ts"]: rec for rec in existing_records}

        # 새 레코드 추가 (덮어쓰기)
        for rec in new_records:
            records_dict[rec["ts"]] = rec

        # 시간순 정렬 (최신순)
        all_records = sorted(records_dict.values(), key=lambda x: x["ts"], reverse=True)

        # 최근 1000개로 제한 (약 2.7시간 분량, 10초 버킷 기준)
        all_records = all_records[:1000]

        # Redis에 저장 (최적화: 단일 SET 명령)
        r.set("metrics:events_total_10m_10s", json.dumps(all_records), ex=10800)  # 3시간 TTL
        print(f"[Metric 1] Batch {batch_id}: Added {len(new_records)} records, Total {len(all_records)} records in Redis")
    except Exception as e:
        print(f"[Metric 1] Redis error: {e}")


def write_metric2_to_outputs(batch_df, batch_id):
    """
    메트릭 2 결과를 JSON 파일과 Redis에 저장
    - 배치(정적 DF)이므로 row_number 사용 가능
    - window별 top5 + others로 묶는다
    최적화: window 함수 최적화, Redis TTL 추가
    """
    if batch_df.isEmpty():
        return

    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, sum as _sum

    # 1) window별 cnt 기준 rank (최적화: 파티션 수 최소화)
    w = Window.partitionBy("window_start", "window_end").orderBy(col("cnt").desc())
    ranked = batch_df.withColumn("rank", row_number().over(w))

    # 2) top5 유지, 나머지는 others
    grouped = ranked.withColumn(
        "final_type",
        when(col("rank") <= 5, col("event_type")).otherwise(lit("others"))
    ).groupBy("window_start", "window_end", "final_type") \
    .agg(_sum("cnt").alias("cnt"))

    # 3) JSON 파일 저장 (최적화: 단일 파일)
    grouped.coalesce(1).write.mode("append").json(f"{PROCESSED_DATA_PATH}/events_by_type")

    # 4) Redis에 누적 저장
    records = grouped.collect()

    # 새로운 윈도우 데이터 구성
    new_window_dict = {}
    for row in records:
        ts = row.window_start.isoformat()
        if ts not in new_window_dict:
            new_window_dict[ts] = {}
        new_window_dict[ts][row.final_type] = int(row.cnt)

    try:
        r = get_redis_client()

        # 기존 데이터 읽기
        existing_data = r.get("metrics:events_by_type_10m_10s")
        if existing_data:
            try:
                existing_records = json.loads(existing_data)
                # 리스트를 딕셔너리로 변환
                existing_dict = {rec["ts"]: rec["type_counts"] for rec in existing_records}
            except json.JSONDecodeError:
                existing_dict = {}
        else:
            existing_dict = {}

        # 새 데이터로 업데이트 (덮어쓰기)
        existing_dict.update(new_window_dict)

        # 시간순 정렬 (최신순)하여 리스트로 변환
        all_records = [
            {"ts": ts, "type_counts": counts}
            for ts, counts in sorted(existing_dict.items(), reverse=True)
        ]

        # 최근 1000개 윈도우로 제한 (약 2.7시간 분량, 10초 버킷 기준)
        all_records = all_records[:1000]

        # Redis에 저장 (최적화: TTL 추가)
        r.set("metrics:events_by_type_10m_10s", json.dumps(all_records), ex=10800)  # 3시간 TTL
        print(f"[Metric 2] Batch {batch_id}: Added {len(new_window_dict)} windows, Total {len(all_records)} windows in Redis")
    except Exception as e:
        print(f"[Metric 2] Redis error: {e}")



def write_metric3_to_outputs(batch_df, batch_id):
    """
    메트릭 3 결과를 JSON 파일과 Redis에 저장
    Redis key: metrics:bot_ratio_1m
    최적화: TTL 추가, 안전한 float 변환
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
            "bot_ratio": float(latest.bot_ratio) if latest.bot_ratio else 0.0,
            "bot_count": int(latest.bot_count),
            "total_count": int(latest.total_count)
        }

        try:
            r = get_redis_client()
            r.set("metrics:bot_ratio_1m", json.dumps(result), ex=300)  # 5분 TTL
            print(f"[Metric 3] Batch {batch_id}: bot_ratio={result['bot_ratio']:.2%} saved to Redis")
        except Exception as e:
            print(f"[Metric 3] Redis error: {e}")


def write_metric4_to_outputs(batch_df, batch_id):
    """
    메트릭 4 결과를 JSON 파일과 Redis에 저장
    - 배치 DF에서 최신 window를 고르고 그 window에서 top10만 추출
    최적화: TTL 추가, 불필요한 필터링 제거
    """
    if batch_df.isEmpty():
        return

    # 1) 최신 window 하나 선택
    latest_window_row = batch_df.orderBy(col("window_start").desc()).select("window_start", "window_end").first()
    if not latest_window_row:
        return

    ws, we = latest_window_row.window_start, latest_window_row.window_end

    latest_window_df = batch_df.filter((col("window_start") == ws) & (col("window_end") == we))

    # 2) top10 (최적화: limit으로 데이터 양 최소화)
    top10 = latest_window_df.orderBy(col("cnt").desc()).limit(10)

    # 3) JSON 파일 저장
    top10.coalesce(1).write.mode("append").json(f"{PROCESSED_DATA_PATH}/top_domains")

    # 4) Redis 저장
    rows = top10.collect()
    result = [{"domain": r.domain, "count": int(r.cnt)} for r in rows]

    try:
        r = get_redis_client()
        r.set("metrics:top_domain_5m_top10", json.dumps(result), ex=600)  # 10분 TTL
        print(f"[Metric 4] Batch {batch_id}: {len(result)} domains saved to Redis")
    except Exception as e:
        print(f"[Metric 4] Redis error: {e}")


def write_metric5_to_outputs(batch_df, batch_id):
    """
    메트릭 5 결과를 JSON 파일과 Redis에 저장
    Redis key: metrics:content_change_ratio_1m
    최적화: TTL 추가, 안전한 float 변환
    """
    if batch_df.isEmpty():
        return

    # JSON 파일로 저장
    batch_df.coalesce(1).write \
        .mode("append") \
        .json(f"{PROCESSED_DATA_PATH}/content_change_ratio")

    # Redis에 저장 (최신 1개만)
    latest = batch_df.orderBy(col("window_start").desc()).first()
    if latest:
        result = {
            "ts": latest.window_start.isoformat(),
            "content_change_ratio": float(latest.content_change_ratio) if latest.content_change_ratio else 0.0,
            "content_changed_count": int(latest.content_changed_count),
            "total_revision_count": int(latest.total_revision_count)
        }

        try:
            r = get_redis_client()
            r.set("metrics:content_change_ratio_1m", json.dumps(result), ex=300)  # 5분 TTL
            print(f"[Metric 5] Batch {batch_id}: content_change_ratio={result['content_change_ratio']:.2%} saved to Redis")
        except Exception as e:
            print(f"[Metric 5] Redis error: {e}")



def main():
    print("=" * 60)
    print("Wikimedia Spark Structured Streaming")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topics: {KAFKA_TOPIC_RECENTCHANGE}, {KAFKA_TOPIC_REVISION_CREATE}")
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

    # 메트릭 5: 실제 content 변경 비율
    metric5_df = metric5_content_change_ratio(events_df)
    metric5_query = metric5_df.writeStream \
        .foreachBatch(write_metric5_to_outputs) \
        .outputMode("update") \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/metric5") \
        .start()
    print("[Metric 5] Started: Content change ratio (1min window, revision-create only)")

    print("\nAll 5 metrics streaming queries started. Waiting for termination...")
    print("Press Ctrl+C to stop")

    # 모든 쿼리 대기
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
