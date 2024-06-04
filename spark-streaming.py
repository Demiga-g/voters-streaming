
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

if __name__ == "__main__":
    # Initializing spark session
    spark = SparkSession \
        .builder \
        .master('local[*]') \
        .appName("VoteStreaming") \
        .config('spark.streaming.stopGracefullyShutdown', 'true')  \
        .config("spark.jars", "./postgresql-42.6.2.jar") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .config("spark.sql.adaptive.enable", "false") \
        .getOrCreate()

    vote_schema = StructType([
        StructField("voter_id", StringType(), True),
        StructField("candidate_id", StringType(), True),
        StructField("voting_time", TimestampType(), True),
        StructField("voter_name", StringType(), True),
        StructField("vote", IntegerType(), True),
        StructField("date_of_birth", StringType(), True),
        StructField("registration_date", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("nationality", StringType(), True),
        StructField("registration_number", StringType(), True),
        StructField("address_street", StringType(), True),
        StructField("address_city", StringType(), True),
        StructField("address_state", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("email", StringType(), True),
        StructField("picture", StringType(), True),
        StructField("candidate_name", StringType(), True),
        StructField("party_affiliation", StringType(), True),
        StructField("biography", StringType(), True),
        StructField("campaign_platform", StringType(), True)
    ])

    votes_df = spark \
        .readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', 'localhost:9092') \
        .option('subscribe', 'votes_topic') \
        .option('startingOffsets', 'earliest') \
        .load() \
        .selectExpr('CAST(value AS STRING)') \
        .select(from_json(col('value'), vote_schema).alias('data')) \
        .select('data.*')

    votes_df = votes_df.withColumn("voting_time", col("voting_time").cast(TimestampType())) \
        .withColumn("vote", col("vote").cast(IntegerType()))
    enriched_votes_df = votes_df.withWatermark("voting_time", "1 minute")

    # Aggregate votes per candidate and turnout by location
    votes_per_candidate_df = enriched_votes_df \
        .groupBy("candidate_id", "candidate_name", "party_affiliation", "picture") \
        .agg(_sum("vote").alias("total_votes"))

    turnout_per_location_df = enriched_votes_df \
        .groupBy("address_state") \
        .count().alias("total_votes")

    # Write aggregated data to Kafka topics and start the streaming query
    votes_per_candidate_to_kafka = votes_per_candidate_df \
        .selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "aggregated_votes_per_candidate") \
        .option("checkpointLocation", "./checkpoints/checkpoint1") \
        .outputMode("update") \
        .start()

    turnout_per_location_to_kafka = turnout_per_location_df \
        .selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "aggregated_turnout_per_location") \
        .option("checkpointLocation", "./checkpoints/checkpoint2") \
        .outputMode("update") \
        .start()

    # Await termination for the stream queries
    votes_per_candidate_to_kafka.awaitTermination()
    turnout_per_location_to_kafka.awaitTermination()
