import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, window
from pyspark.sql.types import StringType, StructType, StructField, FloatType, TimestampType
from transformers import pipeline

# Configuration
CONFIG = {
    "KAFKA": {
        "BROKER": os.environ.get("KAFKA_BROKER", "kafka:9092"),
        "TOPIC": os.environ.get("TOPIC_NAME", "youtube-comments"),
        "MAX_OFFSETS_PER_TRIGGER": "200"
    },
    "SPARK": {
        "APP_NAME": "YouTubeCommentsConsumer",
        "LOG_LEVEL": "WARN",
        "CHECKPOINT_LOCATION": "/tmp/checkpoint"
    },
    "MODELS": {
        "SENTIMENT": "distilbert-base-uncased",
        "TOXICITY": "unitary/toxic-bert"
    }
}

# Initialize Spark Session
spark = SparkSession.builder \
    .appName(CONFIG["SPARK"]["APP_NAME"]) \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel(CONFIG["SPARK"]["LOG_LEVEL"])

# Schema definition
schema = StructType([
    StructField("comment", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("video_id", StringType(), True)
])

# Initialize ML models
try:
    sentiment_classifier = pipeline("sentiment-analysis", model=CONFIG["MODELS"]["SENTIMENT"])
    toxicity_classifier = pipeline("text-classification", model=CONFIG["MODELS"]["TOXICITY"])
except Exception as e:
    print(f"Error loading models: {str(e)}")
    raise

# UDF definitions
@udf(returnType=FloatType())
def predict_engagement(comment):
    try:
        if not comment or not isinstance(comment, str):
            return 0.0
        
        comment = comment.lower()
        score = 0.5  # Base score
        
        # Length-based scoring
        if len(comment) < 10:
            return 0.1
        elif len(comment) > 100:
            score += 0.2
            
        # Content-based scoring
        if "?" in comment:
            score += 0.3
        if any(word in comment for word in ["love", "awesome", "amazing", "great"]):
            score += 0.4
            
        return min(score, 1.0)
    except Exception:
        return 0.0

@udf(returnType=StringType())
def classify_sentiment(comment):
    try:
        res = sentiment_classifier(comment)[0]
        return f"{res['label']} ({res['score']:.2f})"
    except Exception as e:
        return f"Error: {str(e)}"

@udf(returnType=StringType())
def detect_toxicity(comment):
    try:
        res = toxicity_classifier(comment)[0]
        return f"{res['label']} ({res['score']:.2f})"
    except Exception as e:
        return f"Error: {str(e)}"

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", CONFIG["KAFKA"]["BROKER"]) \
    .option("subscribe", CONFIG["KAFKA"]["TOPIC"]) \
    .option("maxOffsetsPerTrigger", CONFIG["KAFKA"]["MAX_OFFSETS_PER_TRIGGER"]) \
    .load()

# Process the stream
try:
    # Decode Kafka message
    decoded_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")
    
    # Process with ML models
    processed_df = decoded_df \
        .withColumn("sentiment", classify_sentiment(col("comment"))) \
        .withColumn("toxicity", detect_toxicity(col("comment"))) \
        .withColumn("engagement_score", predict_engagement(col("comment"))) \
        .withWatermark("timestamp", "5 minutes")
    
    # Write stream
    query = processed_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("checkpointLocation", CONFIG["SPARK"]["CHECKPOINT_LOCATION"]) \
        .trigger(processingTime="1 minute") \
        .start()
    
    query.awaitTermination()

except Exception as e:
    print(f"Error processing stream: {str(e)}")
    spark.stop()