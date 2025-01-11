import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StringType, StructType, StructField, FloatType

from transformers import pipeline

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
TOPIC_NAME = os.environ.get("TOPIC_NAME", "youtube-comments")

spark = SparkSession.builder.appName("YouTubeCommentsConsumer").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([StructField("comment", StringType(), True)])

# Hugging Face pipelines
topic_classifier = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")
toxicity_classifier = pipeline("text-classification", model="unitary/toxic-bert")

# Engagement prediction model (simple rule-based regression for demo purposes)
def predict_engagement(comment):
    if len(comment) < 10:
        return 0.1  # Very short comments are low engagement
    if "?" in comment:
        return 0.8  # Comments with questions show high interest
    if "love" in comment or "awesome" in comment:
        return 0.9
    return 0.5  # Default average

engagement_udf = udf(predict_engagement, FloatType())

def classify_topic(comment):
    labels = ["technology", "gaming", "sports", "music", "entertainment", "politics", "others"]
    res = topic_classifier(comment, candidate_labels=labels, multi_label=False)
    return res["labels"][0]

def detect_toxicity(comment):
    res = toxicity_classifier(comment)[0]
    return f"{res['label']} ({res['score']:.2f})"

# Spark UDFs
topic_udf = udf(classify_topic, StringType())
toxicity_udf = udf(detect_toxicity, StringType())

# Read from Kafka
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC_NAME)
    .load()
)

# Decode the Kafka message
decoded_df = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")).select("data.*")

# Process the DataFrame
processed_df = decoded_df.withColumn("topic", topic_udf(col("comment"))) \
                         .withColumn("toxicity", toxicity_udf(col("comment"))) \
                         .withColumn("engagement_score", engagement_udf(col("comment")))

# Console output
query = processed_df.writeStream.outputMode("append").format("console").start()
query.awaitTermination()
