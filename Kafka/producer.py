import json
import argparse
from googleapiclient.discovery import build
from kafka import KafkaProducer

# Import configuration from config.py
from config import config

# Use values from config dictionary
YOUTUBE_API_KEY = config["google_api_key"]
KAFKA_BROKER = config["KAFKA_BROKER"]
TOPIC_NAME = config["TOPIC_NAME"]

def get_youtube_comments(video_id, max_results=50):
    """
    Fetches YouTube comments for a given video ID.
    """
    youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
    results = youtube.commentThreads().list(
        part="snippet", videoId=video_id, maxResults=max_results, textFormat="plainText"
    ).execute()
    return [item["snippet"]["topLevelComment"]["snippet"]["textDisplay"] for item in results.get("items", [])]

def send_to_kafka(comments):
    """
    Sends the YouTube comments to the specified Kafka topic.
    """
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER, value_serializer=lambda v: json.dumps(v).encode("utf-8"))
    for comment in comments:
        producer.send(TOPIC_NAME, {"comment": comment})
    producer.flush()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--video_id", required=True, help="YouTube video ID")
    args = parser.parse_args()
    
    comments = get_youtube_comments(video_id=args.video_id)
    send_to_kafka(comments)
    print(f"Sent {len(comments)} comments to Kafka")
