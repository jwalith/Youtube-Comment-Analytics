
# YouTube Comment Analytics with Kafka, PySpark, and Hugging Face

This project provides a **real-time analytics pipeline** for YouTube comments, using:
- **Kafka** for streaming comments.
- **PySpark** for distributed data processing.
- **Hugging Face** models for **topic modeling** and **toxicity detection**.
- A simple **engagement prediction** function to estimate user interest.

We designed this project to perform **sentiment analysis** and **categorization** of YouTube comments using AI-powered models from Hugging Face, making it easy to classify content and detect toxicity in real time. The system can also estimate engagement levels by evaluating the nature of user comments.

---

## Table of Contents

1. Project Structure
2. Setup Instructions
3. Pipeline Overview
4. Analysis and AI Components
5. Future Enhancements
6. License

---

## Project Structure

```
Yotube Comment Analytics/
├── docker-compose.yml       # Docker configuration
├── config.py                # Configuration file with API key, Kafka settings, and video ID
├── kafka/
│   └── producer.py           # Kafka producer script to fetch and send YouTube comments
├── spark/
│   ├── Dockerfile            # PySpark consumer image configuration
│   ├── requirements.txt      # Dependencies for PySpark consumer
│   └── pyspark_app/
│       └── consumer.py       # PySpark consumer script for topic classification and toxicity detection
└── README.md                 # Project documentation
```

---

## Setup Instructions

### Pre-requisites
- **Docker** installed and running.
- **Python 3.8+** with `venv` or `conda` installed (for local development).
- A **YouTube Data API key**: Get this from [Google Cloud Console](https://console.cloud.google.com/).

### Steps

1. **Clone this repository:**
   ```bash
   git clone https://github.com/jwalith/Youtube-Comment-Analytics.git
   cd Youtube-Comment-Analytics
   ```

2. **Create and activate your Python virtual environment:**
   ```bash
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1  # Windows
   source .venv/bin/activate     # macOS/Linux
   ```

3. **Install dependencies (for the YouTube comments producer):**
   ```bash
   pip install google-api-python-client kafka-python python-dotenv
   ```

4. **Add your YouTube API key and video ID to `config.py`:**
   ```python
   config = {
       "google_api_key": "YOUR_API_KEY",
       "playlistId": "YOUR_PLAYLIST_ID",
       "KAFKA_BROKER": "localhost:9092",
       "TOPIC_NAME": "youtube-comments",
       "video_id": "YOUR_VIDEO_ID"
   }
   ```

5. **Start the Kafka and PySpark environment using Docker:**
   ```bash
   docker-compose up -d
   ```

6. **Run the YouTube comments producer:**
   ```bash
   python kafka/producer.py
   ```

7. **Monitor PySpark consumer logs (for real-time processing):**
   ```bash
   docker logs -f spark_consumer
   ```

---

## Pipeline Overview

1. **Kafka Producer (`producer.py`)**:
   - Fetches **YouTube comments** for a given **video ID** using the **YouTube Data API**.
   - Sends the comments to a **Kafka topic** (`youtube-comments`).

2. **PySpark Consumer (`consumer.py`)**:
   - Subscribes to the **Kafka topic**.
   - Performs:
     - **Topic Classification** using Hugging Face (`facebook/bart-large-mnli`).
     - **Toxicity Detection** using Hugging Face (`unitary/toxic-bert`).
     - **Engagement Prediction** (rule-based, based on comment features).

---

## Analysis and AI Components

- **Sentiment Analysis:**  Provides insights into the emotional tone of comments (e.g., positive, neutral, negative).
- **Toxicity Detection:** Identifies whether a comment contains offensive or harmful language.
- **Keyword Frequency Analysis: **  Calculates the most frequent keywords or phrases used in the comments to understand trending topics.

This project demonstrates the power of **AI-driven analytics** by combining **real-time streaming** with **natural language processing (NLP)**. The PySpark consumer leverages Hugging Face models to provide insights into the nature of user comments, detecting offensive content, assigning topics, and predicting user engagement.

---

## Future Enhancements

- Add **sentiment analysis** and **language translation**.
- Store processed results in a **database** or **dashboard** (e.g., Postgres, Elasticsearch).
- Improve engagement prediction using a **trained ML model**.
- Handle large datasets using **Spark clusters** with multiple worker nodes.

---


This project is licensed under the MIT License.

Feel free to submit **pull requests** or **issues** for improvements and feature requests.
