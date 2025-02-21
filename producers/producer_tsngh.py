"""
producer_tsngh.py

Stream JSON data to a file and - if available - a Kafka topic.

Example JSON message
{
    "message": "I just ice skating in Minneapolis! It was invigorating.",
    "author": "Judy",
    "timestamp": "2025-01-29 14:35:20",
    "category": "winter sports",
    "sentiment": 0.87,
    "keyword_mentioned": "skating",
    "message_length": 45
}

Environment variables are in utils/utils_config module. 
"""

#####################################
# Import Modules
#####################################

# import from standard library
import json
import os
import pathlib
import random
import sys
import time
from datetime import datetime

# import external modules
from kafka import KafkaProducer

# import from local modules
import utils.utils_config as config
from utils.utils_producer import verify_services, create_kafka_topic
from utils.utils_logger import logger

#####################################
# Stub Sentiment Analysis Function
#####################################


def assess_sentiment(text: str) -> float:
    """
    Stub for sentiment analysis.
    Returns a random float between 0 and 1 for now.
    """
    return round(random.uniform(0, 1), 2)


#####################################
# Define Message Generator
#####################################


def generate_winter_messages():
    """
    Generate a stream of JSON messages for winter activities in Minneapolis.
    """
    ADJECTIVES = ["snowy", "frigid", "invigorating", "festive", "cozy"]
    ACTIONS = ["tried", "enjoyed", "experienced", "participated in", "loved"]
    TOPICS = [
        "ice skating",
        "cross-country skiing",
        "snowshoeing",
        "ice fishing",
        "sledding",
        "St. Paul Winter Carnival",
        "Great Northern Festival",
        "Minneapolis Boat Show",
        "winter photography",
        "indoor museum visit"
    ]
    AUTHORS = ["Prince", "Bob", "Walter", "Judy"]
    KEYWORD_CATEGORIES = {
        "skating": "winter sports",
        "skiing": "winter sports",
        "snowshoeing": "winter sports",
        "fishing": "outdoor recreation",
        "sledding": "winter sports",
        "Carnival": "events",
        "Festival": "events",
        "Boat Show": "events",
        "photography": "arts",
        "museum": "indoor activities"
    }
    
    while True:
        adjective = random.choice(ADJECTIVES)
        action = random.choice(ACTIONS)
        topic = random.choice(TOPICS)
        author = random.choice(AUTHORS)
        message_text = f"I just {action} {topic} in Minneapolis! It was {adjective}."
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Find category based on keywords
        keyword_mentioned = next(
            (word for word in KEYWORD_CATEGORIES if word in topic), "other"
        )
        category = KEYWORD_CATEGORIES.get(keyword_mentioned, "other")

        # Simulate sentiment (replace with actual sentiment analysis if available)
        sentiment = random.uniform(0, 1)

        # Create JSON message
        json_message = {
            "message": message_text,
            "author": author,
            "timestamp": timestamp,
            "category": category,
            "sentiment": round(sentiment, 2),
            "keyword_mentioned": keyword_mentioned,
            "message_length": len(message_text),
            "season": "Winter",
            "average_temp": f"{random.randint(9, 29)}°F"
        }

        yield json_message


#####################################
# Define Main Function
#####################################


def main() -> None:

    logger.info("Starting Producer to run continuously.")
    logger.info("Things can fail or get interrupted, so use a try block.")
    logger.info("Moved .env variables into a utils config module.")

    logger.info("STEP 1. Read required environment variables.")

    try:
        interval_secs: int = config.get_message_interval_seconds_as_int()
        topic: str = config.get_kafka_topic()
        kafka_server: str = config.get_kafka_broker_address()
        live_data_path: pathlib.Path = config.get_live_data_path()
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    logger.info("STEP 2. Delete the live data file if exists to start fresh.")

    try:
        if live_data_path.exists():
            live_data_path.unlink()
            logger.info("Deleted existing live data file.")

        logger.info("STEP 3. Build the path folders to the live data file if needed.")
        os.makedirs(live_data_path.parent, exist_ok=True)
    except Exception as e:
        logger.error(f"ERROR: Failed to delete live data file: {e}")
        sys.exit(2)

    logger.info("STEP 4. Try to create a Kafka producer and topic.")
    producer = None

    try:
        verify_services()
        producer = KafkaProducer(
            bootstrap_servers=kafka_server,
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )
        logger.info(f"Kafka producer connected to {kafka_server}")
    except Exception as e:
        logger.warning(f"WARNING: Kafka connection failed: {e}")
        producer = None

    if producer:
        try:
            create_kafka_topic(topic)
            logger.info(f"Kafka topic '{topic}' is ready.")
        except Exception as e:
            logger.warning(f"WARNING: Failed to create or verify topic '{topic}': {e}")
            producer = None

    logger.info("STEP 5. Generate messages continuously.")
    try:
        for message in generate_winter_messages():
            logger.info(message)

            with live_data_path.open("a") as f:
                f.write(json.dumps(message) + "\n")
                logger.info(f"STEP 4a Wrote message to file: {message}")

            # Send to Kafka if available
            if producer:
                producer.send(topic, value=message)
                logger.info(f"STEP 4b Sent message to Kafka topic '{topic}': {message}")

            time.sleep(interval_secs)

    except KeyboardInterrupt:
        logger.warning("WARNING: Producer interrupted by user.")
    except Exception as e:
        logger.error(f"ERROR: Unexpected error: {e}")
    finally:
        if producer:
            producer.close()
            logger.info("Kafka producer closed.")
        logger.info("TRY/FINALLY: Producer shutting down.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
