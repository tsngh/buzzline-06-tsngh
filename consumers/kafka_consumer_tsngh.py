"""
kafka_consumer_tsngh.py

Consume json messages from a live data file. 
Insert the processed messages into a database.

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


Database functions are in consumers/db_sqlite_case.py.
Environment variables are in utils/utils_config module. 
"""

#####################################
# Import Modules
#####################################

import json
import os
import pathlib
import sys
import time
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.cm as cm
from matplotlib.animation import FuncAnimation
from collections import defaultdict

from kafka import KafkaConsumer

import utils.utils_config as config
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger
from utils.utils_producer import verify_services, is_topic_available

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from consumers.db_sqlite_tsngh import init_db, insert_message


#####################################
# Function to process a single message
# #####################################

sentiment_data = defaultdict(list)

# Set up the plot
fig, ax = plt.subplots(figsize=(12, 6))
plt.ion()

def update_chart(frame):
    ax.clear()
    categories = list(sentiment_data.keys())
    avg_sentiments = [sum(sentiments)/len(sentiments) if sentiments else 0 for sentiments in sentiment_data.values()]

    # Create a color map
    colors = cm.rainbow(np.linspace(0, 1, len(categories)))

    bars = ax.bar(categories, avg_sentiments, color=colors)
    ax.set_xlabel("Categories")
    ax.set_ylabel("Average Sentiment")
    ax.set_title("Real-Time Average Sentiment by Category - Winter Activities")
    ax.set_ylim(0, 1)

    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.2f}',
                ha='center', va='bottom')

    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()

def process_message(message: dict) -> dict:
    logger.info(f"Processing message: {message}")
    try:
        processed_message = {
            "message": message.get("message"),
            "author": message.get("author"),
            "timestamp": message.get("timestamp"),
            "category": message.get("category"),
            "sentiment": float(message.get("sentiment", 0.0)),
            "keyword_mentioned": message.get("keyword_mentioned"),
            "message_length": int(message.get("message_length", 0)),
            "season": message.get("season", "Winter"),
            "average_temp": message.get("average_temp")
        }
        
        # Update sentiment data for visualization
        category = processed_message["keyword_mentioned"]
        sentiment = processed_message["sentiment"]
        sentiment_data[category].append(sentiment)
        
        logger.info(f"Processed message: {processed_message}")
        return processed_message
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return None

def consume_messages_from_kafka(topic, kafka_url, group, sql_path, interval_secs):
    logger.info("Step 1. Verify Kafka Services.")
    try:
        verify_services()
    except Exception as e:
        logger.error(f"ERROR: Kafka services verification failed: {e}")
        sys.exit(11)

    logger.info("Step 2. Create a Kafka consumer.")
    try:
        consumer = create_kafka_consumer(
            topic,
            group,
            value_deserializer_provided=lambda x: json.loads(x.decode("utf-8")),
        )
    except Exception as e:
        logger.error(f"ERROR: Could not create Kafka consumer: {e}")
        sys.exit(11)

    logger.info("Step 3. Verify topic exists.")
    try:
        if not is_topic_available(topic):
            raise ValueError(f"Topic '{topic}' does not exist.")
        logger.info(f"Kafka topic '{topic}' is ready.")
    except Exception as e:
        logger.error(f"ERROR: Topic '{topic}' does not exist or could not be verified: {e}")
        sys.exit(13)

    logger.info("Step 4. Process messages.")
    try:
        last_update_time = time.time()
        for message in consumer:
            processed_message = process_message(message.value)
            if processed_message:
                insert_message(processed_message, sql_path)
                logger.info(f"Inserted message into database: {processed_message}")
            
            # Update chart every 5 seconds
            current_time = time.time()
            if current_time - last_update_time >= 5:
                plt.draw()
                plt.pause(0.001)
                last_update_time = current_time

    except Exception as e:
        logger.error(f"ERROR: Could not consume messages from Kafka: {e}")
        raise
    finally:
        if consumer:
            consumer.close()
            logger.info("Kafka consumer closed.")

def main():
    try:
        topic = config.get_kafka_topic()
        kafka_url = config.get_kafka_broker_address()
        group_id = config.get_kafka_consumer_group_id()
        interval_secs = config.get_message_interval_seconds_as_int()
        sqlite_path = config.get_sqlite_path()
        
        if sqlite_path.exists():
            sqlite_path.unlink()
        
        init_db(sqlite_path)
        
        # Set up the sentiment analysis plot
        ani = FuncAnimation(fig, update_chart, interval=5000)  # Update every 5 seconds
        
        plt.show(block=False)  # Show the plot without blocking
        consume_messages_from_kafka(topic, kafka_url, group_id, sqlite_path, interval_secs)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        logger.info("Consumer shutting down.")
        plt.close()

if __name__ == "__main__":
    main()
