import socket
from confluent_kafka import Producer, Consumer, KafkaError
import os
from googleapiclient.discovery import build

# Kafka settings
BROKER = 'localhost:9092'
REQUEST_TOPIC = 'youtube_requests'
RESPONSE_TOPIC = 'youtube_topic'

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "cmu-key.json"
youtube = build('youtube', 'v3')

# Function to create a Kafka producer
def create_kafka_producer(broker):
    conf = {
        'bootstrap.servers': broker,
        'client.id': socket.gethostname()
    }
    return Producer(conf)

# Function to create a Kafka consumer
def create_kafka_consumer(broker, group_id, topic):
    conf = {
        'bootstrap.servers': broker,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'client.id': socket.gethostname()
    }
    consumer = Consumer(conf)
    consumer.subscribe([topic])
    return consumer

# Function to get video data
def get_most_popular_videos(video_list):
    request = youtube.videos().list(part="snippet,statistics", id=video_list, maxResults=5)
    response = request.execute()
    videos = []

    for item in response.get('items', []):
        video_title = item['snippet']['title']
        video_like = int(item['statistics'].get('likeCount', 0))
        video_id = item['id']
        video_data = {
            'id': video_id,
            'title': video_title,
            'likeCount': video_like,
        }
        videos.append(video_data)

    return videos

# Main function for producer to process requests
def process_requests():
    producer = create_kafka_producer(BROKER)
    consumer = create_kafka_consumer(BROKER, 'producer_group', REQUEST_TOPIC)

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        video_list = msg.value().decode('utf-8')
        print(f"Received video list: {video_list}")

        videos = get_most_popular_videos(video_list)
        if not videos:
            continue

        max_like = max(videos, key=lambda item: item['likeCount'])
        result = f"{max_like['id']}; {max_like['title']}; {max_like['likeCount']} likes"

        # Send the response back to the consumer
        producer.produce(RESPONSE_TOPIC, key="The most popular video", value=result)
        producer.flush()

if __name__ == "__main__":
    process_requests()
