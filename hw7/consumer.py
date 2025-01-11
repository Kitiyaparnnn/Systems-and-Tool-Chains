import streamlit as st
from confluent_kafka import Consumer, Producer, KafkaError
import socket
import datetime

# Kafka settings
BROKER = 'localhost:9092'
REQUEST_TOPIC = 'youtube_requests'
RESPONSE_TOPIC = 'youtube_topic'
GROUP_ID = 'analytics'

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

# Function to create a Kafka producer
def create_kafka_producer(broker):
    conf = {
        'bootstrap.servers': broker,
        'client.id': socket.gethostname()
    }
    return Producer(conf)

# Streamlit app
def display_kafka_data():
    consumer = create_kafka_consumer(BROKER, GROUP_ID, RESPONSE_TOPIC)
    producer = create_kafka_producer(BROKER)

    st.write("Listening to Kafka topic:", RESPONSE_TOPIC)
    input_ids = st.text_input('Enter up to five YouTube video IDs (comma-separated)')
    button = st.button("Submit")

    if button and input_ids:
        # Send request to producer
        producer.produce(REQUEST_TOPIC, value=input_ids)
        producer.flush()
        st.write("Request sent. Waiting for response...")

        # Poll for the response
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                st.error(msg.error())
                break

            key = msg.key().decode('utf-8')
            value = msg.value().decode('utf-8').split(';')

            # Display the result
            st.markdown(f"🚀 **{key}** ")
            st.markdown(f"**ID:** {value[0]} **Title:** {value[1]}")
            st.markdown(f"**Likes:** {value[2]}")
            break

    consumer.close()

if __name__ == "__main__":
    st.title("YouTube Video Popularity Checker")
    display_kafka_data()