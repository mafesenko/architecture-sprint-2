from flask import Flask, jsonify, request
from kafka import KafkaProducer, KafkaConsumer
import json
import os
import threading
import time

app = Flask(__name__)

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
TOPICS = ['user-events', 'payment-events', 'movie-events']

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def consume_topic(topic):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    for msg in consumer:
        print(f"[{topic}] {msg.value}")

def start_consumers():
    for topic in TOPICS:
        threading.Thread(target=consume_topic, args=(topic,), daemon=True).start()

@app.route('/api/events/health', methods=['GET'])
def health():
    return jsonify({'status': True}), 200

@app.route('/api/events/movie', methods=['POST'])
def movie_event():
    data = request.json
    producer.send('movie-events', value={
        'type': 'movie',
        'data': data,
        'timestamp': time.time()
    })
    return jsonify({'status': 'success'}), 201

@app.route('/api/events/user', methods=['POST'])
def user_event():
    data = request.json
    producer.send('user-events', value={
        'type': 'user',
        'data': data,
        'timestamp': time.time()
    })
    return jsonify({'status': 'success'}), 201

@app.route('/api/events/payment', methods=['POST'])
def payment_event():
    data = request.json
    producer.send('payment-events', value={
        'type': 'payment',
        'data': data,
        'timestamp': time.time()
    })
    return jsonify({'status': 'success'}), 201

if __name__ == '__main__':
    time.sleep(15)
    start_consumers()
    app.run(host='0.0.0.0', port=8082)