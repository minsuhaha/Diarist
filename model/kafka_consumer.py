from confluent_kafka import Consumer, KafkaError
import os
from dotenv import load_dotenv
from openai import OpenAI
import json
import psycopg2

# Load environment variables
load_dotenv()

KAFKA_BROKER_URL = os.getenv('KAFKA_BROKER_URL')
# KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
KAFKA_TOPIC="create-diary"
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
POSTGRESQL_NAME = os.getenv('POSTGRESQL_NAME')
POSTGRESQL_USER = os.getenv('POSTGRESQL_USER')
POSTGRESQL_PWD = os.getenv('POSTGRESQL_PWD')
POSTGRESQL_HOST = os.getenv('POSTGRESQL_HOST')
POSTGRESQL_PORT = os.getenv('POSTGRESQL_PORT')

client = OpenAI(api_key=OPENAI_API_KEY)

def generate_description(diary_text, artist_style, emotion):
    query = f"{diary_text}라는 내용을 {artist_style}의 화풍으로 그려줘. 감정은 {emotion}이야."
    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "Create an art diary entry response reflecting a specific artist's style without naming the artist."},
            {"role": "system", "content": "Describe the scene you want to illustrate. Mention key characteristics of the style like impressionistic, vibrant colors, or soft lighting."},
            {"role": "user", "content": query}
        ],
    )
    return response.choices[0].message.content

def generate_image(description):
    response = client.images.generate(
        model="dall-e-3",
        prompt=description,
        size="1024x1024",
        quality="standard",
        n=1
    )
    return response.data[0].url

def save_diary_entry(user_id, emotion_id, artist_id, diary_date, text, image_url):
    conn = psycopg2.connect(
        dbname=POSTGRESQL_NAME,
        user=POSTGRESQL_USER,
        password=POSTGRESQL_PWD,
        host=POSTGRESQL_HOST,
        port=POSTGRESQL_PORT
    )
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO diary_entry (user_id, emotion_id, artist_id, diary_date, text, image_url) VALUES (%s, %s, %s, %s, %s, %s)",
        (user_id, emotion_id, artist_id, diary_date, text, image_url)
    )
    conn.commit()
    cursor.close()
    conn.close()

def process_message(message):
    try:
        data = json.loads(message.value().decode('utf-8'))
    except json.JSONDecodeError as e:
        print(f"Failed to decode JSON message: {message.value()}")
        print(f"Error: {e}")
        return
    
    user_id = data.get('user_id')
    emotion_id = data.get('emotion_id')
    artist_id = data.get('artist_id')
    diary_date = data.get('diary_date')
    text = data.get('text')

    if not all([user_id, emotion_id, artist_id, diary_date, text]):
        print("Missing data in message, skipping...")
        return

    description = generate_description(text, artist_id, emotion_id)
    image_url = generate_image(description)

    print(f"Generated image URL: {image_url}")
    
# def process_message(message):
#     data = json.loads(message.value())
#     user_id = data['user_id']
#     emotion_id = data['emotion_id']
#     artist_id = data['artist_id']
#     diary_date = data['diary_date']
#     text = data['text']

#     description = generate_description(text, artist_id, emotion_id)
#     image_url = generate_image(description)

#     save_diary_entry(user_id, emotion_id, artist_id, diary_date, text, image_url)

def consume():
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER_URL,
        'group.id': 'hanium',
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe([KAFKA_TOPIC])

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        process_message(msg)

    consumer.close()
