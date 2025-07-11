from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pika
import json
from pymongo import MongoClient
from typing import Dict, Any
import threading
from dotenv import load_dotenv
import os
import time

load_dotenv()
app = FastAPI()

MONGODB_URL = os.getenv("MONGO_URI")
client = MongoClient(MONGODB_URL)
db = client[os.getenv("DB_NAME")]
collection = db["test_collection"]

RABBITMQ_URL = "localhost"
queue_name = "mongo_queue"

class DataModel(BaseModel):
    text: str

class ResponseModel(BaseModel):
    analysis: Dict[str, Any]
    status: Dict[str, str]

def rabbitmq_consumer():
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_URL))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)
    
    def callback(ch, method, properties, body):
        try:
            data = json.loads(body)
            print(f"Received {data}")
            time.sleep(5)
            result = collection.insert_one(data)
            print(f"Insert into MongoDB: {result.inserted_id}")
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print(f"Error message: {e}")
    
    channel.basic_consume(queue=queue_name, on_message_callback=callback)
    print('Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

@app.on_event("startup")
def startup_event():
    thread = threading.Thread(target=rabbitmq_consumer)
    thread.daemon = True
    thread.start()

def process_text(text: str) -> Dict[str, Any]:
    words = text.split()
    return {
        "word_count": len(words),
        "character_count": len(text),
        "unique_words": len(set(words))
    }

@app.post("/data", response_model=ResponseModel)
def process_and_queue_text(item: DataModel):
    try:
        analysis = process_text(item.text)
        
        connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_URL))
        channel = connection.channel()
        channel.queue_declare(queue=queue_name, durable=True)
        
        message = json.dumps({"text": item.text})
        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
            ))
        connection.close()
        
        return {
            "analysis": analysis,
            "status": {"message": "Data processed and queued for storage"}
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))