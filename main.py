from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pika
import json
from motor.motor_asyncio import AsyncIOMotorClient
from typing import Dict
import asyncio
import threading
from dotenv import load_dotenv
import os

load_dotenv()
app = FastAPI()

MONGODB_URL = os.getenv("MONGO_URI")
client = AsyncIOMotorClient(MONGODB_URL)
db = client[os.getenv("DB_NAME")]
collection = db["test_collection"]

RABBITMQ_URL = "localhost"
queue_name = "mongo_queue"

class DataModel(BaseModel):
    data: str

def setup_rabbitmq_consumer():
    def start_consumer():
        connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_URL))
        channel = connection.channel()
        channel.queue_declare(queue=queue_name, durable=True)
        
        def callback(ch, method, properties, body):
            try:
                data = json.loads(body)
                print(f"Received {data}")
                
                sync_client = AsyncIOMotorClient(MONGODB_URL)
                sync_db = sync_client["test_db"]
                sync_collection = sync_db["test_collection"]
                
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                result = loop.run_until_complete(sync_collection.insert_one(data))
                print(f" Inserted into MongoDB with id: {result.inserted_id}")
                loop.close()
                
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                print(f"Error processing message: {e}")
        
        channel.basic_consume(queue=queue_name, on_message_callback=callback)
        print('Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()
    
    thread = threading.Thread(target=start_consumer)
    thread.daemon = True
    thread.start()

@app.on_event("startup")
async def startup_event():
    setup_rabbitmq_consumer()

@app.post("/data/", response_model=Dict[str, str])
async def create_data(item: DataModel):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_URL))
        channel = connection.channel()
        channel.queue_declare(queue=queue_name, durable=True)
        
        message = json.dumps({"data": item.data})
        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
            ))
        connection.close()
        
        return {"status": "Data sent to queue", "data": item.data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/data/", response_model=Dict[str, str])
async def get_latest_data():
    document = await collection.find_one(sort=[("_id", -1)])
    if document:
        return {"data": document.get("data", "")}
    return {"data": ""}