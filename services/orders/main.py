from fastapi import FastAPI
from contextlib import asynccontextmanager
from aiokafka import AIOKafkaProducer
from pydantic import BaseModel
import uuid, json

producer = None
@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    await producer.start()
    yield
    await producer.stop()

app = FastAPI(title="Orders Service", version="1.0", lifespan=lifespan)

class Order(BaseModel):
    product_id: str
    customer_id: str
    quantity: int

@app.post("/orders/", status_code=201)
async def create_order(order: Order):
    order_id = str(uuid.uuid4())

# kafka ici à regarder doc
    event_message = {
            "order_id": order_id,
            "product_id": order.product_id,
            "quantity": order.quantity,
            "customer_id": order.customer_id,
            "status": "pending"
        }
    await producer.send_and_wait("orders.created", event_message)

    return {"order_id": order_id, "status": "pending", "message": "Order created successfully", "data": order}

@app.get("/health")
async def health_check():
    return {"status": "OK", "service":"orders"}