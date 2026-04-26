from fastapi import FastAPI
from contextlib import asynccontextmanager
from aiokafka import AIOKafkaProducer
from pydantic import BaseModel
import uuid, json, logging, asyncio
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.resources import Resource

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("orders")

resource = Resource(attributes={"service.name": "orders"})
provider = TracerProvider(resource=resource)
exporter = OTLPSpanExporter(endpoint="http://otel-collector:4317", insecure=True)
provider.add_span_processor(BatchSpanProcessor(exporter))
trace.set_tracer_provider(provider)

producer = None
@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    logger.info("Demarrage du service Orders...")
                
    producer = AIOKafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    logger.info("Tentative de connexion au broker Kafka (Producer)...")

    await producer.start()
    logger.info("Producer Kafka connecte avec succes au cluster.")
    yield
    await producer.stop()
    logger.info("Producer deconnecte proprement.")

app = FastAPI(title="Orders Service", version="1.0", lifespan=lifespan)
FastAPIInstrumentor.instrument_app(app)

class Order(BaseModel):
    product_id: str
    customer_id: str
    quantity: int

@app.post("/orders/", status_code=201)
async def create_order(order: Order):
    order_id = str(uuid.uuid4())
    logger.info(f"Requete de creation de commande recue (Client: {order.customer_id}, Produit: {order.product_id})")

# kafka ici à regarder doc
    event_message = {
            "order_id": order_id,
            "product_id": order.product_id,
            "quantity": order.quantity,
            "customer_id": order.customer_id,
            "status": "pending"
        }
    await producer.send_and_wait("orders.created", event_message)
    logger.info(f"Evenement publie sur le topic 'orders.created' (Order ID: {order_id})")

    return {"order_id": order_id, "status": "pending", "message": "Order created successfully", "data": order}

@app.get("/health")
async def health_check():
    return {"status": "OK", "service":"orders"}