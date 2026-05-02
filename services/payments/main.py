from fastapi import FastAPI
from pydantic import BaseModel
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from contextlib import asynccontextmanager
import uuid, json, asyncio, logging
from opentelemetry.sdk.resources import Resource
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
# Config des traces avec tempo
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
# config de logs otel pour loki
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
# config de metrics otel prometheus
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

resource = Resource(attributes={"service.name": "payments"})
provider = TracerProvider(resource=resource)
exporter = OTLPSpanExporter(endpoint="http://otel-collector:4317", insecure=True)
provider.add_span_processor(BatchSpanProcessor(exporter))
trace.set_tracer_provider(provider)

log_provider = LoggerProvider(resource=resource)
log_exporter = OTLPLogExporter(endpoint="http://otel-collector:4317", insecure=True)
log_provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))
handler = LoggingHandler(logger_provider=log_provider)

logging.basicConfig( level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("payments")
logger.addHandler(handler)

metric_exporter = OTLPMetricExporter(endpoint="http://otel-collector:4317", insecure=True)
metric_reader = PeriodicExportingMetricReader(metric_exporter)
meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
metrics.set_meter_provider(meter_provider)

producer = None
async def listen_kafka():
    consumer = AIOKafkaConsumer(
        'orders.created',
        bootstrap_servers='kafka:9092',
        group_id='payment-group',
       auto_offset_reset="earliest",
    )
    logger.info("Tentative de connexion au broker Kafka (Consumer)...")
    while True:
        try:
            await consumer.start()
            logger.info("Connecté au broker Kafka (Consumer)")
            break
        except Exception:
            logger.error("Erreur lors de la connexion au broker Kafka (Consumer), nouvelle tentative dans 5 secondes...")
            await asyncio.sleep(5)
   
    try:
        async for msg in consumer:
            commande = json.loads( msg.value.decode('utf-8'))
            montant = commande['quantity'] * 50.0
            logger.info(f"Commande interceptée : {commande['order_id']} (Client: {commande['customer_id']})")
            logger.info(f"Paiement simulé de {montant}€ validé.")

            event = {
                "order_id":    commande["order_id"],
                "customer_id": commande["customer_id"],
                "product_id":  commande["product_id"], 
                "quantity":    commande["quantity"],   
                "amount":      montant,
                "status":      "success",
            }
            await producer.send_and_wait("payments.processed", event)
            logger.info(f"Événement de paiement publié sur le topic 'payments.processed'.")            
    finally:
        await consumer.stop()

@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    logger.info("Démarrage du service Payments...")

    producer = AIOKafkaProducer(
        bootstrap_servers="kafka:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()
    logger.info("Producer Kafka connecté !")

    task = asyncio.create_task(listen_kafka())
    yield 
    task.cancel()
    await producer.stop()

app = FastAPI(title="Payment Service", version="1.0", lifespan=lifespan)
FastAPIInstrumentor.instrument_app(app, tracer_provider=provider, meter_provider=meter_provider)

class Payment(BaseModel):
    order_id: str
    amount: float

@app.post("/pay", status_code=201)
async def process_payment(payment: Payment):
    transaction_id = str(uuid.uuid4())
    logger.info(f"Requête HTTP (API) reçue pour payer la commande {payment.order_id}")

    return {
        "transaction_id": transaction_id, 
        "order_id": payment.order_id,
        "status": "success", 
        "message": "Payment processed successfully"
    }

@app.get("/health")
async def health_check():
    return {"status": "OK", "service":"payments"}