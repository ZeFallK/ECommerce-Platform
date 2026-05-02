from fastapi import FastAPI
import random, logging, asyncio, json
from aiokafka import AIOKafkaConsumer
from contextlib import asynccontextmanager
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

resource = Resource(attributes={"service.name": "inventory"})

tracer_provider = TracerProvider(resource=resource)
trace_exporter = OTLPSpanExporter(endpoint="http://otel-collector:4317", insecure=True)
tracer_provider.add_span_processor(BatchSpanProcessor(trace_exporter))
trace.set_tracer_provider(tracer_provider)

log_provider = LoggerProvider(resource=resource)
log_exporter = OTLPLogExporter(endpoint="http://otel-collector:4317", insecure=True)
log_provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))
handler = LoggingHandler(logger_provider=log_provider)

logging.basicConfig( level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("inventory")
logger.addHandler(handler)

metric_exporter = OTLPMetricExporter(endpoint="http://otel-collector:4317", insecure=True)
metric_reader = PeriodicExportingMetricReader(metric_exporter)
meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
metrics.set_meter_provider(meter_provider)

inventory_db = {
    "LAPTOP-001": {"total": 50, "reserved": 0},
    "PHONE-002": {"total": 2, "reserved": 0},
    "MUG-003": {"total": 100, "reserved": 0}
}

async def listen_kafka():
    consumer = AIOKafkaConsumer(
        'orders.created', 
        'payments.processed',
        bootstrap_servers='kafka:9092',
        group_id='inventory-group',
        auto_offset_reset="earliest",
    )    
    logger.info("Tentative de connexion au broker Kafka (Consumer)...")
    while True:
        try:
            await consumer.start()
            logger.info("Connecté au broker Kafka, en écoute sur orders.created et payments.processed !")
            break
        except Exception:
            logger.error("Erreur de connexion Kafka, nouvelle tentative dans 5 secondes...")
            await asyncio.sleep(5)   
    try:
        async for msg in consumer:
            topic = msg.topic
            data = json.loads(msg.value.decode('utf-8'))            
            # orders.created envoie "product_id", mais on gère aussi si ce n'est pas présent
            product_id = data.get("product_id")
            qty = data.get("quantity", 0)

            # ── ACTION 1 : RÉSERVATION (Quand la commande est créée) ──
            if topic == 'orders.created':
                if product_id not in inventory_db:
                    logger.error(f"ALERTE : Commande {data.get('order_id')} demande un produit inconnu ({product_id}). Réservation annulée.")
                    continue

                stock_dispo = inventory_db[product_id]["total"] - inventory_db[product_id]["reserved"]
                if qty > stock_dispo:
                    logger.error(f"RUPTURE : Pas assez de stock pour {product_id}. Demandé: {qty}, Dispo: {stock_dispo}")
                    continue

                logger.info(f"Réservation de {qty} unité(s) de {product_id}.")
                inventory_db[product_id]["reserved"] += qty
                nouveau_dispo = inventory_db[product_id]["total"] - inventory_db[product_id]["reserved"]
                logger.info(f"Nouveau stock dispo pour {product_id} : {nouveau_dispo}")

            elif topic == 'payments.processed':
                product_id = data.get("product_id")
                qty = data.get("quantity", 0)

                if product_id and product_id in inventory_db:
                    inventory_db[product_id]["reserved"] -= qty
                    inventory_db[product_id]["total"]    -= qty
                    logger.info(
                        f"Paiement confirmé : {qty} unité(s) de {product_id} "
                        f"définitivement déduite(s). "
                        f"Stock restant : {inventory_db[product_id]['total'] - inventory_db[product_id]['reserved']}"
                    )
                else:
                    logger.warning(f"payments.processed reçu sans product_id valide pour la commande {data.get('order_id')}")             
    finally:
        await consumer.stop()

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Démarrage du service Inventory...")
    task = asyncio.create_task(listen_kafka())
    yield 
    task.cancel()

app = FastAPI(title="Inventory Service", version="1.0", lifespan=lifespan)
FastAPIInstrumentor.instrument_app(app, tracer_provider=tracer_provider, meter_provider=meter_provider)

@app.get("/stock/{product_id}")
async def get_stock(product_id: str):
    logger.info(f"Requête API reçue pour vérifier le stock de {product_id}")

    if product_id not in inventory_db:
        logger.warning(f"Produit {product_id} inconnu au catalogue.")
        return {
            "product_id": product_id,
            "error": "Produit introuvable",
            "in_stock": False
        }

    item = inventory_db[product_id]
    stock_dispo = item["total"] - item["reserved"]
    
    logger.info(f"Stock {product_id}: Total={item['total']}, Réservé={item['reserved']}, Dispo={stock_dispo}")    
    return {
        "product_id": product_id,
        "quantity": stock_dispo,
        "in_stock": stock_dispo > 0
    }
@app.get("/health")
async def health_check():
    return {"status": "OK", "service":"inventory"}