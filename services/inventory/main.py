from fastapi import FastAPI
import random, logging
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.resources import Resource

resource = Resource(attributes={"service.name": "inventory"})
provider = TracerProvider(resource=resource)
exporter = OTLPSpanExporter(endpoint="http://otel-collector:4317", insecure=True)
provider.add_span_processor(BatchSpanProcessor(exporter))
trace.set_tracer_provider(provider)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("inventory")

app = FastAPI(title="Inventory Service")
FastAPIInstrumentor().instrument_app(app)

@app.get("/stock/{product_id}")
async def get_stock(product_id: str):
    quantity = random.randint(0, 100)
    logger.info(f"Requête HTTP (API) reçue pour vérifier le stock du produit {product_id}")

    in_stock = quantity > 0
    logger.info(f"Produit {product_id} - Quantité disponible : {quantity} - En stock : {'Oui' if in_stock else 'Non'}")
    
    return {
        "product_id": product_id,
        "quantity": quantity,
        "in_stock": quantity > 0
    }
@app.get("/health")
async def health_check():
    return {"status": "OK", "service":"inventory"}