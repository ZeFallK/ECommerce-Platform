from fastapi import FastAPI
import random

app = FastAPI(title="Inventory Service")

@app.get("/stock/{product_id}")
async def get_stock(product_id: str):
    quantity = random.randint(0, 100)
    
    return {
        "product_id": product_id,
        "quantity": quantity,
        "in_stock": quantity > 0
    }
@app.get("/health")
async def health_check():
    return {"status": "OK", "service":"inventory"}