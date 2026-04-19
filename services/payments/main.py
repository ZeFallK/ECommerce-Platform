from fastapi import FastAPI
from pydantic import BaseModel
import uuid

app = FastAPI(title="Payment Service")

class Payment(BaseModel):
    order_id: str
    amount: float

@app.post("/pay", status_code=201)
async def process_payment(payment: Payment):
    transaction_id = str(uuid.uuid4())

    return {
        "transaction_id": transaction_id, 
        "order_id": payment.order_id,
        "status": "success", 
        "message": "Payment processed successfully"
    }

@app.get("/health")
async def health_check():
    return {"status": "OK", "service":"payments"}