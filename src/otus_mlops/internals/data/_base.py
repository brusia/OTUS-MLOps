from pydantic import BaseModel
from datetime import date


class TransactionInfo(BaseModel):
    id: int
    timestamp: date
    customer_id: int
    terminal_id: int
    amount: float
    duration: int
    time_days: int
    fraud: int
    scenario: int
