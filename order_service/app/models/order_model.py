from sqlmodel import SQLModel, Field
from typing import Optional

class Order(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    item_name: str
    quantity: int
    price: float


