from sqlmodel import SQLModel, Field, Relationship


class InventoryItem(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    product_id: int 
    quantity: int
    status: str 


