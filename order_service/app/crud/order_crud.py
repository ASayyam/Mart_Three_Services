from sqlmodel import Session, select
from app.models.order_model import Order

def create_order(db: Session, order: Order):
    db.add(order)
    db.commit()
    db.refresh(order)
    return order

def get_order(db: Session, order_id: int):
    return db.get(Order, order_id)

def get_orders(db: Session):
    return db.exec(select(Order)).all()






