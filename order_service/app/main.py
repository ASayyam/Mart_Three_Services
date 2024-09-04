from fastapi import FastAPI, Depends, HTTPException
from sqlmodel import Session, SQLModel
from app.models.order_model import Order
from app.deps import get_session, get_kafka_producer
from app.crud.order_crud import create_order, get_order, get_orders
from app.db_engine import engine
import json
from typing import Annotated
from aiokafka import AIOKafkaProducer
from app.topic import create_topic
from app.consumers.order_consumer import consume_order_messages


from fastapi import FastAPI
from asyncio import get_event_loop
from contextlib import asynccontextmanager
from app import settings

def create_db_and_tables():
    SQLModel.metadata.create_all(engine)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize the database and create necessary tables
    create_db_and_tables()
    
    # Create Kafka topic for orders if necessary
    await create_topic(topic=settings.KAFKA_ORDER_TOPIC)
    
    # Setup Kafka consumer to consume order messages
    loop = get_event_loop()
    tasks = [
        loop.create_task(consume_order_messages(
            settings.KAFKA_ORDER_TOPIC, 'broker:19092'))
    ]
    
    yield
    
    # Cleanup tasks when shutting down
    for task in tasks:
        task.cancel()
        await task

# Create the FastAPI application with the customized lifespan
app = FastAPI(
    lifespan=lifespan,
    title="Order Service API",
    version="1.0.0",
)


# @app.post("/orders/", response_model=Order)
# def create_order_endpoint(order: Order, db: Session = Depends(get_session)):
#     return create_order(db, order)


@app.post("/manage-orders/", response_model=Order)
async def create_new_order(order: Order, session: Annotated[Session, Depends(get_session)], producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)]):
    """Create a new order and send it to Kafka"""
    
    # Convert the order to a dictionary and then to JSON
    order_dict = {field: getattr(order, field) for field in order.dict()}
    order_json = json.dumps(order_dict).encode("utf-8")
    print("Order_JSON:", order_json)
    
    # Produce the order message to Kafka
    await producer.send_and_wait(settings.KAFKA_ORDER_TOPIC, order_json)
    
    # Save the order to the database
    new_order = create_order(session, order)
    
    return new_order

@app.get("/orders/{order_id}", response_model=Order)
def get_order_endpoint(order_id: int, db: Session = Depends(get_session)):
    db_order = get_order(db, order_id)
    if db_order is None:
        raise HTTPException(status_code=404, detail="Order not found")
    return db_order

@app.get("/orders/", response_model=list[Order])
def get_orders_endpoint(db: Session = Depends(get_session)):
    return get_orders(db)





  

