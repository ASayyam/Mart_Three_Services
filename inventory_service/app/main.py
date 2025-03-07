# main.py
from contextlib import asynccontextmanager
from typing import Annotated
from sqlmodel import Session, SQLModel
from fastapi import FastAPI, Depends, HTTPException
from typing import AsyncGenerator
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import asyncio
import json

from app import settings
from app.db_engine import engine
from app.models.inventory_model import InventoryItem
from app.crud.inventory_crud import add_new_inventory_item, delete_inventory_item_by_id, get_all_inventory_items, get_inventory_item_by_id
from app.deps import get_session, get_kafka_producer
from app.consumers.inventory_consumer import consume_inventory_messages
from app.topic import create_topic


def create_db_and_tables() -> None:
    SQLModel.metadata.create_all(engine)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    create_db_and_tables()
    print("Creating table!!")
    
    tasks = asyncio.create_task(consume_inventory_messages(
        settings.KAFKA_PRODUCT_TOPIC, 'broker:19092'))
    await create_topic(topic=settings.KAFKA_PRODUCT_TOPIC)
    create_db_and_tables()
    yield
    
    for task in tasks:
        task.cancel()
        await task

app = FastAPI(
    lifespan=lifespan,
    title="Hello World API with DB",
    version="0.0.1",
)


@app.get("/")
def read_root():
    return {"Hello": "Inventory Service"}


@app.post("/manage-inventory/", response_model=InventoryItem)
async def create_new_inventory_item(item: InventoryItem, session: Annotated[Session, Depends(get_session)], producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)]):
    """ Create a new inventory item and send it to Kafka"""
    item_dict = {field: getattr(item, field) for field in item.dict()}
    item_json = json.dumps(item_dict).encode("utf-8")
    print("item_JSON:", item_json)
    # await producer.send_and_wait("AddStock", item_json)
    await producer.send_and_wait(settings.KAFKA_PRODUCT_TOPIC, item_json)
    new_item = add_new_inventory_item(item, session)
    return new_item 


@app.get("/manage-inventory/all", response_model=list[InventoryItem])
def all_inventory_items(session: Annotated[Session, Depends(get_session)]):
    """ Get all inventory items from the database"""
    return get_all_inventory_items(session)


@app.get("/manage-inventory/{item_id}", response_model=InventoryItem)
def single_inventory_item(item_id: int, session: Annotated[Session, Depends(get_session)]):
    """ Get a single inventory item by ID"""
    try:
        return get_inventory_item_by_id(inventory_item_id=item_id, session=session)
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/manage-inventory/{item_id}", response_model=dict)
def delete_single_inventory_item(item_id: int, session: Annotated[Session, Depends(get_session)]):
    """ Delete a single inventory item by ID"""
    try:
        return delete_inventory_item_by_id(inventory_item_id=item_id, session=session)
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



