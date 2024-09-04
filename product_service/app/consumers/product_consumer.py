
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import json
from app.models.product_model import Product, ProductUpdate
from app.crud.product_crud import add_new_product, get_all_products, get_product_by_id, delete_product_by_id, update_product_by_id
from app.deps import get_session
from app.settings import KAFKA_CONSUMER_GROUP_ID_FOR_PRODUCT


async def consume_messages(topic, bootstrap_servers):
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=KAFKA_CONSUMER_GROUP_ID_FOR_PRODUCT,
    )

    await consumer.start()
    try:
        async for message in consumer:
            try:
                print("RAW")
                print(f"Received message on topic {message.topic}")

                product_data = json.loads(message.value.decode())
                print("TYPE", type(product_data))
                print(f"Product Data {product_data}")

                with next(get_session()) as session:
                    print("SAVING DATA TO DATABASE")
                    
                    # Attempt to insert product
                    try:
                        db_insert_product = add_new_product(
                            product_data=Product(**product_data), session=session)
                        session.commit()  # Ensure session commit
                        print("DB_INSERT_PRODUCT", db_insert_product)
                    except Exception as e:
                        print(f"Error inserting product into DB: {e}")
                        session.rollback()  # Rollback in case of failure
                        
            except Exception as e:
                print(f"Error processing message: {e}")
    finally:
        await consumer.stop()