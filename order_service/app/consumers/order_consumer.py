from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import json
from app.models.order_model import Order
from app.crud.order_crud import create_order, get_order, get_orders
from app.deps import get_session
from app.settings import KAFKA_CONSUMER_GROUP_ID_FOR_ORDER

async def consume_order_messages(topic, bootstrap_servers):
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=KAFKA_CONSUMER_GROUP_ID_FOR_ORDER,
    )

    await consumer.start()
    try:
        async for message in consumer:
            try:
                print(f"Received message on topic {message.topic}")

                order_data = json.loads(message.value.decode())
                print(f"Order Data: {order_data}")

                with next(get_session()) as session:
                    print("SAVING ORDER TO DATABASE")
                    
                    # Attempt to insert order
                    try:
                        new_order = create_order(session, Order(**order_data))
                        session.commit()  # Ensure session commit
                        print(f"Order inserted into DB: {new_order}")
                    except Exception as e:
                        print(f"Error inserting order into DB: {e}")
                        session.rollback()  # Rollback in case of failure
                        
            except Exception as e:
                print(f"Error processing message: {e}")
    finally:
        await consumer.stop()












# from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
# import json
# from app.models.order_model import Order
# from app.crud.order_crud import create_order, get_order, get_orders
# from app.deps import get_session
# from app.settings import KAFKA_CONSUMER_GROUP_ID_FOR_PRODUCT


# async def consume_messages(topic, bootstrap_servers):
#     consumer = AIOKafkaConsumer(
#         topic,
#         bootstrap_servers=bootstrap_servers,
#         group_id=KAFKA_CONSUMER_GROUP_ID_FOR_PRODUCT,
#     )

#     await consumer.start()
#     try:
#         async for message in consumer:
#             try:
#                 print("RAW")
#                 print(f"Received message on topic {message.topic}")

#                 product_data = json.loads(message.value.decode())
#                 print("TYPE", type(product_data))
#                 print(f"Product Data {product_data}")

#                 with next(get_session()) as session:
#                     print("SAVING DATA TO DATABASE")
                    
#                     # Attempt to insert product
#                     try:
#                         db_insert_product = add_new_product(
#                             product_data=Product(**product_data), session=session)
#                         session.commit()  # Ensure session commit
#                         print("DB_INSERT_PRODUCT", db_insert_product)
#                     except Exception as e:
#                         print(f"Error inserting product into DB: {e}")
#                         session.rollback()  # Rollback in case of failure
                        
#             except Exception as e:
#                 print(f"Error processing message: {e}")
#     finally:
#         await consumer.stop()
