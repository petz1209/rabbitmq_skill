from queue.producer import publish
import uuid

if __name__ == '__main__':

    order_id = uuid.uuid4().hex
    publish("order", {"tries": 0, "order_id": order_id, "products": [{"name": "coca-cola"},
                                               {"name": "fanta"},
                                               {"name": "redbull"}
                                               ]}
            )