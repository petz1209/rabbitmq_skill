from queue.consumer import Consumer
from queue.producer import publish
from station_2.models import Order

products_db = ["coca-cola", "fanta", "redbull"]


def check_warehouse(_input: dict) -> dict or False:
    """The business Logic function represents what the callback is going to produce"""
    order = Order(**_input)
    print(order)
    if order.tries >= 3:
        return handle_error()
    status = True
    for product in order.products:
        if product.name not in products_db:
            print(f"product {product.name} not available")
            status = False
            break
        else:
            print(f"product {product.name} is available")
    if status:

        publish("prices", {"tries": 0, "monitoring_date": _input["monitoring_date"],
                           "order_id": order.order_id,
                           "products": [p.dict() for p in order.products]})


def handle_error():
    print("WTF")
    return False


if __name__ == '__main__':

    consumer = Consumer(queue="order", logic=check_warehouse)
    consumer.consume()




