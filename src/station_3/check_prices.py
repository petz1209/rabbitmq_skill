from queue.consumer import Consumer
from queue.producer import publish
from station_3.models import Order


products_db = [{"name": "coca-cola", "price": 1.99},
               {"name": "fanta", "price": 1.29},
               {"name": "redbull", "price": 1.29}
               ]


def check_prices(_input: dict):
    """The business Logic function represents what the callback is going to produce"""
    order = Order(**_input)
    print(order)
    if order.tries >= 3:
        return handle_error()
    status = True
    price = 0.0
    for product in order.products:
        price += get_price(product, products_db)

    if status:
        print(f"order: {order.order_id} total price: {price}")
        publish("contract", {"tries": 0, "order_id": order.order_id, "monitoring_date": _input["monitoring_date"], "total_price": price})


def get_price(product, products_db) -> float:
    for p in products_db:
        if p["name"] == product.name:
            return p["price"]
    return 0.0


def handle_error():
    print("WTF")
    return False


if __name__ == '__main__':

    consumer = Consumer(queue="prices", logic=check_prices)
    consumer.consume()
