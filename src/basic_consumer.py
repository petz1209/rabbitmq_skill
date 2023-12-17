import time

import pika
import json


rq_host, rq_port = "localhost", 5672
rq_username, rq_pw = 'guest', 'guest'


def callback(ch, method, properties, body):
    print("consuming message...")
    time.sleep(9)
    payload = json.loads(body)
    print(f" [x] Notifying {payload}")
    print(f" [x] Done.")
    ch.basic_ack(delivery_tag=method.delivery_tag)


def consume():
    # the pika hearbeat is relevant because it works like a connection timeout. if a message is not acknoledged within
    # this time the server closes the connection -> timeout error
    credentials = pika.PlainCredentials(rq_username, rq_pw)
    connection_parameters = pika.ConnectionParameters(host=rq_host, port=rq_port, heartbeat=10,
                                                      blocked_connection_timeout=10)
    connection = pika.BlockingConnection(connection_parameters)

    channel = connection.channel()
    queue = channel.queue_declare("api")
    queue_name = queue.method.queue
    channel.queue_bind(exchange="test",
                       queue=queue_name,
                       routing_key="test.api")

    channel.basic_consume(on_message_callback=callback, queue=queue_name, auto_ack=False)
    channel.start_consuming()


if __name__ == '__main__':
    consume()
