import pika
import json
import time
import random

# Settings for consumer
C_QUEUE = "letterbox"
# Settings for producer
P_QUEUE = "letterbox"
P_ROUTING_KEY = "letterbox"
P_EXCHANGE = "" # Default Exchange

rq_user = "guest"
rq_password = "guest"


# callback function for the consumer: -> what should be done when mesage is recieved
def on_message_recieved(ch, method, properties, body):
    job = json.loads(body)
    print(job)
    if job["tries"] == 3:
        print("Job was tried 3 times allready")
        print("======================================================================================================")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return None
    time.sleep(random.randint(2, 10))
    # increase tries by one, then publish job again
    job["tries"] += 1
    producer = Producer(queue=P_QUEUE, message=json.dumps(job), exchange=P_EXCHANGE, routing_key=P_ROUTING_KEY)
    producer.publish()
    ch.basic_ack(delivery_tag=method.delivery_tag)
    return None






class Consumer:
    def __init__(self, queue, ):
        self.queue = queue

    def consume(self):

        # connect to rabbiMQ
        _credentials = pika.PlainCredentials(username=rq_user, password=rq_password)
        _connection_parameters = pika.ConnectionParameters("localhost", credentials=_credentials, port=30003)
        _connection = pika.BlockingConnection(_connection_parameters)

        # Create a channel -> One connection can have many channels:
        _channel = _connection.channel()

        # First Of let's declare a queue
        _channel.queue_declare(queue=self.queue)
        _channel.basic_qos(prefetch_count=1)
        _channel.basic_consume(queue=self.queue, on_message_callback=on_message_recieved)

        print("started Consuming")
        _channel.start_consuming()


class Producer:
    def __init__(self, queue, message, exchange, routing_key):
        self.queue = queue
        self.message = message
        self.exchange = exchange
        self.routing_key = routing_key

    def publish(self):
        _credentials = pika.PlainCredentials(username=rq_user, password=rq_password)
        # connect to rabbiMQ
        _connection_parameters = pika.ConnectionParameters(host="localhost", credentials=_credentials)
        _connection = pika.BlockingConnection(_connection_parameters)

        # Create a channel -> One connection can have many channels:
        _channel = _connection.channel()

        # First Of let's declare a queue
        _channel.queue_declare(queue=self.queue)

        _channel.basic_publish(exchange=self.exchange, routing_key=self.routing_key, body=self.message)
        print(f"send message: {self.message}")
        _connection.close()


if __name__ == '__main__':
    c = Consumer(queue=C_QUEUE)
    c.consume()
