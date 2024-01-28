import pika
import json
import time
import random
from threading import Thread

# Settings for consumer
C_QUEUE = "letterbox"
# Settings for producer
P_QUEUE = "letterbox"
P_ROUTING_KEY = "api"
P_EXCHANGE = "test" # Default Exchange
PORT = 5672
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


def do_work(body):
    print("do_work from thread")
    time.sleep(10)
    print("-" * 200)
    print(json.loads(body))

class Consumer:
    def __init__(self, queue):
        self.queue = queue

    def consume(self):
        while True:
            message = self.consume_single_message()
            do_work(message)

    def consume_single_message(self):
        # connect to rabbiMQ
        _credentials = pika.PlainCredentials(username=rq_user, password=rq_password)
        _connection_parameters = pika.ConnectionParameters("localhost", credentials=_credentials, port=PORT,
                                                           heartbeat=2,
                                                           # blocked_connection_timeout=1

                                                           )
        _connection = pika.BlockingConnection(_connection_parameters)

        # Create a channel -> One connection can have many channels:
        _channel = _connection.channel()
        # First Of let's declare a queue
        _channel.queue_declare(queue=self.queue)
        _channel.queue_bind(queue=C_QUEUE, exchange=P_EXCHANGE, routing_key=P_ROUTING_KEY)
        _channel.basic_qos(prefetch_count=1)
        print("started Consuming")

        # for loop consumer. as _channel.consume is actually a generator I can just run it as a for loop
        for method_frame, properties, body in _channel.consume(self.queue):
            _channel.basic_ack(delivery_tag=method_frame.delivery_tag)

            # Cancel the consumer and return any pending messages
            requeued_messages = _channel.cancel()
            print('Requeued %i messages' % requeued_messages)

            # Close the channel and the connection
            _channel.close()
            _connection.close()
            return body


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
