import pika
import uuid
import json

# setup connection
credentials = pika.PlainCredentials('guest', 'guest')
connection_parameters = pika.ConnectionParameters(host="localhost", credentials=credentials)
connection = pika.BlockingConnection(connection_parameters)


def publish(queue: str, message: dict, exchange: str = ""):
    # Create a channel -> One connection can have many channels:
    channel = connection.channel()
    # First Of let's declare a queue
    channel.queue_declare(queue=queue)

    message = json.dumps(message)
    channel.basic_publish(exchange=exchange, routing_key=queue, body=message)
    channel.close()
