import pika
import json


# setup connection
credentials = pika.PlainCredentials('guest', 'guest')
connection_parameters = pika.ConnectionParameters(host="localhost", credentials=credentials)
connection = pika.BlockingConnection(connection_parameters)
# Create a channel -> One connection can have many channels:
channel = connection.channel()


def consume(queue: str, callback):
    # connect to rabbiMQ

    # First Of let's declare a queue
    channel.queue_declare(queue=queue)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=queue, on_message_callback=callback)

    print("started Consuming")
    channel.start_consuming()
    channel.close()


# callback function for the consumer: -> what should be done when mesage is recieved
def callback(ch, method, properties, body):
    job = json.loads(body)
    print("======================================================================================================")
    # increase tries by one, then publish job again
    job["tries"] += 1
    print(job)
    ch.basic_ack(delivery_tag=method.delivery_tag)
    return True
