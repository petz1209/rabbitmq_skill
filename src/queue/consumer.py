import datetime

import pika
import json


# setup connection
credentials = pika.PlainCredentials('guest', 'guest')
connection_parameters = pika.ConnectionParameters(host="localhost", credentials=credentials)
connection = pika.BlockingConnection(connection_parameters)
# Create a channel -> One connection can have many channels:
channel = connection.channel()


class Consumer:
    """class that runs consumtion"""
    def __init__(self, queue, logic):
        self.queue = queue
        self.logic = logic

    def consume(self):
        # First Of let's declare a queue
        channel.queue_declare(queue=self.queue)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=self.queue, on_message_callback=self.callback)

        channel.start_consuming()
        channel.close()

    def callback(self, ch, method, properties, body):
        # do some cleanups of job items
        job = self.clean_job(json.loads(body))
        self.logic(job)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return True

    @staticmethod
    def clean_job(job):
        if "tries" not in job:
            job["tries"] = 1
        else:
            job["tries"] += 1

        if "monitoring_date" not in job:
            job["monitoring_date"] = datetime.date.today().strftime("%Y-%m-%d")
        return job


