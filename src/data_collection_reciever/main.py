import pika
import json
import time
import random
from models.JobHandler import JobHandler


def main():
    def deliver_callback(ch, method, properties, body):
        message = json.loads(body)
        print(message)
        customerId = message.get("customerId")
        expectedJobs = message.get("jobCount")
        jobHandler = JobHandler(customerId, expectedJobs)
        pdf_message = jobHandler.handle()
        print(pdf_message)

    queue_listen = "jobs"
    queue_produce = "pdf"

    connection_parameters = pika.ConnectionParameters("localhost", port=30003)
    connection = pika.BlockingConnection(connection_parameters)

    listen_channel = connection.channel()
    produce_channel = connection.channel()

    listen_channel.queue_declare(queue_listen)
    produce_channel.queue_declare(queue_produce)

    listen_channel.basic_consume(queue_listen, on_message_callback=deliver_callback, auto_ack=True)
    listen_channel.start_consuming()


if __name__ == '__main__':
    main()











