import json

import pika


class JobHandler:
    def __init__(self, customerId, expectedJobs):
        self.customerId = customerId
        self.expectedJobs = expectedJobs
        self.queueName = "customerId_" + str(customerId)
        self.message_queue = list()

    def handle(self):
        def deliver_callback(ch, method, properties, body):
            message = json.loads(body)
            print(message)
            self.message_queue.append(message)
            print(len(self.message_queue))
            if len(self.message_queue) == self.expectedJobs:
                print(f"message count: {len(self.message_queue)}")
                channel.basic_cancel(consumer_tag=self.queueName)
                channel.close()
                connection.close()

        connection_parameters = pika.ConnectionParameters("localhost", port=30003)
        connection = pika.BlockingConnection(connection_parameters)
        channel = connection.channel()
        print(f"size: {len(self.message_queue)}")
        channel.queue_declare(self.queueName)
        channel.basic_consume(self.queueName, consumer_tag=self.queueName, on_message_callback=deliver_callback)
        channel.start_consuming()


        return self.message_queue
