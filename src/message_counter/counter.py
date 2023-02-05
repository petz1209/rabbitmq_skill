import json
from queue.producer import publish
from queue.consumer import consume


# callback function for the consumer: -> what should be done when message is received
def callback(ch, method, properties, body):
    job = json.loads(body)
    print("======================================================================================================")
    ch.basic_ack(delivery_tag=method.delivery_tag)
    return True



if __name__ == '__main__':
    consume("count", callback)