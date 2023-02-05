import json
from queue.producer import publish
from queue.consumer import consume


# callback function for the consumer: -> what should be done when mesage is recieved
def callback(ch, method, properties, body):
    job = json.loads(body)
    print("======================================================================================================")
    # increase tries by one, then publish job again
    print(job)
    ch.basic_ack(delivery_tag=method.delivery_tag)
    publish(queue="count", message={"count": 1})
    return True


if __name__ == '__main__':
    consume("2", callback)


