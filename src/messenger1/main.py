import json
from queue.producer import publish
from queue.consumer import consume


# callback function for the consumer: -> what should be done when message is received
def callback(ch, method, properties, body):
    job = json.loads(body)
    print("======================================================================================================")
    print(properties)
    # increase tries by one, then publish job again
    print(job)
    ch.basic_ack(delivery_tag=method.delivery_tag)
    return True


if __name__ == '__main__':

    while True:
        txt = input("add message: ")
        if txt == "0":
            break
        message = {"sender": "messenger1", "message": txt}
        publish("2", message)
    print("exited program")
