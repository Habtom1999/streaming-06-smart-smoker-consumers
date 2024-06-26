"""
    This program listens for work messages continuously. 
    It is listening for messages from bbq_producer_smoker. It will recieve 
    messages that include the temperature of food B that is located in 
    the smoker. It will send an alert if the food has not warmed up by 
    at least a degree in 10 minutes.

    Habtom Woldu
    
    June 03, 2024

"""

import pika
import sys
import time
from collections import deque

FB_deque = deque(maxlen = 20)
alert = "Alert! Alert! Alert! Food B is stalled and not warming up!"


# define a callback function to be called when a message is received
def FoodB_callback(ch, method, properties, body):
    """ Define behavior on getting a message."""
    #splitting the data to isolate the temp
    FoodB_message =  body.decode().split(",")
    #creating a temp variable
    temp = [0]
    #converting the temp string to a float
    temp[0] = float(FoodB_message[1])
    #putting the data in the right side of the deque
    FB_deque.append(temp[0])
    #creating the food B alert
    if len(FB_deque) == 20:
        FBalert = FB_deque[19]-FB_deque[0]
        if FBalert < 1:
            print(alert)
    print(f" [x] Received food B temperature.  Food B temperature is {FoodB_message}")

 
    

    # acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)


# define a main function to run the program
def main_FoodB(hn: str = "localhost", qn: str = "task_queue"):
    """ Continuously listen for task messages on a named queue."""

    # when a statement can go wrong, use a try-except block
    try:
        # try this code, if it works, keep going
        # create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))

    # except, if there's an error, do this
    except Exception as e:
        print()
        print("ERROR: connection to RabbitMQ server failed.")
        print(f"Verify the server is running on host={hn}.")
        print(f"The error says: {e}")
        print()
        sys.exit(1)

    try:
        # use the connection to create a communication channel
        channel = connection.channel()

    

        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        channel.queue_declare(queue=qn, durable=True)

        # The QoS level controls the # of messages
        # that can be in-flight (unacknowledged by the consumer)
        # at any given time.
        # Set the prefetch count to one to limit the number of messages
        # being consumed and processed concurrently.
        # This helps prevent a worker from becoming overwhelmed
        # and improve the overall system performance. 
        # prefetch_count = Per consumer limit of unaknowledged messages      
        channel.basic_qos(prefetch_count=1) 

        # configure the channel to listen on a specific queue,  
        # use the callback function named callback,
        # and do not auto-acknowledge the message (let the callback handle it)
        channel.basic_consume( queue=qn, auto_ack=False, on_message_callback=FoodB_callback)

        # print a message to the console for the user
        print(" [*] Ready for work. To exit press CTRL+C")

        # start consuming messages via the communication channel
        channel.start_consuming()

    # except, in the event of an error OR user stops the process, do this
    except Exception as e:
        print()
        print("ERROR: something went wrong.")
        print(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print()
        print(" User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        print("\nClosing connection. Goodbye.\n")
       
        connection.close()


# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # call the main function with the information needed
    main_FoodB("localhost", "02-Food-B")