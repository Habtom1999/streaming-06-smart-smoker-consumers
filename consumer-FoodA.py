"""
    This program listens for work messages continuously. 
    It is listening for messages from bbq_producer_smoker.  
    It will recieve messages that include the temperature of food A that is located in 
    the smoker. It will also send an alert if the food is not warmed up
    by at least a degree in a 10 minute period.

    Habtom Woldu
    
    June 03,2024

"""

import pika
import sys
from collections import deque

FA_deque = deque(maxlen=20)
alert = "Alert! Alert! Alert! Food A's temperature is stalled and not warming up"

# Define a callback function to be called when a message is received
def FoodA_callback(ch, method, properties, body):
    """Define behavior on getting a message."""
    try:
        # Splitting the data to isolate the temp
        FoodA_message = body.decode().split(",")
        # Converting the temp string to a float and appending to deque
        temp = float(FoodA_message[1])
        FA_deque.append(temp)

        # Creating the food A alert
        if len(FA_deque) == 20:
            FAalert = FA_deque[-1] - FA_deque[0]
            if FAalert <= 1:
                print(alert)

        print(f" [x] Received the food A temperature. Food A temperature is {FoodA_message}")
        
    except ValueError as e:
        print(f"Error processing message: {e}")

    # Acknowledge the message was received and processed
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Define a main function to run the program
def main_FoodA(hn: str = "localhost", qn: str = "02-Food-A"):
    """Continuously listen for task messages on a named queue."""
    try:
        # Create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))
    except Exception as e:
        print(f"\nERROR: connection to RabbitMQ server failed.\nVerify the server is running on host={hn}.\nThe error says: {e}\n")
        sys.exit(1)

    try:
        # Use the connection to create a communication channel
        channel = connection.channel()

        # Use the channel to declare a durable queue
        channel.queue_declare(queue=qn, durable=True)

        # Set the prefetch count to limit the number of messages being consumed concurrently
        channel.basic_qos(prefetch_count=1)

        # Configure the channel to listen on a specific queue
        channel.basic_consume(queue=qn, auto_ack=False, on_message_callback=FoodA_callback)

        # Print a message to the console for the user
        print(" [*] Ready for work. To exit press CTRL+C")

        # Start consuming messages via the communication channel
        channel.start_consuming()
    except Exception as e:
        print(f"\nERROR: something went wrong.\nThe error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nUser interrupted continuous listening process.")
        sys.exit(0)
    finally:
        print("\nClosing connection. Goodbye.\n")
        try:
            connection.close()
        except:
            pass

# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions without executing the code below.
if __name__ == "__main__":
    main_FoodA("localhost", "02-Food-A")
