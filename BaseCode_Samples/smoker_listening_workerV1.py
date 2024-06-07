"""
Created by: A. C. Coffin
Date: 05 June 2024

This Program is designed to collect mesages from the queue "01-smoker" for a Smart Smoker using the "temp_producerV1.py".
Each message is decoded, the temperature is collected and then added to the smoker_deque.
A seperate function to determine if the deque is full was added. 
Based on that information we compare the temperatures inside the deque to determine if the food has stalled. 
If Y then an Alert is issued.

The Process can be interrupted using Ctrl + C if an escape is needed.


Base Code Written By: Denise Case
    Date: January 15, 2023
"""

import pika
import sys
import time
from collections import deque
import re
from utils.util_logger import setup_logger

# Configuring the Logger:
logger, logname = setup_logger(__file__)

# define the deque outside of functions, allowing them to be appended
# each reading is 30 seconds apart, so the maxlen of each dequeue = 2 * window in minutes.\
# 5/2 = 2.5 minute window
smoker_deque = deque(maxlen=5) 


# Define Program functions
#--------------------------------------------------------------------------


# define a callback function to be called when a message is received
def callback(ch, method, properties, body):
    """ Define behavior on getting a message."""
    # decode the binary message body to a string
    #print(f" [x] Received {body.decode()}")
    logger.info(f"[x] Received: {body.decode()}")
    
    # acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)

    # Clean the body of the message to find the temperature:
    body_decode = body.decode('utf-8')
    temps = re.findall(r'Smoker is temp: (\d+\.\d+)', body_decode)
    temps_float = float(temps[0])
    smoker_deque.append(temps_float)

    # Objective, to know if the smoker temp decreases by more than 15 deg F
    # in 2.5 minutes, resulting in a SMOKER ALERT! being generated
    if len(smoker_deque) == smoker_deque.maxlen:
        if smoker_deque[0] - temps_float >= 15:
            smoker_change = smoker_deque[0] - temps_float
            logger.info(f'''
                        ************************ [SMOKER ALERT!!!!] *****************************
                        Smoker Temperature has fell by 15 deg F {smoker_change} in 2.5 minutes!
                        Please Check Fuel Source and Lid Closure!!!
                        *************************************************************************
                        ''')

                       


# define a main function to run the program
def main(hn: str = "localhost", qn: str = "01-smoker"):
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
        logger.error(f"ERROR: connection to RabbitMQ server failed. The error is {e}.")
        sys.exit(1)

    try:
        # use the connection to create a communication channel
        channel = connection.channel()

        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        channel.queue_delete(queue=qn)
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
        channel.basic_consume( queue=qn, on_message_callback=callback, auto_ack=False)

        # print a message to the console for the user
        print(" [*] Ready for work. To exit press CTRL+C")
        logger.info(" [*] Ready for work. To exit press CTRL+C")

        # start consuming messages via the communication channel
        channel.start_consuming()

    # except, in the event of an error OR user stops the process, do this
    except Exception as e:
        print()
        print("ERROR: something went wrong.")
        print(f"The error says: {e}")
        logger.error(f"Error: Something whent wrong. Error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print()
        print(" User interrupted continuous listening process.")
        logger.info("KeyboardInterrupt. Stopping the Program")
        sys.exit(0)
    finally:
        print("\nClosing connection. Goodbye.\n")
        logger.info("\nclosing connection. Goodby\n")
        connection.close()


# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # call the main function with the information needed
    main("localhost", "01-smoker")
