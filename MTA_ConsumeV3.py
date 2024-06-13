import pika
import sys
import time
from collections import deque
import struct
from datetime import datetime
from utils.util_logger import setup_logger

# Configuring the Logger:
logger, logname = setup_logger(__file__)

# Variables
# ----------------------------------------------------------------------------
# define the deque outside of functions, allowing them to be appended
# each reading is 1 minute apart to simulate the hourly readings
# We want to know if the station is busy every three hours
# 24 hours/3 hours with sleep time of 60 sec
# 8 readings per interval
Station447_deque = deque(maxlen=8) 
Station463_deque = deque(maxlen=8)
Station447_queue = "Station-447"
Station463_queue = "Station-463"

# Define Program functions
#--------------------------------------------------------------------------


# define a callback function to be called when a message is received for Station 447
def Station447_callback(ch, method, properties, body):
    """ Define behavior on getting a message."""
    # decode the binary message body to a string
    #print(f" [x] Received {body.decode()}")
    
    # acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    

    # Clean the body of the message to find the temperature:
    body_decode = struct.unpack('=QI',body)
    transit_timestamp, Station447 = body_decode
    
    # Converst the timestamp back to a string for logging
    transit_timestamp_str = datetime.fromtimestamp(transit_timestamp).strftime("%m/%d/%y %H:%M")
    logger.info(f"[Station 447]: {transit_timestamp_str}: {Station447} Passengers")

    # Add new ridership number to deque
    Station447_deque.append(Station447)

    # Objective, to know if the station is busy, more than 1000 people is busy
    # in 3 hours, if so a Station Alert is generated
    if len(Station447_deque) == Station447_deque.maxlen:
        if Station447_deque[0] - Station447 >= 1000:
            Station447_change = Station447_deque[0] - Station447
            logger.info(f'''
                        ************************ [STATION 447 BUSY ALERT!!!!] *****************************
                        * Station 447 is busier than normal
                        * In the past 3 hours ther have been {Station447_change} people
                        Please Adjsut your commute accordingly.
                        *************************************************************************
                        ''')
    # Acknowlege it was recieved and processed
    # Can now be deleted from queue
    ch.basic_ack(delivery_tag=method.delivery_tag)

def Station463_callback(ch, method, properties, body):
    """ Define behavior on getting a message. Including unpacking struct."""
    # decode the binary message body to a string
    #print(f" [x] Received {body.decode()}")
    logger.info(f"[x] Received")
    
    # acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    

    # Clean the body of the message to find the temperature:
    body_decode = struct.unpack('=QI',body)
    transit_timestamp, Station463 = body_decode
    
    # Converst the timestamp back to a string for logging
    transit_timestamp_str = datetime.fromtimestamp(transit_timestamp).strftime("%m/%d/%y %H:%M")
    logger.info(f"[Station 463]: {transit_timestamp_str}: {Station463} Passengers")

    # Add new ridership number to deque
    Station463_deque.append(Station463)

    # Objective, to know if the station is busy, more than 100 people is busy
    # in 3 hours, if so a Station Alert is generated
    if len(Station463_deque) == Station463_deque.maxlen:
        if Station463_deque[0] - Station463 >= 100:
            Station463_change = Station463_deque[0] - Station463
            logger.info(f'''
                        ************************ [STATION 463 BUSY ALERT!!!!] *****************************
                        * Station 463 is busier than normal
                        * In the past 3 hours ther have been {Station463_change} people
                        Please Adjsut your commute accordingly.
                        *************************************************************************
                        ''')
    # Acknowlege it was recieved and processed
    # Can now be deleted from queue
    ch.basic_ack(delivery_tag=method.delivery_tag)
                       


# define a main function to run the program
def main(hn: str = "localhost"):
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
        channel.queue_delete(Station447_queue)
        channel.queue_delete(Station463_queue)
        channel.queue_declare(Station447_queue, durable=True)
        channel.queue_declare(Station463_queue, durable=True)

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
        channel.basic_consume( queue="Station-447", on_message_callback=Station447_callback, auto_ack=False)
        channel.basic_consume( queue="Station-463", on_message_callback=Station463_callback, auto_ack=False)

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
    main("localhost")
