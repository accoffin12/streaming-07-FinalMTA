"""
Created by: A. C. Coffin
Date: 12 June 2024

*** CONSUME FOR LINE 7 DATA ***

A Consumer developed to recieve the selected data from the "MTA_ProducerV2.py", see notes in README about Original Project Concept.
Please note that this is one of THREE Consumers to run simultaneously, each producing thier own output CSV. 
Each of these Consumers does the process seperatly to focus on ensuring that the formatting is correct, and allowing for easier tracking of data based on Line.

This consumer does the following:
1. decodes the message from the queue associated with Line-5_queue
2. splits the original message with ',' to facilitate writing the CSV
3. Writes the data to a CSV file with only the desired columns from the producer.
    
    This program listens for work messages contiously. 
    Start multiple versions to add more workers. 

    Base Code Author: Denise Case
    Date: January 15, 2023

"""

import pika
import sys
import time
import pickle
from datetime import datetime
import csv
from utils.util_logger import setup_logger

# Configuring the Logger:
logger, logname = setup_logger(__file__)

# Variables
csv_file_path = 'Data_MTA_Line7.csv'
column_headers = ["transit_timestamp", "transit_mode", "station_complex_id", "station_complex", "Line", "borough", "payment_method", "fare_class_category", "ridership", "transfers", "latitude", "longitude", "Georeference"]

# Define Program functions
#--------------------------------------------------------------------------


# define a callback function to be called when a message is received
def callback(ch, method, properties, body):
    """ 
    Define behavior on getting a message.  This process utilizes pickle in order to decode the contents of the message.
    Message is then added to a CSV specifically for Line Q.
    """
    # decode the binary message body to a string
    subway_data = pickle.loads(body)
    logger.info(f'{subway_data["transit_timestamp"]},'
                    f'{subway_data["transit_mode"]},'
                    f'{subway_data["station_complex_id"]},'
                    f'{subway_data["station_complex"]},'
                    f'{subway_data["Line"]},'
                    f'{subway_data["borough"]},'
                    f'{subway_data["fare_class_category"]},'
                    f'{subway_data["ridership"]},'
                    f'{subway_data["transfers"]},'
                    f'{subway_data["latitude"]},'
                    f'{subway_data["longitude"]},'
                    f'{subway_data["Georeference"]}')


    # Write the filtered data into a new file
    with open ('Data_MTA_Line7.csv', 'a', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=column_headers)
        if file.tell() == 0:
            writer.writeheader()
        # Process each message
        writer.writerow(subway_data)
        logger.info(f'Row ingested: {subway_data["transit_timestamp"]},'
                    f'{subway_data["transit_mode"]},'
                    f'{subway_data["station_complex_id"]},'
                    f'{subway_data["station_complex"]},'
                    f'{subway_data["Line"]},'
                    f'{subway_data["borough"]},'
                    f'{subway_data["fare_class_category"]},'
                    f'{subway_data["ridership"]},'
                    f'{subway_data["transfers"]},'
                    f'{subway_data["latitude"]},'
                    f'{subway_data["longitude"]},'
                    f'{subway_data["Georeference"]}')

        # when done with task, tell the user
        print(" [x] Done.")
        logger.info(" [x] Done.")
        # acknowledge the message was received and processed 
        # (now it can be deleted from the queue)
        ch.basic_ack(delivery_tag=method.delivery_tag)


# define a main function to run the program
def main(hn: str = "localhost", qn: str = "Line-7_queue"):
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
    main("localhost", 'Line-7_queue')
