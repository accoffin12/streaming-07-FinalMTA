"""
Created by: A. C. Coffin
Date 12 June 2024

*** Consumer V3 ***

This producer was designed to read through "Data_MTAAlerts.csv" a file was was modified from a generated output from ConsumerV2. 
The Producer will offer RabbitMQ, create a connection to RabbitMQ servers, read in the data from the CSV and then set up a message.
The Timestamps have to be altered because in this case we are using struct encoding. 
Structs were selected because it isn't as sensetive to version issues as pickle, and offered an opportunity to improve on this skill.

ONLY TWO stations are used due to time constraints.

---

Base Code Written By: Denise Case
Date: January 15, 2023
"""

import pika
import sys
import webbrowser
import csv
import struct
from utils.util_logger import setup_logger
from datetime import datetime
import time

# Configuring the Logger:
logger, logname = setup_logger(__file__)


# Declaring variables:
host = 'localhost'
input_file_name = 'Data_MTAAlerts.csv'
Station447_queue = "Station-447"
Station463_queue = "Station-463"


# Define Program functions
#--------------------------------------------------------------------------

def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    print()
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        print()



def send_message(host: str, queue_name: str, message: str):
    """
    publish the message the the desired queue.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
         (str): the name of the first queue
        foodA_queue (str): the name of the second queue
        foodB_queue (str): the name of the thrid queue
        message (str): the message to be sent to the queue
    """
    try:
        """Connect to RabbitMQ Server, return the connection and channel.
            Creating multiple queues to handle the required data.
            Queues are durable to ensure messages are delivered in order and queue will restart if disconnected."""
        # Creating a connection to the RabbitMQ Server:
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # Use the connection to create a communication channel
        ch = conn.channel()
        
        # Delete existing queues and declares them anew to clear previous queue information.
        # use the channel to declare a durable queue for each of the queues.
        #ch.queue_delete(Station463_queue)
        #ch.queue_delete(Station447_queue)
        

        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(Station447_queue, durable=True)
        ch.queue_declare(Station463_queue, durable=True)
        
        # publish to a queue:
        ch.basic_publish(exchange= "", routing_key=queue_name, body =message)
        # Exception handling should something go wrong.
    except KeyboardInterrupt:
         logger.info("KeyboardInterrupt. Stopping the program.")
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        logger.error(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()   


def main(host: str, input_file: str):
        """
        Open a CSV and iterate through each row of the CSV.
        Seperate three processes by column and send the individual messages to the corresponding queue, 
        by calling send_message function.

        Parameters:
        host (str): The host name or IP address of the RabbitMQ server
        input_file_name (str): the location of the input file.

        Comments above the code are reffering to the code in the next line and its function.
        """
try:
        with open(input_file_name, 'r', newline='', encoding='utf-8') as input_file:
            reader = csv.reader(input_file)
            next(reader)
            # reading rows from csv
            for row in reader:
                transit_timestamp_str = row[0]
                Station447 = row[1]
                Station463 = row[2]

                # Converting timestamp to "%m/%d/%y %H:%M"
                transit_timestamp = datetime.strptime(transit_timestamp_str, "%m/%d/%y %H:%M:%S")


                # Using an f string to send data with timestamp to send data to Station447_queue
                #message = (f"{Station447_queue} Reading = {transit_timestamp}; Ridership = {Station447}").encode()
                message = struct.pack('=QI', int(transit_timestamp.timestamp()), int(Station447))
                send_message(host, "Station-447", message)
                logger.info(f'[x] Sent: {message} to {Station447_queue}')

                # Using an f string to send data to Station463_queue
                #message = (f"{Station463_queue} Reading = {transit_timestamp}; Ridership = {Station463}").encode()
                message = struct.pack('=QI', int(transit_timestamp.timestamp()), int(Station463))
                send_message(host, "Station-463", message)
                logger.info(f'[x] Sent: {message} to {Station463_queue}')

                # Set sleep for 60 seconds before reading the next row:
                time.sleep(60)
                #time.sleep(5) # Altered to Test Connection
except KeyboardInterrupt:
        print()
        print(" User interrupted streaming process.")
        logger.info("KeyboardInterrupt. Stopping the Program")
        sys.exit(0)                
except FileNotFoundError:
         logger.error("CSV file not found")
         sys.exit(1)
except ValueError as e:
         logger.error(f"An unecpected error has occured: {e}")
         sys.exit(1)  
 
# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":  
    # ask the user if they'd like to open the RabbitMQ Admin site
    offer_rabbitmq_admin_site()
    main('localhost', input_file_name)