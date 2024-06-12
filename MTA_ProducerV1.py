"""
   Created by: A. C. Coffin
    Date: 10 June 2024

   This script was developed to pull only selected columns for the larger data stream. In this case we are interested in:
   1. transit_timestamp
   2. station_complex_id
   3. station_complex
   4. borough
   5. ridership

   By doing this, we are able to gather data to help us determine how many people are riding the subway at specific times, 
   from specific stations with their attached boroughs. 
   The first part of the script sets up the connection with the RabbitMQ server.
   The second function reads the CSV, selects the desired data, creates the message and then send the message to queue.

    Base Code Author: Denise Case
    Date: January 15, 2023

"""

import pika
import sys
import webbrowser
import csv
from datetime import datetime
import time


from utils.util_logger import setup_logger

# Configuring the Logger:
logger, logname = setup_logger(__file__)

# Declare Variables:
host = 'localhost'
input_file_name = 'MTA_SubwayW1Feb22.csv'
Num7Sub_queue = '07-Line'


# Define Program functions
#--------------------------------------------------------------------------

def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    print()
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        print()
        logger.info()

def send_message(host: str, queue_name: str, message: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
    """

    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()

        # Delete existing queus and declares them anew to clear the previous queue.
        #ch.queue_delete(Num7Sub_queue)
        
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(Num7Sub_queue, durable=True)
        # use the channel to publish a message to the queue
        # every message passes through an exchange
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        # print a message to the console for the user
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        logger.error(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()
     

def main(host: str, input_file:str):
    """
    Open a CSV and iterate through each row of the CSV to trun it to a list of dictionars (JSON format)
    Seperate processes by column and send message by calling the send message function.

    Parameters:
    host (str): Name of host or IP address fo the RabbitMQ server
    input_file_name (str): The location of the input file.

    Comments above the code are reffering to the code in the next line and its function.
    """
try:
    with open(input_file_name, 'r', newline='', encoding='utf-8') as input_file:
        reader = csv.reader(input_file, delimiter=',')
        next(reader)
        # reading rows from csv
        for row in reader:
                # Seperate row into variables by column:
                #transit_timestamp, transit_mode, station_complex_id, station_complex, borough, payment_method, fare_class_category, ridership, transfers, latitude, longitude, Georeference = row
                transit_timestamp=row[0]
                station_complex_id = row[2]
                station_complex = row[3]
                borough = row[4]
                ridership = row[7]

                # logging the row being ingested
                logger.info(f'{transit_timestamp=} - Row ingested: {station_complex_id=}, {station_complex=}, {borough=}, {ridership=}')
                # Convert the transit_timestamp_str into a datetime object in Unix:
                #transit_timestamp = datetime.strptime(transit_timestamp_str, "%m/%d/%y %H:%M:%S"). timestamp()
                # Pulling the desired info
                message =(f" {transit_timestamp}, {station_complex_id}, {station_complex}, {borough}, {ridership}").encode() 
                send_message(host, "07-Line", message)
                logger.info(f"[x] sent {message} at {transit_timestamp} to {Num7Sub_queue}")
            
            
        # set sleep for 60 seconds before reading next row to simulate an hour.
        #time.sleep(30)# Time was set to 30 seconds for Producer Test 1.
        time.sleep(60)

# A Keyboard Interrupt was added as the Process to pull all of the data from the stream is long. 
# Escape also adds note to the log.            
except KeyboardInterrupt:
        print()
        print(" User interrupted continuous listening process.")
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

    # send the message to the queue
    main("localhost", input_file_name)