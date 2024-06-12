"""
    **** UNDER DEVELOPMENT ***

    ----
    
    This program sends a message to a queue on the RabbitMQ server.
    Make tasks harder/longer-running by adding dots at the end of the message.

    Author: Denise Case
    Date: January 15, 2023

"""

import pika
import sys
import webbrowser
import csv
import pandas as pd
from datetime import datetime
import time


from utils.util_logger import setup_logger

# Configuring the Logger:
logger, logname = setup_logger(__file__)

# Declare Variables:
host = 'localhost'
#input_file_name = 'MTA_SubwayHR_June22.csv'
input_file_name = 'MTA_SubwayW1Feb22.csv'



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
        channel = conn.channel()
        
        #Declare the exchange
        #channel.exchange_declare(exchange='SubwayExchange', exchange_type='direct')

        # List of Station IDS
        unique_station_ids = [45, 46, 47, 50, 354, 445, 446, 447, 448, 449, 450, 451, 452, 453, 455, 456, 457, 458, 459, 460, 461, 463, 464]
        channel.queue_declare(queue='LineQ_queue', durable = True)
        channel.queue_declare(queue= "Line5_queue", durable = True)
        channel.queue_declare(queue= "Line7_queue", durable= True)

        # Bind queues tot he exchange with routing keys
        #channel.queue_bind(exchange='SubwayExchange', routing_key='LineQ')
        #channel.queue_bind(exchange=)

       
        channel.basic_publish(exchange="", routing_key=queue_name, body=message)
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
    # Initializing directories to store data by subway line:
    subway_data_by_line = {}

    # Intializing empty lists to store extracted data
    transit_timestamps = []
    station_complex_ids = []
    station_complexes = []
    boroughs = []
    riderships = []

    with open(input_file_name, 'r', newline='', encoding='utf-8') as input_file:
        reader = csv.reader(input_file, delimiter=',')
        next(reader)
        # reading rows from csv
        for row in reader:
                # Seperate row into variables by column:
                #transit_timestamp, transit_mode, station_complex_id, station_complex, borough, payment_method, fare_class_category, ridership, transfers, latitude, longitude, Georeference = row
                #transit_timestamp=row[0]
                #station_complex_id = row[2]
                #station_complex = row[3]
                line = [4]
                #borough = row[4]
                #ridership = row[7]
                transit_timestamps.append(row[0])
                station_complex_ids.append(row[2])
                station_complexes.append(row[3])
                boroughs.append(row[5])
                riderships.append(row[7])

                # Append data to the appropriate list based on Subway Line
                if line not in subway_data_by_line:
                    subway_data_by_line[line]
                
                subway_data_by_line[line].append((transit_timestamp, station_complex_id, station_complex, borough, ridership))

                # Iterate through the tuples and send the message:
                for i in range(len(transit_timestamp)):
                     transit_timestamp = transit_timestamps[i]
                     station_complex_id = station_complex_ids[i]
                     station_complex = station_complexes[i]
                     borough = boroughs[i]
                     ridership = riderships[i]
                # logging the row being ingested
                #logger.info(f'{transit_timestamp=} - Row ingested: {station_complex_id=}, {station_complex=}, {borough=}, {ridership=}')
                # Convert the transit_timestamp_str into a datetime object in Unix:
                #transit_timestamp = datetime.strptime(transit_timestamp_str, "%m/%d/%y %H:%M:%S"). timestamp()
                # Pulling the desired info

                #Sending message to LineQ_queue:
                message =(f" {transit_timestamp}, {station_complex_id}, {station_complex},{line}, {borough}, {ridership}").encode()
                send_message("localhost", "LineQ_queue", message)
                logger.info(f"[x] sent {message} at {transit_timestamp}")

                # Sending message to Line5_queue
                message = (f" {transit_timestamp}, {station_complex_id}, {station_complex},{line}, {borough}, {ridership}").encode() 
                send_message("localhost", "Line5_queue", message)
                logger.info(f"[x] sent {message} at {transit_timestamp}")

                # Sending message to Line7_queue:
                message = (f" {transit_timestamp}, {station_complex_id}, {station_complex},{line}, {borough}, {ridership}").encode() 
                send_message("localhost", "Line7_queue", message)
                logger.info(f"[x] sent {message} at {transit_timestamp}")
            
            
        # set sleep for 60 seconds before reading next row to simulate an hour.
        #time.sleep(30)# Time was set to 30 seconds for Producer Test 1.
        time.sleep(60)

# A Keyboard Interrupt was added as the Process to pull all of the data from the stream is long. 
# Escape also adds note to the log.            
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

    # send the message to the queue
    main("localhost", input_file_name)