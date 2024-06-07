"""
   Program will read in temepratures provided in 'smoker-temps.csv'.
   Will send temperatures to respective consumers based on what we are reading the temperature of (smoker, food_A, food_B).

   Author: Alex Coffin
   Date: May 30, 2024

   Copied and modified from streaming-04-multiple-consumers/v3_emitter_of_tasks.py
   Basic Steps:
   1. Decision to Open RabbitMQ Admin Pannel
   2. Send_message, which establishes creating of queues and RabbitMQ server connection
   3. Declare queues and set it up so that the queues can also delete messages, that way we can repeatedly use the same queue.
   4. Create exception handling for send_message and instruct it to close connection when the data has been fully streamed.
   5. Main function handles the csv file
   6. Instruct the reader to read the file, and keep the data in a similar structure.
   7. Once the data is read, instruct the reader on which information to pull and how to handle the information.
   8. Send each of the columns data as individual messages to a specific queue. 
   9. Exception handling for the Main function.
   10. Entry Point Creation.
"""

import pika
import sys
import webbrowser
import csv
from utils.util_logger import setup_logger
from datetime import datetime
import time

# Configuring the Logger:
logger, logname = setup_logger(__file__)


# Declaring variables:
host = 'localhost'
input_file_name = 'smoker-temps.csv'
smoker_queue = "01-smoker"
foodA_queue = "02-food-A"
foodB_queue = "03-food-B"

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
        smoker_queue (str): the name of the first queue
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
        #ch.queue_delete(smoker_queue)
        #ch.queue_delete(foodA_queue)
        #ch.queue_delete(foodB_queue)

        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(smoker_queue, durable=True)
        ch.queue_declare(foodA_queue, durable=True)
        ch.queue_declare(foodB_queue, durable=True)
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
        with open(input_file_name, 'r', newline='') as input_file:
            reader = csv.reader(input_file)
            next(reader)
            # reading rows from csv
            for row in reader:
                timestamp = row[0]
                smoker_temp = row[1]
                food_A_temp = row[2]
                food_B_temp = row[3]


                # Created for smoker_temp, using encode which encodes the data as a binary output
                if smoker_temp:
                    smoker_temp = float(smoker_temp)
                    # Using an f string to send data with timestamp
                    message = (f"{smoker_queue} Reading = {timestamp}; Smoker is temp: {smoker_temp} deg F.").encode()
                    send_message(host, "01-smoker", message)
                    # Prepare a binary message to stream:
                    logger.info(f"[x] sent {smoker_temp} {timestamp} to {smoker_queue}, {message}")
                # Created message for food_A_temp
                if food_A_temp:
                     food_A_temp = float(food_A_temp)
                     message = (f"{foodA_queue} Reading = Date: {timestamp}; Food-A is temp: {food_A_temp} deg F.").encode()
                     send_message(host, "02-food-A", message)
                     logger.info(f"[x] sent {food_A_temp} at {timestamp} to {foodA_queue}, {message}")
                # Created a message for food_B_temp
                if food_B_temp:
                    food_B_temp = float(food_B_temp)
                    message = (f"{foodB_queue} Reading = {timestamp}; Food-B is temp: {food_B_temp} deg F.").encode()
                    send_message(host, "03-food-B", message)
                    logger.info(f"[x] sent {food_B_temp} at {timestamp} to {foodB_queue}, {message}")

                # Set sleep for 30 seconds before reading the next row:
                time.sleep(30)
                #time.sleep(5) #Used to test Producer and Consumer Pairs

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