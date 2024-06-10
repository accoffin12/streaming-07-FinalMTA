# streaming-07-Final
> Created by: A. C. Coffin | Date: 07 June 2024 | 
> NW Missouri State University | CSIS: 44671-80/81: Data Streaming | Dr. Case

# Overview: 
Weekly approximatly 5.5 million people use the Subway in New York City, annually the number of passengers recorded in 2019 was 1.598 billion. The NYC Subway system has a total of  24 subway lines and 472 stations throughout Manhattan, Brooklyn, Queens, and the Bronx with a staggering 665 miles of mainline track. The Subways in New York are a lifeline for residents and businesses, which opperates 24 hours a day, 7 days a week. It's one of the busiest transportation systems with a complex network of track, stops and hubs running through out 4 of the 5 Bouroughs. 

This project was designed to collect data from a modified CSV file that will be streamed through RabbitMQ, a message broker to simulate the process of filtering and collecting data from a data stream. There are several goals that we will be fullfilling:

* Filtering out the data based on stations on the Number 7 Train Line.
* Creating an alert for when a station is busy based on the number of passengers at it for Grand Central.
* Creating another alert for if a station is busy for Hunters Point. 

Two of these will require the use of windowing to create alerts, and the first will be an example of collecting data and exporting it to CSV. 
Traditionally Log files are not uploaded to GitHub, however, as this is a streaming project these logs have been left as a way to demonstrate the success of the project.

# Table of Contents
1. [File List](File_List)
2. [Machine Specs](Machine_specs)
3. [Prerequisites](Prerequisites)
4. [Before you Begin](Before_you_begin)
5. [Data Source](Data_Source)
    * [About the NYC Subway System](About_the_NYC_Subway_System)
6. [Modifications of Data](Modifications_of_Data)
7. [Creating an Environment & Installs](Creating_an_Enviroment_&_Installs)
8. [Method](Method)
9. [Executing the Code](Executing_the_Code)
10. [Results](Results)
11. [References](References)

# 1. File List
| File Name | Repo Location | Type |
| ----- | ----- | ----- |
| util_about.py | utils folder | python script |
| util_aboutenv.py | utils folder | python script |
| util_logger.py | utils folder | python script |
| Subway Map.pdf | Maps folder | PDF |
| SubwayMap.PNG | Maps folder | PNG |
| v2_emitter_of_tasks.py | BaseCode_Samples folder | python script |
| v2_listenining_worker.py | BaseCode_Samples folder | python script |
| requriements.txt | main repo | text doc |
| MTA_SubwayHR_June22.csv | main repo | CSV |
| MTA_SubwayW1Feb22.csv | main repo | CSV |
| aboutenv.txt | utils\util_outputs | text |
| util_about.txt | utils\util_outputs | text |
# 2. Machine Specs

# 3. Prerequisites
1. Git
2. Python 3.7+ (3.11+ preferred)
3. VS Code Editor
4. VS Code Extension: Python (by Microsoft)
5. RabbitMQ Server Installed and Running Locally
6. Anaconda Installed

# 4. Before you Begin
1. Fork this starter repo into your GitHub.
2. Clone your repo down to your machine.
3. View / Command Palette - then Python: Select Interpreter
4. Select your conda environment.

# 5. Data Source

The Metropolitan Transportation Authority(MTA) is responsible for all public transport in New York City and collects data in batches by the hour. This batching creates counts for the number of passengers boarding a subway at a specific station. It also provides data concerning payment, geography, time, date, and location of moving populations based on stations. This data was collected from February 2022 to May 4, 2024. 

MTA Data is readily available from New York State from their Portal.

NYC MTA Data for Subways: https://data.ny.gov/Transportation/MTA-Subway-Hourly-Ridership-Beginning-February-202/wujg-7c2s/about_data

## 5a. About the NYC Subway System

The New York City Subway system has 24 subway lines and 472 stations throughout Manhattan, Brooklyn, Queens, and the Bronx. Staten Island does not have a subway system but a ferry system and an above-ground train. The lines are listed in the chart based on their Line Reference. Some Lines do have local express services that share a line but stop at different stations. For the full MTA Subway Map view [Subway Map.pdf](Maps/Subway%20Map.pdf).

In 2019 an average of 5.5 million people ride the subway weekly. According to the MTA the anual rideship is 1.598 billion people in 2019. There are seven numbered routes and 15 letter routes, not including the shuttles. The system consists of 6,684 subway cars and 665 miles of mainline track. 

| Line Reference | Line Name | Area of NYC |
| ----- | ----- | ----- |
| 1, 2, 3 | Red Line | Runs along the west side of Manhattan |
| 4, 5, 6 | Green Line | East side of Manhattan and parts of the Bronx |
| 7 | Flushing Line | Connects manhattan to Queens |
| A, C | Blue Line | Runs from norther Manhattan through Brooklyn |
| B, D | Orange Line | Connects Manhattan to Brooklyn |
| E, F, M | Purple Line | Serves Queens and Manhattan |
| G | Light Green Line | Connects Brooklyn and Queens |
| J, Z | Brown Line | Runs through Brookelyn and into Queens |
| L | Gray Line | Connects Manhattan and Brooklyn |
| N, Q, R | Yellow Line | serves Manhattan, Brooklyn and Queens |
| S | 42nd Street Shuttle | Short Shuttle line in Manhattan |
| W | White Line | Runs between Manhattan and Queens |
| Z | Jamaica Line | Connects Brooklyn and Queens |

![MTA Subway Map](Maps/SubwayMap.PNG)

In 2019 the Busiest subway stations Accordign to the MTA were as Follows:
| Rank | Station/Complex | Lines | Annual ridership |
| ----- | ----- | ----- | ----- |
| 1 | Times Sq-42St/42 St | N, Q, R, W, S, 1, 2, 3, 7, A, C, E | 65,020,294 |
| 2 | Grand Central-42 St | S, 4, 5, 6, 7 | 45,745,700 |
| 3 | 34 St-Herald Sq | B, D, F, M, N, Q, R, W | 39,385,436 |
| 4 | 14 St-Union Sq | L, N, Q, R, W, 4, 5, 6 | 32,385,260 |
| 5 | Fulton St | A, C, J, Z, 2, 3, 4, 5 | 27,715,365 |
| 6 | 34 St-Penn Station | 1, 2, 3 | 25,967,676 |
| 7 | 34 St-Penn Station | A, C, E | 25,631,364 |
| 8 | 59 St-Columbus Circle | A, B, C, D, 1 | 23,040,650 |
| 9 | Chambers St, WTC /Park Pl/Cortlandt | A, C, E, 2, 3, R, W | 20,820,549|
| 10 | Lexington Av-53 St/51 St | E, M, 6 | 18,957,465 |


# 6. Modifications of Data
A secondary file containing the data utilized in this repo is located in the main repo. This variation of the altered file was modified by selecting data from week 1 of February 2022. The first week of February 2022 data was selected as it contained an example of each station multiple times over the period. Due to the size of the dataset it is not possible to upload the entire CSV to github. 

NOTE TO SELF: KEEP SECONDARY JUNE DATA IN CASE ISSUES WITH FEB. 

# 7. Creating an Environment & Installs
Before beginning this project two environments were made, one as a VS Code environment and the other as an Anaconda environment. RabbitMQ requires the Pika Library to function, to ensure that the scripts execute and create an environment in either VS Code or Anaconda.

While the Anaconda Environment is not necessary for this project it was utilized to ensure that the environments between VS Code and Anaconda were consistent when running the Producers and Consumers.

**If you have an Anaconda Enviroment that contains Pika or have Pika installed on the base be sure to activate that Env. For this example I will be using my previously created Anaconda Env - RabbitEnv.**

## 7a. Creating VS Code Enviroment
To create a local Python virtual environment to isolate our project's third-party dependencies from other projects. Use the following commands to create an environment, when prompted in VS Code set the .venv to a workspace folder and select yes.
```
python - m venv .venv # Creates a new environment
.venv\Scripts\activate # Activates the new environment
```
Once the environment is created install the following:
```
python -m pip install -r requirements.txt
```
For more information on Pika see the [Pika GitHub](https://github.com/pika/pika)

## 7b. Creating Anaconda Environment
To create an Anaconda environment open an Anaconda Prompt, the first thing that will pop up is the base. Then we are going to locate our folder, to do this type the following:
```
cd Dcuments\folder_where_repo_is\ 
cd Documents\ACoffinCSIS44671\streaming-07-Final # This is where the file is located on my computer
```
Once the folder has been located the line should look like this:
```
(base) C:\Users\Documents\folder_where_repo_is\streaming-07-Final>
(base) C:\Users\Tower\Documents\ACoffinCSIS44671\streaming-07-Final> # My File Path
```
To create an environment do the following:
```
conda create -n RabbitEnv # Creates the environment
conda activate RabbitEnv # Activates Environment
This will create the environment, if you want to deactivate it, enter: conda deactivate
```

Once the environment is created execute the following:
```
python --version # Indicates Python Version Installed
conda config --add channels conda-forge # connects to conda forge
conda config --set channel_priority strict # sets priority
install pika # library installation
```
Be sure to do each individually to install Pika in the environment. You have to use the forge to do this with Anaconda. Each Terminal should look similar to the following:

# 8. Method
In this assignment base code that was developed by Dr. Case in her repository, "[streaming-04-multiple-consumers](https://github.com/denisecase/streaming-04-multiple-consumers)" was utilized in combination with previous work completed in "streaming-04-bonus-ACoffin". Examples of base codes can be found in the BaseCode folder. 

## 8a. Producer(s):
Two Producer were created in this project. The first is a Producer that simulates a constant stream of a large amount of data that we will filter and the second was created in associationw with generating Alerts in the Consumers. These two Producers were not combined into a single project as they have different objectives.

### 8a1. MTA_Num7_Producer
This producer was created to send data to queue "07-Line" using the join method. The sleep time was set to every 60 seconds. Since the original data is generated hourly, using 60 seconds as a sleep time will simulate the passage of time with each seconds representing a minute that passes. Since we are looking for all the stations along the Number 7, the following is a list of station_complex_id and station_complex that meets our criteria. 

| station_complex_id | station_complex |
| 447 | Flushing-Main St (7) |
| 448 | Mets-Willets Point (7) |
| 449 | 111 St (7) |
| 450 | 103 St-Corona Plaza (7) |
| 451 | Junction Blvd (7) |
| 452 | 90 St-Elmhurst Av (7) |
| 453 | 82 St-Jackson Hts (7) |
| 455 | 69 St (7) |
| 456 | 61 St-Woodside (7) |
| 457 | 52 St (7) |
| 458 | 46 St-Bliss St (7) |
| 459 | 40 St-Lowery St (7) |
| 460 | 33 St-Rawson St (7) |
| 461 | Queensboro Plaza (7, N, W) |
| 463 | Hunters Point Av (7) |
| 464 | Vernon Blvd-Jackson Av (7) |

### 8a2. MTAProducer2
NAME WILL CHANGE, STILL PLANNING.

## 8b. Consumer(s)

# 9. Executing the Code

# 10. Results

# 11. References
- MTA Subway Data from NYC Open Portal, Downloaded: 04 May 2024: [https://data.ny.gov/Transportation/MTA-Subway-Hourly-Ridership-Beginning-February-202/wujg-7c2s/about_data](https://data.ny.gov/Transportation/MTA-Subway-Hourly-Ridership-Beginning-February-202/wujg-7c2s/about_data)
- MTA (14 April 2020). Subway and Bus facts 2019 Facts. Accessed: 07 June 2024: https://new.mta.info/agency/new-york-city-transit/subway-bus-facts-2019 
- Pika Documentation: [Pika GitHub](https://github.com/pika/pika)
- M4 Streaming multiple consumers by accoffin12: [streaming-04-multiple-consumers](https://github.com/accoffin12/streaming-04-multiple-consumers)
- M4-Bonus Repo Exploring MTA Data by accoffin12:   [streaming-04-bonus-ACoffin](ttps://github.com/accoffin12/streaming-04-bonus-ACoffin)
- M4 Streamining Multiple Consumers by Dr. Case: [streaming-04-multiple-consumers](https://github.com/denisecase/streaming-04-multiple-consumers)


