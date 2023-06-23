# The repository is aimed to build an ETL pipeline that loads JSON data from AWS S3, process it into parquet files and saves to another S3 bucket. 

## Assignment
A music streaming startup, Sparkify, has grown their user base and song database and want to move their data warehouse to a data lake. You are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

## Logical Data Model for the project
![Data Model for ETL AWS Sparkify project](/img/DataLake.jpg)

## Overview of the files in the repository
- etl.py reads data from S3, processes that data using Spark, and writes them back to S3
- dl.cfg contains AWS credentials
- img folder contains an image for the current file

# Running the project
## Pre-requisites
- In your AWS account, create an S3 bucket
- In 'dl.cfg' file, fill input and output data paths, and also AWS user credentials (user should have full access to S3 buckets that will be used)

## How to run the project
In the terminal window, run 'etl.py' file (e.g. `python etl.py`)

## Project results
The result tables look like in the following screenshots:

1. Songs input file, songs table:

![Songs table for ETL AWS Sparkify project](/img/1_songs_table.jpg)


2. Songs input file, artists table:

![Artists table for ETL AWS Sparkify project](/img/2_artists_table.jpg)


3. Logs input file, users table:

![Users table for ETL AWS Sparkify project](/img/3_users_table.jpg)


4. Logs input file, time table:

![Time table for ETL AWS Sparkify project](/img/4_time_table.jpg)


5. Songplays table:

![Songplays table for ETL AWS Sparkify project](/img/5_songplays_table.jpg)
