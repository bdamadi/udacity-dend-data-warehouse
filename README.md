# Project: Data Warehouse

## Project Summary

This project is to build an ETL pipeline for analysis songs and users
activity on the specified music streaming app.

The given data set contains data of song data files and log files
stored in AWS S3. The ETL pipeline firstly transfers song data and
log data from this dataset into Redshift staging tables. It then
transform data for these staging tables into set of dimensional tables
for using by analytics team.

In the provided dataset, a song data file contains information of a 
song and its artist, and a log file records user activity on the app.
Each activity contains information that describes:

* The user who performs the action
* When the action is performed
* Which song (and its relevent data) the user was listenning.

For this analysic purpose, the fact table mainly records "song play"
action which contains songplay_id, start_time, user_id, level, song_id,
artist_id, session_id, location, and user_agent for a log record.

Dimension tables are for storing users in the app, songs and artists in
music database, and timestamp that broken down into detail resolution,
including hour, day, week, month, year, weekday.

## Database schema

### Fact table

`songplays` table stores records in log data associated with song plays.
Each record in a log file is populated as a recod in this table. There are
following design decisions I have made for this table:

* Primary key: using Redshift's IDENTITY to generate auto-increment
number for primary key instead of using `start_time` timestamp because
there may be multiple events happened at the same time.

### Dimension tables

`songs` table stores song records which are extracted from song data
file. `song_id` is used as primary key.

`artists` table stores artist records which are extracted from song data
file. `artist_id` is used as primary key. Because a single artist may
associated with multiple songs, populating artists records ignores
records if `artist_id` already exists in `artists` table.

`users` table stores user records, extracted from each log record from
log files. `user_id` is used as primary key. Because a single user may
associated with multiple activities, there will be conflicted in user_id
when populating user records. In this case, the insert query resolve
this conflict by updating user's level to the new value because
it likely to have user's level upgrade/downgrade over time.

`time` table stores broken-down timestamps from log records. The
timestamp value is used as primary key. Because multiple activities may
happen at the same timestamp, populating time records ignores
records if `start_time` already exists in `time` table.

### Staging tables

Song data is transfered from the song data JSON files into 
`staging_songs` table in Redshift. This table has all columns from
song JSON object.

Log events are transfered from the log data JSON files into
`staging_events` table in Redshift. This table contains columns to 
capture neccessary log fields for analytics.

Songs staging table will be directly transformed into `songs` and
`artists` tables.

Events staging table will be directly transformed into `times` and
`user` tables.

Both songs staging and event staging tables are joined to transform
into `songplays` table which contains both event's data and associated
songs and artists information.

## Source code

### `sql_queries.py`

This file defines all SQL queries required for running the ETL 
pipeline of this project, including:

* Queries to create staging tables
* Queries to create required fact and dimension tables
* Queries to drop tables if they exist
* Query to copy song data into `staging_songs` table and log data
int `staging_events` table.
* Queries to insert data into `songs`, `artists`, `users`, `time`, and `songplays` table by extracting data from stagin tables.

### `create_tables.py`

This file is used to create the sparkify database and required tables
for this project using queries defined in `sql_queriess.py`.

This file should be run to before running the ETL pipeline processing.
It is also run to reset the database to start over the ETL pipeline.

### `etl.py`

This file implements the ETL pipeline that processes all song data 
files and log files to populate data to the sparkify database.

## Running

Run `create_tables.py` program to create the database and tables by
the following command:

```
python create_tables.py
```

Then run the ETL pipeline by the following command, assume that the
data folder is placed in the same directory with `etl.py` program.

```
python etl.py
```
