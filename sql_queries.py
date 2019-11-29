import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

# Staging table for copying log data into. This table contains columns
# to capture neccessary log fields.
staging_events_table_create= ("""
CREATE TABLE staging_events(
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession INT,
    lastName VARCHAR,
    length NUMERIC,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration NUMERIC,
    sessionId INT,
    song VARCHAR,
    status INT,
    ts BIGINT,
    userAgent VARCHAR,
    userId INT
)
""")

# Staging table for copying song data into. This table contains columns
# to capture neccessary song/artist fields.
staging_songs_table_create = ("""
CREATE TABLE staging_songs(
    song_id VARCHAR,
    num_songs INT,
    title VARCHAR,
    year INT,
    duration NUMERIC,
    artist_id VARCHAR,
    artist_name VARCHAR,
    artist_location VARCHAR,
    artist_latitude NUMERIC,
    artist_longitude NUMERIC
)
""")

# For songplays table:
#   * Use Redshift's IDENTITY datatype to generate primary key songplay_id
#   * All fields except song_id, artist_id, location and user_agent
#     are required.
songplay_table_create = ("""
CREATE TABLE songplays(
    songplay_id INT IDENTITY(0, 1) PRIMARY KEY,
    start_time BIGINT NOT NULL,
    user_id INT NOT NULL,
    level VARCHAR NOT NULL,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id INT NOT NULL,
    location VARCHAR,
    user_agent VARCHAR
)
""")

# For users table, all fields are required.
user_table_create = ("""
CREATE TABLE users(
    user_id INT PRIMARY KEY,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    gender CHAR(1) NOT NULL,
    level VARCHAR NOT NULL
)
""")

# For songs table, all fields are required except duration
song_table_create = ("""
CREATE TABLE songs(
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    year INT NOT NULL,
    duration NUMERIC
)
""")

# For artists table, location, latitude, and longitude may be null
artist_table_create = ("""
CREATE TABLE artists(
    artist_id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    location VARCHAR,
    latitude NUMERIC,
    longitude NUMERIC
)
""")

# For time table, BIGINT is used to store timestamp value to avoid
# out-of-range error when using INT datatype
time_table_create = ("""
CREATE TABLE time(
    start_time BIGINT PRIMARY KEY,
    hour INT NOT NULL,
    day INT NOT NULL,
    week INT NOT NULL,
    month VARCHAR NOT NULL,
    year INT NOT NULL,
    weekday INT NOT NULL
)
""")

# STAGING TABLES

# Copy from log data files into event staging table
staging_events_copy = ("""
COPY staging_events
FROM {}
IAM_ROLE {}
FORMAT AS JSON {} TRUNCATECOLUMNS
COMPUPDATE OFF REGION 'us-west-2';
""").format(config.get('S3', 'LOG_DATA'),
            config.get('IAM_ROLE', 'ARN'), 
            config.get('S3', 'LOG_JSONPATH'))

# Copy from song data files into song staging table
staging_songs_copy = ("""
COPY staging_songs
FROM {}
IAM_ROLE {}
FORMAT AS JSON 'auto' TRUNCATECOLUMNS
COMPUPDATE OFF REGION 'us-west-2';
""").format(config.get('S3', 'SONG_DATA'), 
            config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

# Extract songplays from both staging tables
songplay_table_insert = ("""
INSERT INTO songplays(
    start_time, user_id, level, song_id, 
    artist_id, session_id, location, user_agent
)
SELECT 
    e.ts, e.userId, e.level, s.song_id,
    s.artist_id, e.sessionId, e.location, e.userAgent
FROM staging_events e 
JOIN staging_songs s
  ON e.song = s.title 
 AND e.artist = s.artist_name 
 AND e.length = s.duration
WHERE e.page='NextSong';
""")

# Extract distinct users from event staging table
user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT 
    DISTINCT userId, firstName, lastName, gender, level
FROM staging_events
WHERE page='NextSong';
""")

# Extract distinct songs from song staging table
song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT
    DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs;
""")

# Extract distinct artists from song staging table
artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT
    DISTINCT artist_id, artist_name,
    artist_location, artist_latitude, artist_longitude
FROM staging_songs;
""")

# Extract distinct timestampts from events staging table
time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT
    ts,
    EXTRACT(HOUR FROM ts_date) AS hour,
    EXTRACT(DAY FROM ts_date) AS day,
    EXTRACT(WEEK FROM ts_date) AS week,
    EXTRACT(MONTH FROM ts_date) AS month,
    EXTRACT(YEAR FROM ts_date) AS year,
    EXTRACT(DOW FROM ts_date) AS weekday
FROM (
    SELECT 
        DISTINCT ts, 
        '1970-01-01'::DATE + ts/1000 * INTERVAL '1 second' AS ts_date 
    FROM staging_events
);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
