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

staging_events_table_create= ("""
CREATE TABLE staging_events(
    artist TEXT,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession SMALLINT,
    lastName VARCHAR,
    length DECIMAL,
    level VARCHAR,
    location TEXT,
    method VARCHAR,
    page VARCHAR,
    registration DECIMAL,
    sessionId INTEGER,
    song TEXT,
    status SMALLINT,
    ts VARCHAR,
    userAgent TEXT,
    userId INTEGER
)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs(
    num_songs INTEGER,
    artist_id VARCHAR,
    artist_latitude DECIMAL NULL,
    artist_longitude DECIMAL NULL,
    artist_location TEXT NULL,
    artist_name VARCHAR,
    song_id VARCHAR,
    title TEXT,
    duration DECIMAL,
    year SMALLINT
)
""")


songplay_table_create = ("""
    CREATE TABLE songplays(
        start_time TIMESTAMP, 
        user_id int, 
        level VARCHAR, 
        song_id VARCHAR, 
        artist_id VARCHAR, 
        session_id VARCHAR, 
        location VARCHAR, 
        user_agent TEXT NULL
    )
""")

user_table_create = ("""
CREATE TABLE users(
    user_id INTEGER PRIMARY KEY SORTKEY, 
    first_name VARCHAR, 
    last_name VARCHAR, 
    gender VARCHAR, 
    level VARCHAR
) diststyle all;
""")

song_table_create = ("""
CREATE TABLE songs(
song_id VARCHAR NOT NULL PRIMARY KEY SORTKEY,
title VARCHAR, 
artist_id VARCHAR, 
year INTEGER NOT NULL, 
duration DECIMAL NOT NULL
) diststyle all;
""")

artist_table_create = ("""
CREATE TABLE artists(
    artist_id VARCHAR NOT NULL PRIMARY KEY SORTKEY, 
    name VARCHAR, 
    location VARCHAR, 
    latitude DECIMAL NULL, 
    longitude DECIMAL NULL
) diststyle all;
""")

time_table_create = ("""
CREATE TABLE time(
    start_time TIMESTAMP NOT NULL SORTKEY, 
    hour INTEGER NOT NULL, 
    day INTEGER NOT NULL, 
    week INTEGER NOT NULL, 
    month INTEGER NOT NULL, 
    year INTEGER NOT NULL, 
    weekday VARCHAR
) diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    iam_role {} 
    json {}
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    iam_role {} 
    json 'auto'
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays(
        start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    ) SELECT DISTINCT(timestamp 'epoch' + CAST(se.ts AS bigint)/1000 * interval '1 second') AS start_time,
        se.userId,
        se.level,
        ss.song_id,
        ss.artist_id,
        se.sessionId,
        se.location,
        se.userAgent       
    FROM staging_events se
    JOIN staging_songs ss ON (se.artist = ss.artist_name AND se.song = ss.title)
    WHERE se.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users(
    user_id, first_name, last_name, gender, level
) SELECT DISTINCT(userId),
        firstName,
        lastName,
        gender,
        level
    FROM staging_events
    WHERE page = 'NextSong' AND userId IS NOT NULL
""")

song_table_insert = ("""
INSERT INTO songs(
    song_id, title, artist_id, year, duration
) SELECT DISTINCT(ss.song_id),
    ss.title,
    ss.artist_id,
    ss.year,
    ss.duration
 FROM staging_songs ss WHERE ss.song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists(
artist_id, name, location, latitude, longitude
) SELECT DISTINCT(ss.artist_id), 
ss.artist_name, 
ss.artist_location, 
ss.artist_latitude, 
ss.artist_longitude 
FROM staging_songs ss WHERE ss.artist_id IS NOT NULL
""")

time_table_insert = ("""
    INSERT INTO time(
        start_time, hour, day, week, month, year, weekday
    ) 
    SELECT DISTINCT(timestamp 'epoch' + CAST(se.ts AS bigint)/1000 * interval '1 second') AS start_time, 
    EXTRACT(hour FROM start_time),  
    EXTRACT(day FROM  start_time), 
    EXTRACT(week FROM start_time),
    EXTRACT(month FROM start_time), 
    EXTRACT(year FROM start_time),
    EXTRACT(DOW FROM start_time) 
    FROM staging_events se WHERE se.ts IS NOT NULL
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
