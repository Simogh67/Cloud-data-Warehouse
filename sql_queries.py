import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

staging_events_table_create= (""" 
create table if not exists staging_events (
       artist text, 
       auth varchar, 
       firstName varchar, 
       gender varchar,  
       itemInSession int, 
       lastName varchar, 
       length float, 
       level varchar, 
       location text, 
       method varchar, 
       page varchar, 
       registration varchar, 
       sessionId int, 
       song text, 
       status int, 
       ts float, 
       userAgent text, 
       userId int) 
""")

staging_songs_table_create = ("""
create table if not exists staging_songs (
       num_songs int, 
       artist_id varchar,
       artist_latitude float, 
       artist_longitude float, 
       artist_location text, 
       artist_name varchar, 
       song_id varchar, 
       title text, 
       duration float, 
       year int) 
""")

songplay_table_create = ("""
create table if not exists songplays (
       songplay_id bigint IDENTITY(0,1) primary key,
       start_time timestamp not null, 
       user_id int not null, 
       level varchar, 
       song_id varchar not null, 
       artist_id varchar not null,
       session_id int,
       location varchar, 
       user_agent text)
""")

user_table_create = ("""
create table if not exists users (
       user_id int primary key, 
       first_name varchar, 
       last_name varchar, 
       gender varchar, 
       level varchar)
""")

song_table_create = ("""
create table if not exists songs (
       song_id varchar primary key, 
       title varchar, 
       artist_id varchar not null,
       year int, 
       duration float)
""")

artist_table_create = ("""
create table if not exists artists(
       artist_id varchar primary key,
       name varchar,
       location varchar,
       latitude float,
       longitude float)
""")

time_table_create = ("""
create table if not exists time(
       start_time timestamp primary key,
       hour int,
       day int,
       week int,
       month int,
       year int,
       weekday int)
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events FROM {}
                            iam_role {}
                            format as json {}
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""copy staging_songs FROM {}
                            iam_role {}
                            format as json 'auto'
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""insert into songplays (start_time, 
                            user_id, level, song_id, artist_id, session_id, location, user_agent) 
                            select distinct (TIMESTAMP 'epoch' + 
                            (ts / 1000) * INTERVAL '1 second') asstart_time,
                            userId AS user_id ,
                            level, song_id, 
                            artist_id,  
                            sessionId as session_id, 
                            location, 
                            userAgent as user_agent
                            from staging_events se
                            join staging_songs ss
                            on ss.title=se.song and se.artist=ss.artist_name and se.length=ss.duration
                            where se.page='NextSong'
""")

user_table_insert = ("""insert into users select distinct userId AS user_id, 
                       firstName AS first_name, 
                       lastName AS last_name,
                       gender, 
                       level 
                       from staging_events
                       where page='NextSong'
""")

song_table_insert = ("""insert into songs select distinct song_id, title, 
                        artist_id,
                        year, 
                        duration
                        from staging_songs
""")

artist_table_insert = ("""insert into artists select distinct  artist_id, 
                          artist_name AS name, 
                          artist_location AS location, 
                          artist_latitude AS latitude, 
                          artist_longitude AS longitude
                          from staging_songs
""")

time_table_insert = ("""insert into time select start_time,
                        EXTRACT(hour from start_time) AS hour,
                        EXTRACT(day from start_time) AS day,
                        EXTRACT(week from start_time) AS week,
                        EXTRACT(month from start_time) AS month,
                        EXTRACT(year from start_time) AS year,
                        EXTRACT(weekday from start_time) AS weekday
                        FROM songplays 
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
