import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stage_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS stage_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS dim_user;"
song_table_drop = "DROP TABLE IF EXISTS dim_song;"
artist_table_drop = "DROP TABLE IF EXISTS dim_artist;"
time_table_drop = "DROP TABLE IF EXISTS dim_time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE stage_events
(
  artist          varchar(1000),
  auth            varchar(100),
  firstName       varchar(500),
  gender          varchar(10),
  itemInSession   varchar(100),
  lastName        varchar(500),
  length          varchar(50),
  level           varchar(100),
  location        varchar(1000),
  method          varchar(100),
  page            varchar(100),
  registration    varchar(100),
  sessionId       varchar(100),
  song            varchar(500),
  status          varchar(50),
  ts              varchar(50),
  userAgent       varchar(100),
  userId          varchar(100)
);
""")

staging_songs_table_create = ("""
CREATE TABLE stage_songs
(
  artist_id         varchar(100),
  artist_latitude   varchar(50),
  artist_location   varchar(1000),
  artist_longitude  varchar(50),
  artist_name       varchar(1000),
  duration          varchar(50),
  num_songs         varchar(50),
  song_id           varchar(50),
  title             varchar(1000),
  year              varchar(10)
);
""")

songplay_table_create = ("""
CREATE TABLE songplay
(
  songplay_id  bigint IDENTITY(0,1) PRIMARY KEY,
  start_time   varchar(50) NOT NULL,
  user_id      varchar(100) NOT NULL,
  level        varchar(100),
  song_id      varchar(100) NOT NULL,
  artist_id    varchar(100) NOT NULL,
  session_id   varchar(100),
  location     varchar(100),
  user_agent   varchar(100)
);
""")

user_table_create = ("""
CREATE TABLE dim_user
(
  user_id         varchar(100),
  first_name      varchar(500),
  last_name       varchar(500),
  gender          varchar(50),
  level           varchar(100)
);
""")

song_table_create = ("""
CREATE TABLE dim_song
(
  song_id           varchar(50) NOT NULL,
  title             varchar(1000),
  artist_id         varchar(50),
  year              smallint,
  duration          varchar(20)
);
""")

artist_table_create = ("""
CREATE TABLE dim_artist
(
  artist_id         varchar(50) NOT NULL,
  name              varchar(500),
  location          varchar(1000),
  latitude          varchar(100),
  longitude         varchar(100)
);
""")

time_table_create = ("""
CREATE TABLE dim_time
(
  start_time   varchar(50) NOT NULL,
  hour         smallint NOT NULL,
  day          smallint NOT NULL,
  week         smallint NOT NULL,
  month        smallint NOT NULL,
  year         smallint NOT NULL,
  weekday      varchar(1) NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = ("""
    copy stage_songs from 's3://udacity-dend/song_data' 
    credentials 'aws_iam_role={}' 
    format as json 'auto'
    region 'us-west-2';
""").format(config.get("IAM_ROLE", "ARN"))

staging_songs_copy = ("""
    copy stage_events from 's3://udacity-dend/log_data' 
    credentials 'aws_iam_role={}' 
    format as json 'auto'
    region 'us-west-2';
""").format(config.get("IAM_ROLE", "ARN"))

# FINAL TABLES

user_table_insert = ("""
INSERT INTO dim_user (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId                                              AS user_id,
       firstName                                                    AS first_name,
       lastName                                                     AS last_name,
       gender                                                       AS gender,
       level                                                        AS level
FROM stage_events;
""")

song_table_insert = ("""
INSERT INTO dim_song (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id                                             AS song_id,
       title                                                        AS title,
       artist_id                                                    AS artist_id,
       CAST(year as smallint)                                       AS year,
       duration                                                     AS duration
FROM stage_songs;
""")

artist_table_insert = ("""
INSERT INTO dim_artist (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id                                           AS artist_id,
       artist_name                                                  AS name,
       artist_location                                              AS location,
       artist_latitude                                              AS latitude,
       artist_longitude                                             AS longitude
FROM stage_songs;
""")

time_table_insert = ("""
INSERT INTO dim_time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT SUBSTRING(CAST(timestamp 'epoch' + cast(ts as bigint)/1000 * interval '1 second' as varchar), 12,8)                        AS start_time,
       EXTRACT(hour from timestamp 'epoch' + cast(ts as bigint)/1000 * interval '1 second')                                                AS hour,
       EXTRACT(day from timestamp 'epoch' + cast(ts as bigint)/1000 * interval '1 second')                                                 AS day,
       EXTRACT(week from timestamp 'epoch' + cast(ts as bigint)/1000 * interval '1 second')                                                AS week,
       EXTRACT(month from timestamp 'epoch' + cast(ts as bigint)/1000 * interval '1 second')                                               AS month,
       EXTRACT(year from timestamp 'epoch' + cast(ts as bigint)/1000 * interval '1 second')                                                AS year,
       CASE WHEN EXTRACT(dayofweek FROM timestamp 'epoch' + cast(ts as bigint)/1000 * interval '1 second') IN (6, 7) THEN 'N' ELSE 'Y' END AS weekday
FROM stage_events;
""")
   
songplay_table_insert = ("""
INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT SUBSTRING(CAST(timestamp 'epoch' + cast(ts as bigint)/1000 * interval '1 second' as varchar), 12,8) AS start_time,
       userId                                                                                              AS user_id,
       level                                                                                               AS level,
       song_id                                                                                             AS song_id,
       artist_id                                                                                           AS artist_id,
       sessionId                                                                                           AS session_id,
       location                                                                                            AS location,
       userAgent                                                                                           AS user_agent
FROM stage_events, dim_song
WHERE song = title;
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
