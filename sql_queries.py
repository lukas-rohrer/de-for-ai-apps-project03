import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table_drop"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table_drop"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table_drop"
user_table_drop = "DROP TABLE IF EXISTS user_table_drop"
song_table_drop = "DROP TABLE IF EXISTS song_table_drop"
artist_table_drop = "DROP TABLE IF EXISTS artist_table_drop"
time_table_drop = "DROP TABLE IF EXISTS time_table_drop"

# CREATE TABLES

# registration also timestamp datatype?
staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(
    artist varchar(255),
    auth varchar(255),
    firstName varchar(255),
    gender varchar(255),
    itemInSession int,
    lastName varchar(255),
    length double precision,
    level varchar(255),
    location varchar(255), 
    method varchar(255),
    page varchar(255),
    registration varchar(255), 
    sessionId int,
    song varchar(255),
    status int,
    ts timestamp,
    userAgent varchar(255),
    userId int
)
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(
    num_songs int,
    artist_id varchar(255),
    artist_latitude double precision,
    artist_longitude double precision,
    artist_location varchar(255),
    artist_name	varchar(255),
    song_id	varchar(255),
    title varchar(255),
    duration double precision,
    year int
)
""")

# postgres serial is not supported by redshift -> use IDENTITY(0,1)
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(
    songplay_id IDENTITY(0,1) primary key,
    start_time timestamp references time(start_time) NOT NULL,
    user_id integer references users(user_id) NOT NULL,
    level varchar(255),
    song_id varchar(255) references songs(song_id),
    artist_id varchar(255) references artists(artist_id),
    session_id int,
    location varchar(255),
    user_agent text
)
""")


user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
    user_id integer primary key,
    first_name varchar(255),
    last_name varchar(255),
    gender character (1),
    level varchar(255)
)    
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
    song_id varchar(255) primary key,
    title varchar(255) NOT NULL,
    artist_id varchar(255),
    year int,
    duration double precision NOT NULL
)
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar(255) primary key,
    name varchar(255) NOT NULL,
    location varchar(255),
    latitude double precision,
    longitude double precision
)
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
    start_time timestamp primary key,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday varchar(255)
)
""")

# STAGING TABLES

staging_events_copy = (
    """
    copy staging events
    from '{}'
    iam_role '{}'
    json {}
    compupdate off
    region 'us-west-2'
    """).format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'])


"""
copy staging_events
from {}
iam_role '{}'
json {}
compupdate off
region 'us-west-2';
"""


staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
