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

# registration also timestamp datatype? timestamp doesnt work, aws example uses double precision
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
    artist varchar(max),
    auth varchar(255),
    firstName varchar(255),
    gender varchar(255),
    itemInSession int,
    lastName varchar(255),
    length double precision,
    level varchar(255),
    location varchar(max), 
    method varchar(255),
    page varchar(255),
    registration double precision, 
    sessionId int,
    song varchar(max),
    status int,
    ts double precision,
    userAgent varchar(255),
    userId int
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    song_id	varchar(255),
    num_songs int,
    title varchar(max),
    artist_name	varchar(max),
    artist_latitude double precision,
    year int,
    duration double precision,
    artist_id varchar(255),
    artist_longitude double precision,
    artist_location varchar(max)
)
""")

# postgres serial is not supported by redshift -> use int IDENTITY(0,1)
#TODO froeign keys drin lassen?
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id int IDENTITY(0,1) primary key,
    start_time timestamp NOT NULL SORTKEY DISTKEY,
    user_id integer NOT NULL,
    level varchar(255),
    song_id varchar(255) NOT NULL,
    artist_id varchar(255) NOT NULL,
    session_id int,
    location varchar(255),
    user_agent text
)
""")


user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
    user_id integer primary key sortkey,
    first_name varchar(255) NOT NULL,
    last_name varchar(255) NOT NULL,
    gender character (1),
    level varchar(255)
)    
""")

#TODO sortkey von präsi so übernommen
song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
    song_id varchar(255) primary key sortkey,
    title varchar(255) NOT NULL,
    artist_id varchar(255) NOT NULL,
    year int,
    duration double precision NOT NULL
)
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar(255) primary key sortkey,
    name varchar(255) NOT NULL,
    location varchar(255),
    latitude double precision,
    longitude double precision
)
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
    start_time timestamp primary key sortkey distkey,
    hour int NOT NULL,
    day int NOT NULL,
    week int NOT NULL,
    month int NOT NULL,
    year int NOT NULL,
    weekday varchar(255) NOT NULL
)
""")

# STAGING TABLES

staging_events_copy = (
    """
    copy staging_events
    from {}
    iam_role '{}'
    json {}
    region 'us-west-2'
    """).format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = (
    """
    copy staging_songs
    from {}
    iam_role '{}'
    region 'us-west-2'
    format as json 'auto'
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

"""
the PRIMARY KEY constraint is informational only and will not stop duplicates. Your application should 
be handling by itself
case that source table is an append only table; only new records should be added
In this case your staging table will have the new batch of data. You will delete from teh 
staging table the records already present in the target table as they are duplicates. Then, 
you will insert the new records only (which is the remaining after deletion) into the target 
table.
Case that target table records will require an update
In these cases, You have to perform a merge logic; an upsert. If a record is present in the target 
table update it, if it's not present insert it.
"""

"""

https://docs.aws.amazon.com/redshift/latest/dg/merge-create-staging-table.html
"""


songplay_table_insert = ("""
    
""")


# this query would leave duplicates because some users switch between free and paid plans
# user_table_insert = ("""
#     INSERT INTO users (user_id, first_name, last_name, gender, level)
#     SELECT DISTINCT userid, firstname, lastname, gender, level 
#     FROM staging_events
#     WHERE userid IS NOT NULL
# """)

# this query ensures the latest plan information is used for the users and eliminates duplicate user_ids
user_table_insert = ("""
    create temp table staging_users (like users);

    alter table staging_users
    add ts double precision; 

    insert into staging_users 
    select userid, firstname, lastname, gender, level, ts 
    from staging_events
    where userid is not null;

    begin transaction;

    delete from users
    using staging_users
    where users.user_id = staging_users.user_id;

    insert into users(
        user_id, first_name, last_name, gender,level
    )
    select a.user_id, a.first_name, a.last_name, a.gender, a.level
    from staging_users a
    where not exists (select 1 from staging_users b where a.user_id = b.user_id and a.ts < b.ts);

    end transaction;

    drop table staging_users;
""")

song_table_insert = ("""
    insert into songs (song_id, title, artist_id, year, duration)
    select 
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert] #, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
