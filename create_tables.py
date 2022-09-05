import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """drops all the tables in the db to ensure a fresh start"""
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """create all the tables as specified in sql_queries.py"""
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    print("Reading config.")
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print("Connecting to database.")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(config["DWH"]["DWH_ENDPOINT"], config["DWH"]["DWH_DB"], config["DWH"]["DWH_DB_USER"], config["DWH"]["DWH_DB_PASSWORD"], config["DWH"]["DWH_PORT"]))
    print("Succesfully connected to database.")
    cur = conn.cursor()

    print("Dropping all tables.")
    drop_tables(cur, conn)
    
    print("Creating tables.")
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()