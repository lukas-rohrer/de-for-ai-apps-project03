import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print("Connecting to database.")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(config["DWH"]["DWH_ENDPOINT"], config["DWH"]["DWH_DB"], config["DWH"]["DWH_DB_USER"], config["DWH"]["DWH_DB_PASSWORD"], config["DWH"]["DWH_PORT"]))
    print("Succesfully connected to database.")
    cur = conn.cursor()
    
    print("Copying the data into the staging tables.")
    load_staging_tables(cur, conn)

    # insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()