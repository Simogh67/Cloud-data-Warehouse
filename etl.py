import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''This function copys S3 data into the staging tables
        Args: cur : the psycopg2 cursor, conn: connection object to the database'''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''This function inserts data from the staging tables into the star schema tables
        Args: cur : the psycopg2 cursor, conn: connection object to the database'''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''This function builds connection to redshift database using psycopg2 then the staging tables are called from S3, 
                 then data is copied into the star schema tables'''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
