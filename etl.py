import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Execute each queries defined in `copy_table_queries` to
    load data files from S3 into staging tables.
    """
    for query in copy_table_queries:
        # print out to see which query is being executed
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Execute each queries defined in `insert_table_queries` to
    transform data from staging tables into dimension tables.
    """
    for query in insert_table_queries:
        # print out to see which query is being executed
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    """
    Main function that runs the ETL pipeline.
    It firstly connects to the configured Redshift database (specified
    in dwh.cfg file) to load song data and log data files into the
    corresponding staging tables; then populate data into dimension
    tables by extracting data from staging tables.
    """

    # Load configuration for DWH database and location of log files
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connect to the Redshift database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # Load dataset into staging tables
    load_staging_tables(cur, conn)

    # Insert data from staging tables into dimension tables
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()