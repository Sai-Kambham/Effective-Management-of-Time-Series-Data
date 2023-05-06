import logging

logger = logging.getLogger("create_table")

def create_table(conn, table):
    """Creates table.
    :param object conn: connection object from psycopg2
    :param str table: table name
    """
    # Define the SQL statement to create the new hypertable
    dtable = "schemed_" + table
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {dtable}(
        timestamp TIMESTAMP not NULL, 
        nodeid INTEGER not NULL, 
        source text not NULL, 
        fqdd text not NULL, 
        value FLOAT not NULL, 
        duplicate_timestamps TIMESTAMP[], 
        no_of_duplicates INTEGER
        );
    """
    #create_hypertable_sql = f"SELECT create_hypertable('idrac10.{table}', 'timestamp');"
    cur = conn.cursor()
    try:
        # Execute the SQL statements to create the table
        cur.execute(create_table_sql)
        #cur.execute(create_hypertable_sql)
        # Commit the transaction
        conn.commit()
    except Exception as err:
        logger.error("%s", err)
    finally:
        # Close the cursor and database connection
        cur.close()
