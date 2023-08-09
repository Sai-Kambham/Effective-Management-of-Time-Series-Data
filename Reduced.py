import psycopg2
import numpy as np
import logging
from dotenv import dotenv_values
from datetime import datetime, timedelta
import pytz
import sys
import pandas as pd
from create_table import create_table

end_time = pytz.utc.localize(datetime.strptime((sys.argv[1]),"%Y-%m-%d"))
TIMEINTERVAL=int(sys.argv[2])
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S %Z"
)
logger = logging.getLogger("Reduced")
TSDB_CONFIG = dotenv_values(".env")


CONNECTION_STRING = f"""
  dbname={TSDB_CONFIG['DBNAME']}
  user={TSDB_CONFIG['USER']}
  password={TSDB_CONFIG['PASSWORD']}
  options='-c search_path=idrac10'
 """
TABLES = [
    #"systempowerconsumption"
    "voltagereading"
    #"temperaturereading"
    ]
def main():
    #start_time = end_time - timedelta(days=TIMEINTERVAL)
    #print(start_time)
    #print(end_time)
    with psycopg2.connect(CONNECTION_STRING) as conn:
        for table in TABLES:
            tablet="test_"+table
            # Create a cursor
            cursor = conn.cursor()
            # Read data from idrac8.voltagereading hypertable
            try:
                read_query = f"""SELECT timestamp, nodeid, source, fqdd, value 
                            FROM idrac10.{tablet} 
                            where timestamp < '2022-09-02' AND timestamp >= '2022-09-01' """
                cursor.execute(read_query)
                conn.commit()
            except Exception as err:
                logger.error("%s", err)

            # Convert the data to a pandas dataframe
            df = pd.DataFrame(cursor.fetchall(), columns=["timestamp", "nodeid", "source", "fqdd", "value"])

            # Group the data by nodeid, source, fqdd, and value
            grouped_df = df.groupby(["nodeid", "source", "fqdd", "value"])
            # Create a new dataframe to store the data for the new hypertable
            new_df = pd.DataFrame(columns=["timestamp", "nodeid", "source", "fqdd", "value", "duplicate_timestamps", "no_of_duplicates"])

            # Iterate through each group and get the most recent timestamp
            for group, data in grouped_df:
                recent_timestamp = data["timestamp"].max()

                # Count the number of duplicates
                duplicates = len(data[data["timestamp"] != recent_timestamp])

                # Add the data to the new dataframe
                new_data = {
                    "timestamp": recent_timestamp,
                    "nodeid": group[0],
                    "source": group[1],
                    "fqdd": group[2],
                    "value": group[3],
                    "duplicate_timestamps": data[data["timestamp"] != recent_timestamp]["timestamp"].tolist(),
                    "no_of_duplicates": duplicates+1
                }
                new_df = new_df.append(new_data, ignore_index=True)
            create_table(conn, table)
            dtable="schemed_"+table
            # Insert the data into the new hypertable
            for index, row in new_df.iterrows():
                try:
                    cursor.execute(f" INSERT INTO idrac10.{dtable} (timestamp, nodeid, source, fqdd, value, duplicate_timestamps, no_of_duplicates) VALUES (%s, %s, %s, %s, %s, %s, %s)", (row["timestamp"], row["nodeid"], row["source"], row["fqdd"], row["value"], row["duplicate_timestamps"], row["no_of_duplicates"]))
                    conn.commit()
                except Exception as err:
                    logger.error("%s", err)
            cursor.close()       
    conn.close()
if __name__ == "__main__":
    main()