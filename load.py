import duckdb
import os
import logging
import time

yellow1 = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2015-01.parquet"
green1 = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2015-01.parquet"

yellow_files = []
green_files = []

for year in range(2015, 2025):
    start_month = 2 if year == 2015 else 1
    for month in range(start_month, 13):
        yellow_file = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
        green_file = f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month:02d}.parquet"
        yellow_files.append(yellow_file)
        green_files.append(green_file)
    time.sleep(30)

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)

def load_parquet_files():
    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        #DROPPING TABLES IF THEY EXIST
        con.execute(f"""
            DROP TABLE IF EXISTS yellow_trips;
        """)
        logger.info("Dropped yellow table if exists")

        con.execute(f"""
            DROP TABLE IF EXISTS green_trips;
        """)
        logger.info("Dropped green table if exists")
        #CREATING TABLES
        con.execute(f"""
            CREATE TABLE yellow_trips
            AS
            SELECT * FROM read_parquet('{yellow_files[0]}');
        """)

        con.execute(f"""
            CREATE TABLE green_trips
            AS
            SELECT * FROM read_parquet('{green_files[0]}');
        """)
        #LOADING DATA INTO TABLES
        for file in green_files[1:]:
            con.execute(f"""
            INSERT INTO green_trips
            SELECT * FROM read_parquet('{file}');
        """)
        logger.info(f"Loaded {file}")

        for file in yellow_files[1:]:
            con.execute(f"""
            INSERT INTO yellow_trips
            SELECT * FROM read_parquet('{file}');
        """)
        logger.info(f"Loaded {file}")


        #CHECKING DATA
        result_yellow = con.execute("SELECT COUNT(*) FROM yellow_trips LIMIT 100;").fetchall()
        print(result_yellow)
        logger.info(f"Total rows loaded: {result_yellow[0]}")

        result_green = con.execute("SELECT COUNT(*) FROM green_trips LIMIT 100;").fetchall()
        print(result_green)
        logger.info(f"Total rows loaded: {result_green[0]}")


    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    load_parquet_files()