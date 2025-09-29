import duckdb
import os
import logging
import time

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)

def load_parquet_files():

    con = None

    try:
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        # Drop old tables
        con.execute("DROP TABLE IF EXISTS yellow_trips;")
        logger.info("Dropped yellow_trips if existed")
        con.execute("DROP TABLE IF EXISTS green_trips;")
        logger.info("Dropped green_trips if existed")
        con.execute("DROP TABLE IF EXISTS vehicle_emissions;")
        logger.info("Dropped vehicle_emissions if existed")

        # Load taxi data
        for taxi in ['yellow', 'green']:
            for year in range(2015, 2024):
                for month in range(1, 13):
                    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi}_tripdata_{year}-{month:02}.parquet"
                    logger.info(f"Processing {url}")

                    try:
                        # Check if table exists
                        table_exists = con.execute(
                            f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name='{taxi}_trips'"
                        ).fetchone()[0]

                        if not table_exists:
                            con.execute(f"""
                                CREATE TABLE {taxi}_trips AS
                                SELECT * FROM read_parquet('{url}');
                            """)
                            logger.info(f"Created table {taxi}_trips with data from {url}")
                        else:
                            con.execute(f"""
                                INSERT INTO {taxi}_trips
                                SELECT * FROM read_parquet('{url}');
                            """)
                            logger.info(f"Inserted data from {url} into {taxi}_trips")

                    except Exception as e:
                        logger.error(f"Failed to process {url}: {e}")

                    # Sleep to avoid hammering the server
                    time.sleep(30)

        # Load vehicle emissions data (once)
        csv_file = 'vehicle_emissions.csv'
        if os.path.exists(csv_file):
            con.execute(f"""
                CREATE TABLE vehicle_emissions AS
                SELECT * FROM read_csv_auto('{csv_file}');
            """)
            logger.info(f"Created table vehicle_emissions from {csv_file}")
        else:
            logger.warning(f"{csv_file} not found, skipping vehicle_emissions table creation")

        # Run summary queries
        print(con.execute("SELECT COUNT(*) FROM yellow_trips").fetchall())
        logger.info("Counted yellow_trips rows")

        print(con.execute("SELECT COUNT(*) FROM green_trips").fetchall())
        logger.info("Counted green_trips rows")

        print(con.execute("SELECT AVG(trip_distance) FROM yellow_trips").fetchall())
        logger.info("Calculated average trip_distance from yellow_trips")

        print(con.execute("SELECT AVG(trip_distance) FROM green_trips").fetchall())
        logger.info("Calculated average trip_distance from green_trips")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    load_parquet_files()
