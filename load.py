import duckdb
import os
import logging

yellow1 = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
yellow2 = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet"
yellow3 = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-03.parquet"
yellow4 = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-04.parquet"
yellow5 = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-05.parquet"
yellow6 = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-06.parquet"
yellow7 = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-07.parquet"
yellow8 = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-08.parquet"
yellow9 = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-09.parquet"
yellow10 = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-10.parquet"
yellow11 = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-11.parquet"
yellow12 = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet"
green1 = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-01.parquet"
green2 = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-02.parquet"
green3 = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-03.parquet"
green4 = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-04.parquet"
green5 = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-05.parquet"
green6 = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-06.parquet"
green7 = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-07.parquet"
green8 = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-08.parquet"
green9 = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-09.parquet"
green10 = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-10.parquet"
green11 = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-11.parquet"
green12 = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-12.parquet"

yellow_files = [yellow1, yellow2, yellow3, yellow4, yellow5, yellow6, yellow7, yellow8, yellow9, yellow10, yellow11, yellow12]
green_files = [green1, green2, green3, green4, green5, green6, green7, green8, green9, green10, green11, green12]



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

        con.execute(f"""
            DROP TABLE IF EXISTS yellow_trips;
        """)
        logger.info("Dropped yellow table if exists")

        con.execute(f"""
            DROP TABLE IF EXISTS green_trips;
        """)
        logger.info("Dropped green table if exists")

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