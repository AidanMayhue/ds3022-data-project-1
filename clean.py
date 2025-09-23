import duckdb
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)

def clean_data():
    con = duckdb.connect(database='emissions.duckdb', read_only=False)
    logger.info("Connected to DuckDB instance")

    #YELOW TRIP DATA CLEANING
    con.execute(f"""
    SELECT DISTINCT * FROM yellow_trips;
    """)
    logger.info("Cleaned yellow table")

    con.execute(f"""
    DELETE FROM yellow_trips WHERE passenger_count = 0;
    """)
    logger.info("Deleted yellow trips with 0 passengers")

    con.execute(f"""
    DELETE FROM yellow_trips WHERE trip_distance = 0;
    """)
    logger.info("Deleted yellow trips with 0 distance")

    con.execute(f"""
    DELETE FROM yellow_trips WHERE trip_distance > 100;
    """)
    logger.info("Deleted yellow trips with > 100 distance")

    #GREEN TRIP DATA CLEANING
    con.execute(f"""
    SELECT DISTINCT * FROM green_trips;
    """)
    logger.info("Cleaned green table")

    con.execute(f"""
    DELETE FROM green_trips WHERE passenger_count = 0;
    """)
    logger.info("Deleted green trips with 0 passengers")

    con.execute(f"""
    DELETE FROM green_trips WHERE trip_distance = 0;
    """)
    logger.info("Deleted green trips with 0 distance")

    con.execute(f"""
    DELETE FROM green_trips WHERE trip_distance > 100;
    """)
    logger.info("Deleted green trips with > 100 distance")

if __name__ == "__main__":
    clean_data()