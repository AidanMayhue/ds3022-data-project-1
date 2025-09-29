import duckdb
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='clean.log'
)
logger = logging.getLogger(__name__)

#Instance of try and except handling
try:
    def clean_data():
        con = duckdb.connect(database='emissions.duckdb', read_only = False)

        #Setting memory limit to circumvent a crash
        con.execute("SET memory_limit='16GB'") 
        con.execute("PRAGMA max_temp_directory_size='50GB'")

        #Removing duplicates from yellow
        con.execute(f"""
        SELECT DISTINCT * FROM yellow_trips;
                                """)
        logger.info("Cleaned yellow table")

        #Removing trips with no passengers
        con.execute(f"""
        DELETE FROM yellow_trips WHERE passenger_count = 0;
            """)
        logger.info("Deleted yellow trips with 0 passengers")

        #Removing trips with no distance or over 100 miles
        con.execute(f"""
        DELETE FROM yellow_trips WHERE trip_distance = 0;
            """)
        logger.info("Deleted yellow trips with 0 distance")

        con.execute(f"""
        DELETE FROM yellow_trips WHERE trip_distance > 100;
            """)
        logger.info("Deleted yellow trips with > 100 distance")

        #Removing trips over 24 hours
        con.execute(f"""
        DELETE FROM yellow_trips WHERE date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) >86400;
            """)
        logger.info("Deleted yellow trips with > 24 hour duration")

        #Counting rows after cleaning
        print(con.execute(f"SELECT COUNT(*) FROM yellow_trips").fetchall())
        logger.info("recounted yellow_trips table rows")

        #Performing same cleaning steps for green trips
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

        con.execute(f"""
        DELETE FROM green_trips WHERE date_diff('second', lpep_pickup_datetime, lpep_dropoff_datetime) >86400;
            """)
        logger.info("Deleted yellow trips with > 24 hour duration")

        print(con.execute(f"SELECT COUNT(*) FROM green_trips").fetchall())
        logger.info("Final count of green_trips rows")

        #Creating tests to verify cleaning steps
        try:
            yellow_count = con.execute("SELECT COUNT(*) FROM yellow_trips WHERE passenger_count = 0").fetchone()[0]
            print(f"Yellow trips with 0 passengers: {yellow_count}")
            logger.info(f"Yellow trips with 0 passengers: {yellow_count}")

            green_count = con.execute("SELECT COUNT(*) FROM green_trips WHERE passenger_count = 0").fetchone()[0]
            print(f"Green trips with 0 passengers: {green_count}")
            logger.info(f"Green trips with 0 passengers: {green_count}")

            yellow_distance_count = con.execute("SELECT COUNT(*) FROM yellow_trips WHERE trip_distance = 0 OR trip_distance > 100").fetchone()[0]
            print(f"Yellow trips with 0 or >100 miles: {yellow_distance_count}")
            logger.info(f"Yellow trips with 0 or >100 miles: {yellow_distance_count}")

            green_distance_count = con.execute("SELECT COUNT(*) FROM green_trips WHERE trip_distance = 0 OR trip_distance > 100").fetchone()[0]
            print(f"Green trips with 0 or >100 miles: {green_distance_count}")
            logger.info(f"Green trips with 0 or >100 miles: {green_distance_count}")

            yellow_duration_count = con.execute("SELECT COUNT(*) FROM yellow_trips WHERE date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) > 86400").fetchone()[0]
            print(f"Yellow trips > 24 hours: {yellow_duration_count}")
            logger.info(f"Yellow trips > 24 hours: {yellow_duration_count}")

            green_duration_count = con.execute("SELECT COUNT(*) FROM green_trips WHERE date_diff('second', lpep_pickup_datetime, lpep_dropoff_datetime) > 86400").fetchone()[0]
            print(f"Green trips > 24 hours: {green_duration_count}")
            logger.info(f"Green trips > 24 hours: {green_duration_count}")
        except Exception as e:
            print(f"An error occurred during testing: {e}")
            logger.error(f"An error occurred during testing: {e}")
except Exception as e:
    print(f"An error occurred: {e}")
    logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    clean_data()