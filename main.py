import psycopg2
import logging
import json
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

# Setup the logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Supress other unwanted logs, e.g., from httpx if used elsewhere
logging.getLogger("httpx").setLevel(logging.ERROR)


def connect():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRESDB"),
        user=os.getenv("POSTGRESUSER"),
        password=os.getenv("POSTGRESPASSWORD"),
        host=os.getenv("POSTGRESHOST"),
        port=os.getenv("POSTGRESPORT")
    )


def insert_review(conn, review):
    """
    Insert a single review record into the 'reviews' table.

    :param conn: A psycopg2 connection object
    :param review: A dictionary representing one review from the JSON
    """
    # Convert Unix epoch in milliseconds to a Python datetime
    # (If 'timestamp_updated' is in ms, divide by 1000.0)
    timestamp_value = datetime.utcfromtimestamp(review["timestamp_updated"] / 1000.0)

    # Prepare the INSERT query
    insert_query = """
        INSERT INTO reviews (
            review_id,
            topic,
            sentiment,
            category,
            sentence,
            cluster_id,
            cluster_name,
            timestamp
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Create a cursor, execute the INSERT, commit immediately (or do bulk commits for performance)
    with conn.cursor() as cur:
        cur.execute(insert_query, (
            review["recommendationid"],
            review["topic"],
            review["sentiment"],
            review["category"],
            review["sentence"],
            review["hdbscan_cluster_id"],
            review["hdbscan_cluster_name"],
            timestamp_value
        ))
    conn.commit()


def insert_reviews_from_json(json_file_path):
    """
    Read a JSON file containing reviews and insert them into the 'reviews' table.

    :param json_file_path: The path to your JSON file containing an array of review objects.
    """
    conn = None
    try:
        # Connect to the database
        conn = connect()
        logger.info("Connected to the database.")

        # Load the JSON data
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)  # expecting data to be a list of review dictionaries

        # Insert each review into the database
        for review in data:
            insert_review(conn, review)

        logger.info("All reviews inserted successfully.")
    except (Exception, psycopg2.DatabaseError) as e:
        logger.error(f"Error inserting reviews: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")


def test_connection():
    """
    Quick test of the database connection.
    """
    conn = None
    try:
        conn = connect()
        cur = conn.cursor()
        logger.info('PostgreSQL database version:')
        cur.execute('SELECT version()')
        db_version = cur.fetchone()
        logger.info(db_version)
        cur.close()
    except (Exception, psycopg2.DatabaseError) as e:
        logger.error(e)
    finally:
        if conn is not None:
            conn.close()
            logger.info('Database connection closed.')


if __name__ == '__main__':
    # Example usage:
    test_connection()


    # S:\SID\Analytics\Working Files\Individual\Florian\Projects\Database_and_Agents\Data\HRC\steam_reviews_hrc.json
    # Paths
    root_dir = r'S:\SID\Analytics\Working Files\Individual\Florian\Projects\Database_and_Agents\Data\HRC'
    path_input = os.path.join(root_dir, "steam_reviews_hrc.json")
    insert_reviews_from_json(path_input)
