import psycopg2


import logging
from dotenv import load_dotenv
import os
load_dotenv()

# Setup the logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

logging.getLogger("httpx").setLevel(logging.ERROR)      # Supress API HTTP request logs


def connect():
    return psycopg2.connect(
        dbname= os.getenv("POSTGRESDB"),
        user = os.getenv("POSTGRESUSER"),
        password = os.getenv("POSTGRESPASSWORD"),
        host = os.getenv("POSTGRESHOST"),
        port = os.getenv("POSTGRESPORT")
    )

def test_connection():
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
    test_connection()
