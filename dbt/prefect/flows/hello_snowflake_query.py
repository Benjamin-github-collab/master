from dope_core.dope_client.main import DopeClient
from prefect import flow, get_run_logger
from snowflake.connector import DatabaseError


@flow(name="Snowflake query")
def hello_snowflake_query():
    logger = get_run_logger()

    snowflake_client = DopeClient().snowflake

    cs = None
    connection = None

    try:
        connection = snowflake_client.connect(connection_type="key_pair")
        logger.info("Connected to snowflake")
        cs = connection.cursor()
        cs.execute("SELECT current_user()")
        one_row = cs.fetchone()
        logger.info(f"snowflake user: {one_row}")
    except DatabaseError as e:
        logger.info(f"Database ERROR: {e}")
    finally:
        if cs:
            cs.close()
        if connection:
            connection.close()


if __name__ == "__main__":
    hello_snowflake_query()
