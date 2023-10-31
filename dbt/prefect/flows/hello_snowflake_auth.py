from dope_core.dope_client.main import DopeClient
from prefect import flow, get_run_logger


@flow(name="Snowflake auth")
def hello_snowflake_auth():
    logger = get_run_logger()

    snowflake_client = DopeClient().snowflake

    if snowflake_client.is_authenticated(connection_type="key_pair"):
        logger.info("Able to connect to Snowflake")
    else:
        logger.info("Not able to connect to Snowflake")


if __name__ == "__main__":
    hello_snowflake_auth()
