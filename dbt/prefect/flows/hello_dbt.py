from pathlib import Path

from prefect import flow, get_run_logger
from prefect_dbt.cli.commands import trigger_dbt_cli_command


@flow
def hello_dbt():
    logger = get_run_logger()
    logger.info('Running "dbt debug"')

    result = trigger_dbt_cli_command(
        command="dbt debug",
        project_dir=Path("../../dbt"),
        profiles_dir=Path("../../dbt"),
    )
    return result


if __name__ == "__main__":
    hello_dbt()
