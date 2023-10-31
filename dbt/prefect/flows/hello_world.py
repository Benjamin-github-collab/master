from prefect import flow, task


@task(name="Print Hello Task")
def print_hello(name) -> str:
    message = f"Hello {name}!"
    print(f"Hello {name}")
    return message


@flow(name="Hello World Flow")
def hello_world(name="world") -> None:
    print_hello(name)
    return


if __name__ == "__main__":
    hello_world()
