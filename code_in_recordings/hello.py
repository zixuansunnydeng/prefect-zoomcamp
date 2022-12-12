from prefect import flow


@flow()
def hello():
    print("Hello!")


if __name__ == "__main__":
    hello()
