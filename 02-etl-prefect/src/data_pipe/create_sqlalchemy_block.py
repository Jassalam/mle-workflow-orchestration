from prefect_sqlalchemy import SqlAlchemyConnector

BLOCK_NAME = "ny-taxi-connector"
CONNECTION_INFO = "postgresql+psycopg2://postgres:postgres@localhost:5432/ny_taxi"


def main() -> None:
    block = SqlAlchemyConnector(connection_info=CONNECTION_INFO)
    block.save(BLOCK_NAME, overwrite=True)
    print(f"Created or updated Prefect block: {BLOCK_NAME}")


if __name__ == "__main__":
    main()
