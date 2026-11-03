from prefect import flow


@flow
def common_flow(config: dict):
    print("I am a subgraph that shows up in lots of places!")
    intermediate_result = 42
    return intermediate_result


@flow
def main_flow():
    # Do some things
    # Then call another flow function
    common_flow(config={})
    # Do more things


main_flow()
