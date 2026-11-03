from prefect import flow


@flow
def say_hello(name: str):
    print("Hello, world!")
    return f"Hello {name}!"


print(say_hello(name="ML Engineers"))
