import pathlib


def save_model(dir: str) -> None:

    d = pathlib.Path(dir)
    d.mkdir()
    p = d / "model.txt"
    print(p)

    with open(p, "w") as f:
        f.write("hello")
