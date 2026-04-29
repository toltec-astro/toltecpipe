"""Console script for toltecpipe."""

import typer
from rich.console import Console

app = typer.Typer()
console = Console()


@app.command()
def main() -> None:
    """Console script for toltecpipe."""
    console.print("This is toltecpipe CLI.")


if __name__ == "__main__":
    app()
