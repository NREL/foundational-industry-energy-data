"""FIED's command line interface."""

import click

import fied.frs.frs_extraction
import fied.fied_compilation
from fied import __version__


@click.command()
@click.version_option(__version__)
@click.option(
    "--edition",
    default="2017",
    type=click.Choice(["2017", "2020"]),
    help="Edition of FIED to use. Default is 2017.",
)
def main(edition: int):
    """FIED's command line interface."""
    fied.frs.frs_extraction.doit()

    fied.fied_compilation.doit(year=int(edition))


if __name__ == "__main__":
    main()
