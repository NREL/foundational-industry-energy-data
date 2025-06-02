"""FIED's command line interface."""

import logging

import click

import fied.frs.frs_extraction
import fied.fied_compilation
from fied import __version__


@click.command()
@click.version_option(__version__)
@click.option(
    "-v",
    "verbose",
    count=True,
    help="Increase verbosity. Use -vvv for debug output."
)
@click.option(
    "--vintage",
    default="2017",
    type=click.Choice(["2017", "2020"]),
    help="Edition of FIED to use. Default is 2017.",
)
def main(verbose, vintage: int):
    """FIED's command line interface."""
    if verbose == 1:
        level = logging.WARNING
    elif verbose == 2:
        level = logging.INFO
    elif verbose >= 3:
        level = logging.DEBUG
    else:
        level = logging.ERROR
    logger = logging.getLogger("fied")
    logger.setLevel(level)
    ch = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    logger.info(f"FIED CLI version {__version__}")

    fied.frs.frs_extraction.doit()

    fied.fied_compilation.doit(year=int(vintage))

if __name__ == "__main__":
    main()
