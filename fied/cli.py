"""FIED's command line interface."""

import click

import fied.frs.frs_extraction
import fied.fied_compilation
from fied import __version__


@click.command()
@click.version_option(__version__)
def main():
    """FIED's command line interface."""
    fied.frs.frs_extraction.doit()

    fied.fied_compilation.doit()


if __name__ == "__main__":
    main()
