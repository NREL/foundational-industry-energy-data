"""County Business Patterns data"""

import polars as pl
import pooch
import requests


CBP_DATA_URL = "https://www2.census.gov/programs-surveys/cbp/datasets/{year}/"
CBP_API_URL = "https://api.census.gov/data/{year}/cbp"
SIZE_HEADER_RULE = re.compile("(n<5)|(n\d+_\d+)")

Dogbert = pooch.create(
    path=pooch.os_cache("FIED") / "CBP",
    base_url=CBP_DATA_URL,
    registry={
        "2010/cbp10co.zip": "sha256:c2d1b4b3e7de69e591b1b320570d1bd13be4ea337b0db76da1a0244a674e1470",
        "2012/cbp12co.zip": "sha256:b7bbe26fb31e02fa624e3d64f7e868329d500fd1d0f16e6440e1611d6bafe365",
        "2014/cbp14co.zip": "sha256:e4a360acc0e63b7b5849fa6a983addfd21d8294d454d28f87889975bcfcc5f3b",
        "2018/cbp18co.zip": "sha256:b0265feea47c08e92d8c344154ca8679ffcac3f8e3d1a5387a5d7fee8133b857",
        "2022/cbp22co.zip": "sha256:dcea1d9a7060cdb763c841d863163856ba8d5a50dce9056992bdeacd08ff99b4",
    },
)


def _fix_null(df: pl.DataFrame) -> pl.DataFrame:
    """Fix null values in CBP data

    CBP data uses "N" to indicate suppressed values for
    confidentiality reasons. This function replaces "N" with
    nulls and casts the affected columns to integers.
    """
    columns = (c for c in df.columns if SIZE_HEADER_RULE.match(c))
    for col in columns:
        if df[col].dtype == str:
            df = df.with_columns(
                pl.when(pl.col(col) == pl.lit("N"))
                .then(None)
                .otherwise(pl.col(col))
                .cast(pl.Int32)
                .alias(col)
            )
    return df


def fetch_cbp_county(year: int):
    """Fetch County Business Patterns data for a given year."""
    artifact = f"{year}/cbp{str(year)[2:]}co.zip"

    with tempfile.TemporaryDirectory() as swap:
        fnames = Dogbert.fetch(
            artifact, processor=pooch.Unzip(extract_dir=swap)
        )
        assert len(fnames) == 1
        df = pl.read_csv(fnames[0])

    return _fix_null(df)
