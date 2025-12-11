"""Module with utilities to support FIED"""

import pandas as pd
import polars as pl


def _is_pandas(obj):
    """Check if the object is a pandas DataFrame"""
    return isinstance(obj, pd.DataFrame)


def _is_polars(obj):
    """Check if the object is a polars DataFrame"""
    return isinstance(obj, pl.DataFrame)


def _pandas_to_polars(df: pd.DataFrame) -> pl.DataFrame:
    """Convert a pandas DataFrame to a polars DataFrame"""
    return pl.from_pandas(df.reset_index())


def _polars_to_pandas(df: pl.DataFrame) -> pd.DataFrame:
    """Convert a polars DataFrame to a pandas DataFrame"""
    out = df.to_pandas()
    if "index" in out:
        out = out.set_index("index")
    return out


def expect_polars(func):
    """Decorator to convert pandas DataFrame to polars DataFrame

    This decorator is intended to support a gradual transition from
    pandas to polars. It converts any pandas DataFrame arguments to
    polars. Therefore, it allows new functions based on polars to
    operate in a legacy codebase that still uses pandas.

    Example
    -------
    @expect_polars
    def echo(df: pl.DataFrame) -> pl.DataFrame:
        return df

    echo(pd.DataFrame({"a": [1, 2, 3]}))

    Therefore, `echo` expects a polars DataFrame, but can be
    called with a pandas DataFrame.
    """

    def wrapper(*args, **kwargs):
        new_args = [
            _pandas_to_polars(arg) if _is_pandas(arg) else arg for arg in args
        ]
        new_kwargs = {
            k: _pandas_to_polars(v) if _is_pandas(v) else v
            for k, v in kwargs.items()
        }
        return func(*new_args, **new_kwargs)

    return wrapper


def expect_pandas(func):
    """Decorator to convert polars DataFrame to pandas DataFrame

    This decorator is intended to support a gradual transition from
    pandas to polars. It converts any polars DataFrame arguments to
    pandas. Therefore, it allows legacy functions based on pandas to
    operate in a codebase that is transitioning to polars.

    Example
    -------
    @expect_pandas
    def echo(df: pd.DataFrame) -> pd.DataFrame:
        return df

    echo(pl.DataFrame({"a": [1, 2, 3]}))

    Therefore, `echo` expects a pandas DataFrame, but can be
    called with a polars DataFrame.
    """

    def wrapper(*args, **kwargs):
        new_args = [
            _polars_to_pandas(arg) if _is_polars(arg) else arg for arg in args
        ]
        new_kwargs = {
            k: _polars_to_pandas(v) if _is_polars(v) else v
            for k, v in kwargs.items()
        }
        return func(*new_args, **new_kwargs)

    return wrapper
