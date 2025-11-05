"""North American Industry Classification System (NAICS) codes"""

import logging

import pandas as pd
import pooch
from pooch import HTTPDownloader


module_logger = logging.getLogger(__name__)


def fetch_naics(naics_vintage=2022):
    """Load 6-digit NAICS codes

    Download and saves for future use the National Industry
    Classification System (NAICS) codes from
    https://www.census.gov/naics/ .

    Parameters
    ----------
    naics_vintage : int, optional (default=2022)
        NAICS vintage year. Possible choices are 2017 & 2022.
    """
    url = f"https://www.census.gov/naics/{naics_vintage}NAICS/6-digit_{naics_vintage}_Codes.xlsx"
    if naics_vintage == 2022:
        known_hash = "sha256:3e3c90d4d36d874c0fd2da22a222c794654dbfa404320304152f5317781aefb7"
    elif naics_vintage == 2017:
        known_hash = "sha256:e314cc95191df4a2dd355944afda35cf287712556ccbe90a15a8f24aa3ef1d81"
    else:
        raise NotImplementedError(
            "Only 2017 and 2022 NAICS codes are available."
        )

    fname = pooch.retrieve(
        url=url,
        known_hash=known_hash,
        path=pooch.os_cache("FIED/NAICS"),
        downloader=HTTPDownloader(progressbar=True, verify=True),
    )

    all_naics = pd.read_excel(fname, usecols=[0, 1], engine="openpyxl")
    all_naics.dropna(how="all", axis=1, inplace=True)
    all_naics.dropna(how="all", axis=0, inplace=True)

    return all_naics
