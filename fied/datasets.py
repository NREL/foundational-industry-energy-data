"""Temporary solution to fetch required datasets

The FIED is based on several public datasets. This module manages how these data are obtained and optimize for the analysis.

This is a temporary solution while I work on how to obtain and extract some of the data in an automatic way.
"""

import pandas as pd
import pooch

from pooch import HTTPDownloader


def fetch_nei_2017():
    """Fetch the 2017 National Emissions Inventory (NEI)

    Currently only download the zip file, which uses an unconventional
    compression, thus it can't be processed by Pooch's Unzip processor."""
    fname = pooch.retrieve(
        url="https://gaftp.epa.gov/air/nei/2017/data_summaries/2017v1/2017neiJan_facility_process_byregions.zip",
        known_hash="sha256:8f015ea29fc82e17c370a316020ad76ebb7df16aaeea3fc24425647b0edcb7c9",
        path=pooch.os_cache("FIED"),
        downloader=HTTPDownloader(progressbar=True, verify=False),
    )

    return fname

def fetch_nei_2020():
    """Fetch the 2020 National Emissions Inventory (NEI)

    Currently only download the zip file, which uses an unconventional
    compression, thus it can't be processed by Pooch's Unzip processor."""
    fname = pooch.retrieve(
        url="https://gaftp.epa.gov/air/nei/2020/data_summaries/2020nei_facility_process_byregions.zip",
        known_hash="sha256:1264392ef859801fef7349b796937a974bfcbdaee1f6e8f69c0686b8e6bc9b7d",
        path=pooch.os_cache("FIED"),
        downloader=HTTPDownloader(progressbar=True, verify=False),
    )

    return fname


def fetch_emission():
    """Fetch the Emissions by Unit and Fuel Type"""
    fnames = pooch.retrieve(
        path=pooch.os_cache("FIED"),
        # URL to one of Pooch's test files
        url="https://www.epa.gov/system/files/other-files/2022-10/emissions_by_unit_and_fuel_type_c_d_aa_10_2022.zip",
        known_hash="52fcfa039509bc5c900ab5b27b9ede64398e72bb3fff72cc771bd85cc51bea48",
        processor=pooch.Unzip(
            members=["emissions_by_unit_and_fuel_type_c_d_aa_10_2022.xlsb"]
        ),
    )

    df = pd.read_excel(
        fnames[0], engine="pyxlsb", sheet_name="UNIT_DATA", skiprows=6
    )

    return df


def fetch_webfirefactors():
    """Load all EPA WebFire emissions factors

    Download from EPA's https://www.epa.gov/electronic-reporting-air-emissions/webfire

    Returns
    -------
    pd.DataFrame
        EPA WebFire emissions factors
    """
    fnames = pooch.retrieve(
        url="https://cfpub.epa.gov/webfire/download/webfirefactors.zip",
        known_hash="sha256:18421985874da6f5670fefdb03ecfb21b7e08eb99b39252802e5d95e4a63bd3f",
        path=pooch.os_cache("FIED"),
        # Temporary solution. Don't verify SSL.
        downloader=HTTPDownloader(progressbar=True, verify=False),
        processor=pooch.Unzip(members=["webfirefactors.csv"]),
    )

    return pd.read_csv(fnames[0])


def fetch_scc():
    """Load EPA's Source Classification Codes (SCC)"""
    fname = pooch.retrieve(
        url="https://sor-scc-api.epa.gov/sccwebservices/v1/SCC?format=CSV&sortFacet=scc+level+one&filename=SCCDownload.csv",
        known_hash="sha256:607d8575ee23d7b054143ac30c49e5f96f91303c48bdf26c40d53094716fb178",
        path=pooch.os_cache("FIED"),
        downloader=HTTPDownloader(progressbar=True),
    )

    return pd.read_csv(fname)
