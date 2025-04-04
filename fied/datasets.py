"""Temporary solution to fetch required datasets

The FIED is based on several public datasets. This module manages how these data are obtained and optimize for the analysis.

This is a temporary solution while I work on how to obtain and extract some of the data in an automatic way.

Dev note: Clean and stright datasets access. List all those here before
thinking on optimization and removing redundancies.
"""

from pathlib import Path

import geopandas as gpd
import pandas as pd
import pooch
from pooch import HTTPDownloader
from stream_unzip import stream_unzip


def fetch_frs(combined=True):
    """Fetch the Facility Registry Service (FRS) dataset from EPA

    NOTE: This dataset might be updated frequently. The current
    downloaded files are only 20 days old. If the updates are too
    frequent, it might be cumbersome to keep the hash up to date (but
    still important for reproducibility).

    Combined file was ~732 MB on Dec 2022, and ~1.2 GB on Feb 2025.

    Parameters
    ----------
    combined : bool, optional
        If True, download the combined dataset, by default True.
        Otherwise, download the single dataset.
    """
    if combined:
        url = "https://ordsext.epa.gov/FLA/www3/state_files/national_combined.zip"
        knwon_hash = "sha256:3575cc51c8fa44daa25382871515e068db42645f2e683a80e1238bf5200502ab"
        members = [
            "NATIONAL_ALTERNATIVE_NAME_FILE.CSV",
            "NATIONAL_CONTACT_FILE.CSV",
            "NATIONAL_ENVIRONMENTAL_INTEREST_FILE.CSV",
            "NATIONAL_FACILITY_FILE.CSV",
            "NATIONAL_MAILING_ADDRESS_FILE.CSV",
            "NATIONAL_NAICS_FILE.CSV",
            "NATIONAL_ORGANIZATION_FILE.CSV",
            "NATIONAL_PROGRAM_FILE.CSV",
            "NATIONAL_SIC_FILE.CSV",
            "NATIONAL_SUPP_INTEREST_FILE.CSV",
        ]
    else:
        url = (
            "https://ordsext.epa.gov/FLA/www3/state_files/national_single.zip"
        )
        knwon_hash = "sha256:1c41e349dfcf7f4ac4db2eb99b0814eb89cab980bf4880ad427fdfe289eaa979"
        members = ["NATIONAL_SINGLE.CSV"]

    fnames = pooch.retrieve(
        url=url,
        known_hash=knwon_hash,
        path=pooch.os_cache("FIED") / "FRS",
        downloader=HTTPDownloader(progressbar=True),
        processor=pooch.Unzip(members=members),
    )

    return fnames


def fetch_zip_codes():
    """Fetch the ZIP Code dataset from USPS

    This was originally used by frs_extraction's call_all_fips() on
    demand, i.e. it accessed the remote file every time it was needed,
    thus creating a permanent dependency on that service.
    """
    fname = pooch.retrieve(
        url="https://postalpro.usps.com/mnt/glusterfs/2022-12/ZIP_Locale_Detail.xls",
        known_hash="sha256:fd0689f6801a2d5291354a9d6c25af3656b863c2462b445ba4d4595b024cd5a9",
        path=pooch.os_cache("FIED"),
        downloader=HTTPDownloader(progressbar=True),
    )

    return pd.read_excel(fname)


def fetch_nei_2017():
    """Fetch the 2017 National Emissions Inventory (NEI)

    Temporary solution to deal with the unconventional compression
    deflate64.
    """
    fzname = pooch.retrieve(
        url="https://gaftp.epa.gov/air/nei/2017/data_summaries/2017v1/2017neiJan_facility_process_byregions.zip",
        known_hash="sha256:8f015ea29fc82e17c370a316020ad76ebb7df16aaeea3fc24425647b0edcb7c9",
        path=pooch.os_cache("FIED"),
        downloader=HTTPDownloader(progressbar=True, verify=False),
    )

    members = ["point_unknown.csv", "point_12345.csv", "point_678910.csv"]

    # Temporary solution
    def zipped_chunks(filename, chunk_size=65536):
        with open(filename, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                yield chunk

    path = Path(fzname + ".unzip")

    if path.exists():
        output = [
            f for f in path.iterdir() if f.is_file() and (f.name in members)
        ]
        return [str(f) for f in output]

    path.mkdir()
    output = []
    for fname, fsize, chunks in stream_unzip(zipped_chunks(fzname)):
        print(f"Unzipping {fname.decode()}")
        outname = path / fname.decode()
        with open(outname, "wb") as f:
            for c in chunks:
                f.write(c)
        if fname.decode() in members:
            output.append(str(outname))

    return output


def fetch_nei_2020():
    """Fetch the 2020 National Emissions Inventory (NEI)

    Temporary solution to deal with the unconventional compression
    deflate64.
    """
    fzname = pooch.retrieve(
        url="https://gaftp.epa.gov/air/nei/2020/data_summaries/2020nei_facility_process_byregions.zip",
        known_hash="sha256:1264392ef859801fef7349b796937a974bfcbdaee1f6e8f69c0686b8e6bc9b7d",
        path=pooch.os_cache("FIED"),
        downloader=HTTPDownloader(progressbar=True, verify=False),
    )

    members = [
        "point_unknown.csv",
        "point_1.csv",
        "point_2.csv",
        "point_3.csv",
        "point_4.csv",
        "point_5.csv",
        "point_6.csv",
        "point_7.csv",
        "point_8.csv",
        "point_9.csv",
        "point_10.csv",
    ]

    # Temporary solution
    def zipped_chunks(filename, chunk_size=65536):
        with open(filename, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                yield chunk

    path = Path(fzname + ".unzip")

    if path.exists():
        output = [
            f for f in path.iterdir() if f.is_file() and (f.name in members)
        ]
        return [str(f) for f in output]

    path.mkdir()
    output = []
    for fname, fsize, chunks in stream_unzip(zipped_chunks(fzname)):
        print(f"Unzipping {fname.decode()}")
        outname = path / fname.decode()
        with open(outname, "wb") as f:
            for c in chunks:
                f.write(c)
        if fname.decode() in members:
            output.append(str(outname))

    return output


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

    # df = pd.read_excel(
    #     fnames[0], engine="pyxlsb", sheet_name="UNIT_DATA", skiprows=6
    # )

    return fnames[0]


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
    """Load EPA's Source Classification Codes (SCC)

    Note that downloading directly from website assignes filename for
    csv based as 'SCCDownload-{y}-{md}-{t}.csv'

    Should force the filename to be 'SCCDownload.csv'?

    This was originally defined in scc/scc_unit_id.py.

    payload = {
      "format": "CSV",
      "sortFacet": "scc level one",
      "filename": "SCCDownload.csv",
      }
    """
    fname = pooch.retrieve(
        url="https://sor-scc-api.epa.gov/sccwebservices/v1/SCC?format=CSV&sortFacet=scc+level+one&filename=SCCDownload.csv",
        known_hash="sha256:607d8575ee23d7b054143ac30c49e5f96f91303c48bdf26c40d53094716fb178",
        path=pooch.os_cache("FIED"),
        downloader=HTTPDownloader(progressbar=True),
    )

    return pd.read_csv(fname)


def fetch_naics(naics_vintage=2022):
    """Load NAICS codes

    Download and saves for future use the National Industry
    Classification System (NAICS) codes.

    Those tables are available at https://www.census.gov/naics/ , but
    there is some issue to systematically download it. For now, let's
    secure the data and guarantee an automatic procedure by keeping
    copies of those files in our repository.

    Note the URL for download would be:
    f'https://www.census.gov/naics/{naics_vintage}NAICS/6-digit_{naics_vintage}_Codes.xlsx'

    Parameters
    ----------
    naics_vintage : int, optional (default=2022)
        NAICS vintage year. Possible choices are 2017 & 2022.
    """
    if naics_vintage == 2022:
        url = "https://github.com/NREL/foundational-industry-energy-data/raw/refs/heads/naics_codes/6-digit_2022_Codes.xlsx"
        known_hash = "sha256:3e3c90d4d36d874c0fd2da22a222c794654dbfa404320304152f5317781aefb7"
    elif naics_vintage == 2017:
        url = "https://github.com/NREL/foundational-industry-energy-data/raw/refs/heads/naics_codes/6-digit_2017_Codes.xlsx"
        known_hash = "sha256:e314cc95191df4a2dd355944afda35cf287712556ccbe90a15a8f24aa3ef1d81"
    else:
        raise NotImplementedError(
            "Only 2017 and 2022 NAICS codes are available."
        )

    fname = pooch.retrieve(
        url=url,
        known_hash=known_hash,
        path=pooch.os_cache("FIED"),
        downloader=HTTPDownloader(progressbar=True, verify=False),
    )

    all_naics = pd.read_excel(fname, usecols=[0, 1], engine="openpyxl")
    all_naics.dropna(how="all", axis=1, inplace=True)
    all_naics.dropna(how="all", axis=0, inplace=True)

    return all_naics


def fetch_shapefile_census_block_groups(year, state_fips):
    fname = pooch.retrieve(
        url=f"https://www2.census.gov/geo/tiger/TIGER{year}/BG/tl_{year}_{state_fips}_bg.zip",
        known_hash=None,
        path=pooch.os_cache("FIED"),
        downloader=HTTPDownloader(progressbar=True),
    )
    return gpd.read_file(fname)


def fetch_shapefile_congressional_district(year):
    known_hash = {
        2017: "sha256:b4ae191081b6ae03a03643f2ab8078b21374b825280a6198c910413569c90450",
    }
    fname = pooch.retrieve(
        url=f"https://www2.census.gov/geo/tiger/TIGER{year}/CD/tl_{year}_us_cd115.zip",
        known_hash=known_hash.get(int(year), None),
        path=pooch.os_cache("FIED"),
        downloader=HTTPDownloader(progressbar=True),
    )
    return gpd.read_file(fname)


def fetch_shapefile_county(year):
    known_hash = {
        2017: "sha256:0417e7ca7bb678e64221336f426fdd361d7ed8bb6f57dad9d85d446aa36df593",
        2022: "sha256:a48c6e018d80e5557720971831a37120450f02c6d934687ccb3c26314ae8bda6",
    }
    fname = pooch.retrieve(
        url=f"https://www2.census.gov/geo/tiger/TIGER{year}/COUNTY/tl_{year}_us_county.zip",
        known_hash=known_hash.get(int(year), None),
        path=pooch.os_cache("FIED"),
        downloader=HTTPDownloader(progressbar=True),
    )
    return gpd.read_file(fname)


def fetch_shapefile_NHDP():
    """Consider moving to release 2"""
    fname = pooch.retrieve(
        url="https://prd-tnm.s3.amazonaws.com/StagedProducts/Hydrography/NHDPlusHR/National/GDB/NHDPlus_H_National_Release_1_GDB.zip",
        known_hash="sha256:9df49689812d502dcd8812c23bdf4c030c840624ab62e907e517091da9ece8a5",
        path=pooch.os_cache("FIED"),
        downloader=HTTPDownloader(progressbar=True),
    )

    return gpd.read_file(fname)
