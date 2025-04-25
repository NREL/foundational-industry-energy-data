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
        knwon_hash = "sha256:512455a2d234490f828cab1e4fc85b34f62048513f2ea9473c4b0e9123701538"
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
        known_hash="sha256:6abf5fe5ec090777e10c7c6f91c281b1573be5783031759023cb4840aee30269",
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


def fetch_state_FIPS():
    """Fetch the state FIPS codes"""
    fname = pooch.retrieve(
        url="https://www2.census.gov/geo/docs/reference/state.txt",
        known_hash="sha256:bea4e03f71a1fa0045ae732aabad11fa541e5932b071c2369bb0d325e8cba5a0",
        path=pooch.os_cache("FIED"),
        downloader=HTTPDownloader(progressbar=True),
    )

    return fname


def fetch_QPC(year):
    """
    Quarterly survey began 2008; start with 2010 due to  2007-2009
    recession.
    2017_qtr_table_final_q1.xlsx: 9839eb5b32e2722fb3e38f6ad4c29cb678032eb35b44354cf64cfad89e919caa
    2017_qtr_table_final_q2.xlsx: a06ea334dc2b8d3c1d18e891393a99a1493d1ce313964b7257958c318d63764f
    2017_qtr_table_final_q3.xlsx: 9af153d11e6ae3ee8376e9f081f894dec98e28ae9b6df1c5aef8161ce912dcc6
    2017_qtr_table_final_q4.xlsx: e2927a491a1e8a5add954583456474ebb87ece524ae9f9009eb504ed0235ab87

    2019_qtr_table_final_q1.xlsx: f3412ae9d6f831eb9bcd55ae3005448232bdb6791ad03d6fc39582574a1ad70e
    2019_qtr_table_final_q2.xlsx: e5e8d74dbee8c258e8203d4fdcb6b128aa664d51ed7935580c4170dad9976919
    2019_qtr_table_final_q3.xlsx: 531ad948026713b4a7e000041979cddb81c403f5904113b8aafec78db451230a
    2019_qtr_table_final_q4.xlsx: ad22ba33893545581061d2677982bf9995492e8cbf850e03cdc760c61e8a81d5

    2020-qtr-table-final-q1.xlsx: 619a351a8ae7c39139bab23b3248e41a3476c9674a4fa39b782f31b49e1af022
    2020-qtr-table-final-q2.xlsx: 3332e6806f6ed8984d71ee3fe2d6c43eeb77c81f4169122f01ed149dd9634ed8
    2020-qtr-table-final-q3.xlsx: 475f44c9646d6f848d79e709865255d6bc6724e89352e0848f58e3e39fad0690
    2020_qtr_table_final_q4.xlsx: 93886980180fdf3ec2e5509b5b7e04108311b3a2e85a5c9626b5dfeec87e46e8

    2022-qtr-table-final-q1.xlsx: a67a278bdab928227dcc006e0c0c94a0f1555c0f18a915d90ae90d56c542be41
    2022-qtr-table-final-q2.xlsx: 34d50fc8963edb34ade784c918b6d17083ac6a5965c8cf43a23de80a987333eb
    2022-qtr-table-final-q3.xlsx: fc630bccf8e91e7d53d2feb8e1cc5b63fc9bbdd46c6d95519a4fd5318badd445
    2022-qtr-table-final-q4.xlsx: ac8bfbb7aa685a8aad0e2433a1a07409eccb946c1c231ac8682e0f36d4767edb
    """
    y = str(year)

    if year < 2017:
        excel_ex = ".xls"
    else:
        excel_ex = ".xlsx"

    qpc_data = pd.DataFrame()

    base_url = "https://www2.census.gov/programs-surveys/qpc/tables/"

    for q in ["q" + str(n) for n in range(1, 5)]:
        if (year >= 2017) & (year < 2020):
            y_url = "{!s}/{!s}_qtr_table_final_"

        # elif year < 2010:
        #
        #     y_url = \
        #         '{!s}/qpc-quarterly-tables/{!s}_qtr_combined_tables_final_'

        elif (year == 2020) & (q == "q4"):
            y_url = "{!s}/{!s}_qtr_table_final_"

        elif year > 2019:
            y_url = "{!s}/{!s}-qtr-table-final-"

        else:
            y_url = "{!s}/qpc-quarterly-tables/{!s}_qtr_table_final_"

        if (year == 2016) & (q == "q4"):
            url = base_url + y_url.format(y, y) + q + ".xlsx?#"

        else:
            url = base_url + y_url.format(y, y) + q + excel_ex

        fname = pooch.retrieve(
            url, known_hash=None, path=pooch.os_cache("FIED"), progressbar=True
        )

        # Excel formatting for 2008 is different than all other years.
        # Will need to revise skiprows and usecols.
        try:
            data = pd.read_excel(
                fname, sheet_name=1, skiprows=4, usecols=range(0, 7), header=0
            )

        except urllib.error.HTTPError:
            print(f"Problem with {url}")

        data = data.drop(data.columns[2], axis=1)
        data.columns = [
            "NAICS",
            "Description",
            "Utilization Rate",
            "UR_Standard Error",
            "Weekly_op_hours",
            "Hours_Standard Error",
        ]
        data = data.dropna()
        data["Q"] = q
        data["Year"] = year
        qpc_data = qpc_data.append(data, ignore_index=True)

    return qpc_data
