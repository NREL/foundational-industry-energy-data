import pandas as pd
import pooch


def fetch_emission():
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
