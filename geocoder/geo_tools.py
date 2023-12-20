
import pandas as pd
import math
import requests
import os
import concurrent.futures
import logging

logging.basicConfig(level=logging.INFO)


def fix_county_fips(df):
    """
    County FIPS should be strings. Use geoID or
    censusBlock to replace existing county FIPS.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame with either geoID or censusBlock in the columns

    Returns
    -------
    df : pandas.DataFrame
        DataFrame with updated countyFIPS
    """

    for c in ['geoID', 'countyFIPS']:
        if c in df.columns:
            geo_column = c
        else:
            continue

    df.countyFIPS.update(
        df.dropna(
            subset=[geo_column]
            )[geo_column].astype(str).apply(lambda x: x[0:5])
        )

    missing = df[df[geo_column].isnull()]

    missing_str = missing.countyFIPS.dropna().astype(int).astype(str)

    df.countyFIPS.update(
        missing_str[missing_str.apply(lambda x: len(x)==5)]
        )

    df.countyFIPS.update(
        missing_str[missing_str.apply(lambda x: len(x)==4)].apply(lambda x: f'0{x}')
        )

    # Assume that the countyFIPS with len <4 are missing the state FIPS.
    state_fips = pd.read_csv(
        'https://www2.census.gov/geo/docs/reference/state.txt',
        usecols=[0, 1], names=['statefips', 'stateCode'], sep='|',
        header=0, dtype=str, index_col=['stateCode']
        )

    for n in range(1, 4):

        data = missing_str[missing_str.apply(lambda x: len(x)==n)]
        data = df.loc[data.index, 'stateCode'].map(
            state_fips.to_dict()['statefips']
            ) + data.apply(lambda x: f'{(3-n)*"0"}{x}')

        data.name = 'countyFIPS'

        df.countyFIPS.update(data)

    return df


def find_missing_congress(df):
    """"
    Update Congressional Districts to 118th Congress, for 2020.
    Update the FRS column legislativeDistrictNumber
    """

    cd = pd.read_csv(
        'https://www2.census.gov/geo/docs/maps-data/data/rel2020/cd-sld/tab20_cd11820_county20_natl.txt',
        sep='|', header=0, usecols=[1, 8],
        dtype='str', names=['congressionalDistrictNumber','countyFIPS']
        )

    cd = dict(cd[['countyFIPS', 'congressionalDistrictNumber']].values)

    df.loc[:, 'legislativeDistrictNumber'] = df.countyFIPS.map(cd)

    # Merge created duplicate entries and was slower than mapping with dict.
    # df = pd.merge(
    #     df, cd,
    #     on='countyFIPS',
    #     how='left'
    #     )

    return df


def fcc_block_api(lat_lon, census_year=2020):
    """
    Call FCC's Area API (https://geo.fcc.gov/api/census/)
    with lat, lon coordinates to return the corresponding
    Census Block.

    Parameters
    ----------
    lat_lon : list
        List of lat, lon coordinates

    census_year : default is 2020


    Returns
    -------
    block : int or None
        Census block. Returns None if there is no
        corresponding block (e.g., offshore oil platform)

    """

    url = 'https://geo.fcc.gov/api/census/block/find?'

    params = {
        'latitude': lat_lon[0],
        'longitude': lat_lon[1],
        'censusYear': census_year,
        'showall': True,
        'format': 'json'
        }

    try:
        r = requests.get(url, params=params, timeout=(1, 3))
        logging.info(f'{lat_lon[0]}, {lat_lon[1]}')

    except requests.exceptions.ConnectionError:
        logging.error(
            f'ConnectionError: latitude ({lat_lon[0]}), longitude ({lat_lon[1]})'
            )
        
        block = None
        
    except requests.exceptions.ReadTimeout:
        logging.error(
            f'ReadTimeout exception: latitude ({lat_lon[0]}), longitude ({lat_lon[1]})'
            )
        
        block = None

    else:

        try:
            block = r.json()['Block']['FIPS']

        except:
            block = None

    return block


def get_blocks_parallelized(df):
    """
    Paraellization for FCC API. 
    Final industrial data has ~360,000 unique
    lat, lon coordinates.

    Parameters
    ----------
    df : pandas.DataFrame
        Final foundational energy dataframe

    Returns
    -------
    df : pandas.DataFrame
        Final foundational energy dataframe with
        new colum for censusBlock

    """

    all_latlon = pd.DataFrame(
        df.drop_duplicates(['latitude', 'longitude'])[['latitude', 'longitude']]
        ).values

    results = []

    executor = concurrent.futures.ThreadPoolExecutor()  # max_workers=65

    for result in executor.map(fcc_block_api, all_latlon):
        results.append(result)

    latlon_block = pd.DataFrame(all_latlon, columns=['latitude', 'longitude'])
    latlon_block.loc[:, 'censusBlock'] = results

    df = pd.merge(df, latlon_block, on=['latitude', 'longitude'],
                  how='left')

    return df


# Abandoned geocoder approach
# def create_geocode_batch(df, benchmark="2020", vintage="2020"):
#     """
#     Creates CSV's to submit to Census Geocoder
#     (https://www.census.gov/programs-surveys/geography/technical-documentation/complete-technical-documentation/census-geocoder.html)

#     Format is registryID, locationAddress, cityName, stateCode, postalCode.
#     Each CSV is limited to 10,000 records.

#     Parameters
#     ----------

#     df : pandas.DataFrame
#         Final data dataframe.

#     benchmark : str; default is "2020".
#         Other available benchmarks: https://geocoding.geo.census.gov/geocoder/benchmarks.

#     vintage : str; default is "2020". Dependant on benchmark.
#         Vintages are based on benchmark; see https://geocoding.geo.census.gov/geocoder/vintages?benchmark=benchmarkId., 

#     Returns
#     -------

#     df : pandas.DataFrame
#     """

#     url = 'http://geocoding.geo.census.gov/geocoder/geographies/addressbatch'

#     params = {
#         'returntype': "geographies",
#         'benchmark': benchmark,
#         'vintage': vintage,
#         }

#     geoinfo = pd.DataFrame()

#     cols = ['registryID', 'locationAddress', 'cityName', 'stateCode', 'postalCode']
#     cols_i = []
#     for c in cols:
#         cols_i.append(df.columns.to_list().index(c))

#     geo_df = pd.DataFrame(df.drop_duplicates(subset=cols))
#     geo_df.reset_index(drop=True, inplace=True)

#     chunksize = 10000
#     n_chunks = math.ceil(len(geo_df) / chunksize)

#     for n in range(n_chunks):


#         file_path_name =  os.path.abspath(f'./geocoder/Addresses.csv')
#         # file_path_name = os.path.abspath(f'./geocoder/data_to_geocoder_{n}.csv')

#         geo_df.iloc[n*chunksize:(n+1)*chunksize, cols_i].to_csv(
#             file_path_name, index=False, header=False
#             )

#         files = {'addressFile': (file_path_name, open(file_path_name, 'rb'), 'text/csv')}

#         r = requests.post(url, files=files, data=params)

    

