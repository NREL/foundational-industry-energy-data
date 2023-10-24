
import pandas as pd


def naics_matcher(naics_column, naics_vintage=2017):
    """
    Method for matching aggregated NAICS codes (i.e., <6 digit)
    with 6-digit NAICS codes.

    Parameters
    ----------
    naics_column : pandas.Series
        Series of NAICS codes to match to 6-digit NAICS.

    naics_vintage : int; 2007, 2012, 2017, or 2022
        Year of NAICS codes

    Returns
    -------
    ncmatch : pandas.DataFrame
        Original NAICS matched to 6-digit versions.
    """

    if naics_vintage < 2017:
        naics_url = f'https://www.census.gov/naics/{naics_vintage}NAICS/6-digit_{naics_vintage}_Codes.xls'

    else:
        naics_url = f'https://www.census.gov/naics/{naics_vintage}NAICS/6-digit_{naics_vintage}_Codes.xlsx'

    all_naics = pd.read_excel(naics_url, usecols=[0, 1])
    all_naics.dropna(how='all', axis=1, inplace=True)
    all_naics.dropna(how='all', axis=0, inplace=True)

    all_naics[f'{naics_vintage} NAICS Code'] = \
        all_naics[f'{naics_vintage} NAICS Code'].astype(int)

    nctest = pd.DataFrame(all_naics[f'{naics_vintage} NAICS Code'].values,
                          columns=['n6'])

    for n in range(3, 6):

        nctest[f'n{n}'] = nctest.n6.apply(
            lambda x: int(str(x)[0:n])
            )

    # Drop duplicates
    naics_column = naics_column.drop_duplicates()

    # Match only < 6-digit NAICS
    if any([len(str(x)) == 6 for x in naics_column]):
        naics_column = naics_column.where(
            naics_column.apply(lambda x: len(str(int(x))) < 6)
        ).dropna()

    else:
        pass

    # Int format
    if naics_column.dtype != 'int32':

        #for i, v in naics_column.iteritems():
        # dav
        for i, v in naics_column.items():

            try:
                naics_column.loc[i] = int(v)

            except ValueError:
                naics_column.drop(i, inplace=True, axis=0)

    else:
        pass

    # Loop not converting to int
    naics_column = naics_column.astype(int)

    ncmatch = pd.concat(
        [pd.merge(nctest[column], pd.DataFrame(naics_column),
                  left_on=column, right_on=naics_column.name,
                  how='left')
            for column in ['n6', 'n5', 'n4', 'n3']], axis=1
        )

    ncmatch = ncmatch[naics_column.name].dropna(how='all').join(nctest['n6'])

    nclen = len(ncmatch[naics_column.name].columns)

    if nclen > 1:
        ncog = pd.concat(
            [ncmatch.iloc[:, n].dropna().astype(int) for n in range(0, nclen)],
            axis=0, ignore_index=False
            )

        ncmatch = pd.DataFrame(ncog).join(ncmatch['n6'])

        ncmatch.reset_index(inplace=True, drop=True)

    else:

        ncmatch = pd.concat(
            [ncmatch[naics_column.name].apply(lambda x: int(x.dropna()), axis=1),
            ncmatch['n6']], axis=1
            )

    return ncmatch
