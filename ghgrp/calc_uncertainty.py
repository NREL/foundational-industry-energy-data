# -*- coding: utf-8 -*-
"""
Created on Wed Jul 10 22:16:04 2019

@author: cmcmilla
"""
#%%
import pandas as pd
import numpy as np

class uncertainty:

    def __init__(self, year, ci=95):

        # Dictionary of multipliers for calculating confidence
        # intervals
        ci_mults = {90:1.645, 95:1.96}

        self.ci_mult = ci_mults[ci]

        if year >= 2014:

            filex = '.xlsx'

        else:

            filex = '.xls'

        base_url = 'https://www.eia.gov/consumption/manufacturing/data/'

        mecs32 = pd.read_excel(
                base_url+str(year)+'/xls/table3_2'+filex, sheet_name=1,
                header=None,
                names=['naics', 'naics_desc','Total','Net_electricity',
                       'Residual_fuel_oil', 'Diesel', 'Natural_gas',
                       'LPG_NGL', 'Coal', 'Coke_and_breeze', 'Other']
                ).dropna(thresh=1)

        mecs33 = pd.read_excel(
                base_url+str(year)+'/xls/table3_3'+filex, sheet_name=1,
                header=None,
                names=['Characteristic', 'Total', 'Net_electricity',
                       'Residual_fuel_oil','Diesel', 'Natural_gas', 'LPG_NGL',
                       'Coal','Coke_and_breeze', 'Other']
                ).dropna(thresh=1)

        mecs35 = pd.read_excel(
                base_url+str(year)+'/xls/table3_5'+filex, sheet_name=1,
                header=None,
                names=['naics', 'naics_desc', 'Total',
                       'Blast_furnace_coke_oven', 'Waste_gas', 'Petcoke',
                       'Pulp_liquor', 'Woodchips','Waste_materials']
                ).dropna(thresh=1)

        mecs52 = pd.read_excel(
                base_url+str(year)+'/xls/table5_2'+filex, sheet_name=1,
                header=None,
                names=['naics', 'end_use', 'Total', 'net_electricity',
                       'fuel_oil', 'diesel','natural_gas', 'NGL_HGL', 'coal',
                       'other']
                ).dropna(thresh=1)

        # format tables
        # Following taken from MECS_IPF_seed_format
        self.naics_group_dict = {
            2007: {311:[31131, 3112, 3114, 3115, 3116],
                   3112: [311221], 321: [321113, 3212, 3219], 312: [],
                   3212: [321219], 322: [322110, 322121, 322122, 322130],
                   324: [324110, 324121, 324199],
                   325: [325110, 325120, 325181, 325182, 325188, 325192,
                         325193, 325199, 325211, 325212, 325222, 325311,
                         325312, 325992], 3254: [325412],
                   327: [327121, 327211, 327212, 327213, 327215, 327310,
                         327410, 327420, 327993], 331: [],
                   3313: [331314, 331315, 331316], 3314: [331419],
                   3315: [331511, 331521, 331524], 334: [334413],
                   336: [336111, 336112, 3364], 3364: [336411]},
            2012: {311:[31131, 3112, 3114, 3115, 3116],
                   3112: [311221], 321: [321113, 3212, 3219], 312: [],
                   3212: [321219], 322: [322110, 322121, 322122, 322130],
                   324: [324110, 324121, 324122, 324199],
                   325: [325110, 325120, 325180, 325193, 325194, 325199,
                         325211, 325212, 325220, 325311, 325312, 325992, 3254],
                   3254: [325412],
                   327: [327120, 327211, 327212, 327213, 327215, 327310,
                         327410, 327420, 327993],331: [],
                   3313: [331314, 331315, 331318], 3314: [331410],
                   3315: [331511, 331523, 331524], 334: [334413],
                   336: [336111, 336112, 3364], 3364: [336411]}
            }

        def get_regions(df, df_column):
            """
            Returns dataframe with region values.
            """

            df['region'] = np.nan

            df['region'].update(
                df.loc[
                    df[df_column].apply(lambda x: type(x)) ==str, 'Total'
                    ].apply(lambda x: x.split(' Census Region')[0])
                    )

            df.region.fillna(method='ffill', inplace=True)

            df.replace({'Total United States': 'United States'},
                       inplace=True)

            return df

        def format_mecs_se(df):

            df.iloc[:, 0].fillna(method='ffill', inplace=True)

            df.replace(to_replace={'X':0, '--':0}, value=None, inplace=True)

            df.iloc[:, 0] = df.iloc[:, 0].apply(lambda x: str(x).strip())

            df = get_regions(df, 'Total')

            df.reset_index(inplace=True, drop=True)

            df = pd.DataFrame(df[df.region != 'Total'])

            df.replace(to_replace={' ': np.nan}, inplace=True)

            if 'Characteristic' in df.columns:

                df['Data_cat'] =df[df.Characteristic.isin(
                        ['Value of Shipments and Receipts', 'Employment Size']
                        )].Characteristic

                df.replace(
                    {'Employment Size': 'Employment_size',
                     'Value of Shipments and Receipts': 'Value_of_shipments'},
                     inplace=True
                     )

                df.Data_cat.fillna(method='ffill', inplace=True)

                df.rename(columns={'Characteristic': 'employment_class',
                                      'HGL': 'LPG_NGL'},
                             inplace=True)

            df.dropna(inplace=True)

            df = df.reset_index(drop=True)

            for col in df.columns:

                if col == 'naics':

                    df.loc[:, col] = df[col].astype(int)

                try:

                    df.loc[:, col] = df[col].astype(float)

                    # Standard errors reported as %
                    df.loc[:, col] = df[col].divide(100).multiply(self.ci_mult)

                except ValueError as e:

                    continue

            return df

        self.mecs32 = format_mecs_se(mecs32)

        self.mecs33 = format_mecs_se(mecs33)

        self.mecs35 = format_mecs_se(mecs35)

        self.mecs52 = format_mecs_se(mecs52)

    def calc_sq_se(self, table_avg, table_number):
        """
        Calculate the square of the standard error divided by the mean for
        MECS fuel tables.
        """

        tables = {32: self.mecs32, 33: self.mecs33, 35: self.mecs35,
                  52: self.mecs52}

        se_table = tables[table_number]

        # Calculate the square of the standard error divided by the mean
        sq_se_table = (se_table.divide(table_avg))**2


        return sq_se_table


    def se_seed(self, table_32, table_33, ipf_seed):
        """
        Calculate error propogation for MECS data used to create IPF seed and
        MECS intensities.
        """
        fuels = ['Net_electricity', 'Residual_fuel_oil', 'Diesel',
                 'Natural_gas', 'LPG_NGL', 'Coal', 'Coke_and_breeze', 'Other']

        ase_32 = table_32.set_index('naics', append=True)[fuels].multiply(
                    self.mecs32.set_index(['region', 'naics'])[fuels],
                    fill_value=0
                    )

        ase_33 = table_33[table_33.Data_cat == 'Employment_size'].set_index(
                    ['region', 'employment_class']
                    )[fuels].multiply(
                        self.mecs32[
                                self.mecs32.Data_cat == 'Employment_size'
                                ].set_index(
                                    ['region', 'employment_class']
                                    )[fuels], fill_value=0
                        )

        # Some columns are dtype == Objects. Convert to float.
        for f in fuels:

            for df in [ase_32, ase_33]:

                df[f] = df[f].astype(float)

        ase_seed = pd.concat(
                [ase_32.pivot_table(index=['region'], columns=['naics'],
                                      values=fuels)[f] for f in fuels],
                axis=0
                )

        ase_seed['MECS_FT'] = np.repeat(fuels, len(ase_seed.index.unique()))

        ase_seed.dropna(inplace=True)

        ase_33_melted = ase_33.reset_index().melt(
                id_vars=['region', 'employment_class'], var_name='MECS_FT',
                value_name='std_error'
                ).set_index(['region', 'MECS_FT']).reset_index()

        ase_33_melted = ase_33_melted[
                (ase_33_melted.region != 'United States') &
                (ase_33_melted.employment_class != 'Total')
                ]

        ase_33_melted.set_index(['region', 'MECS_FT', 'employment_class'],
                                inplace=True)

        ase_seed.set_index('MECS_FT', append=True, inplace=True)

        ase_seed = ase_seed.reindex(index=ase_33_melted.index)

        ase_seed = ase_seed.join(ase_33_melted)

        # Calculate error propogation by taking the square root of the
        # sum of squared standard errors
        ase_seed = ase_seed**2

        ase_seed = ase_seed.apply(lambda x: x + x['std_error'], axis=1)

        ase_seed.drop('std_error', axis=1, inplace=True)

        ase_seed = np.sqrt(ase_seed)

        ase_seed.reset_index(inplace=True)

        ase_seed.rename(columns={'employment_class': 'Emp_Size',
                                 'region': 'MECS_Region'},
                        inplace=True)

        ase_seed.replace({'1000 and over': 'n1000', '100-249':'n100_249',
                          'Under 50': 'n1_49', '240-499': 'n250_499',
                          '500-999': 'n500_999', '50-99': 'n50_99'},
                         inplace=True)

        return ase_seed








        #Calculate the absolute standard error for table 3.2 and 3.3
        # table_32.multiply(self.mecs32)
        # table_33.multiply(self.mecs33)
        # Add absolute standard errors by region, NAICS, fuel, and employment size class?
