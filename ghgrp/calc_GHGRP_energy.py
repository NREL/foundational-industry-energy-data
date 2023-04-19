# -*- coding: utf-8 -*-
"""
Created on Wed Mar  6 21:12:25 2019

@author: cmcmilla
"""

import os
import logging
import pandas as pd
import numpy as np
import ghgrp.find_fips as find_fips
import ghgrp.ghg_tiers as ghg_tiers
import ghgrp.get_GHGRP_data as get_GHGRP_data


logging.basicConfig(level=logging.INFO)


class GHGRP:
    """
    Estimates industrial (i.e., manufacturing, ag, construction, mining)
    facility energy use based on either reported energy use or
    reported greenhouse gas emissions.
    """

    table_dict = {'subpartC': 'C_FUEL_LEVEL_INFORMATION',
                  'subpartD': 'D_FUEL_LEVEL_INFORMATION',
                  'subpartV_fac': 'V_GHG_EMITTER_FACILITIES',
                  'subpartV_emis': 'V_GHG_EMITTER_SUBPART',
                  'subpartAA_ff': 'AA_FOSSIL_FUEL_INFORMATION',
                  'subpartAA_liq': 'AA_SPENT_LIQUOR_INFORMATION'}

    tier_data_columns = ['FACILITY_ID', 'REPORTING_YEAR',
                         'FACILITY_NAME', 'UNIT_NAME', 'UNIT_TYPE',
                         'FUEL_TYPE', 'FUEL_TYPE_OTHER',
                         'FUEL_TYPE_BLEND']

    # Set calculation data directories
    file_dir = os.path.abspath('./data/GHGRP')

    if os.path.exists(file_dir):
        pass

    else:
        os.makedirs(file_dir)

    # Set GHGRP data file directory
    ghgrp_file_dir = file_dir

    # List of facilities for correction of combustion emissions from Wood
    # and Wood Residuals for using Subpart C Tier 4 calculation methodology.
    wood_facID = pd.read_csv(
        os.path.abspath(os.path.join(file_dir, 'WoodRes_correction_facilities.csv')),
        index_col=['FACILITY_ID']
        )

    std_efs = pd.read_csv(
        os.path.abspath(os.path.join(file_dir, 'EPA_FuelEFs.csv')),
        index_col=['Fuel_Type']
        )

    std_efs.index.name = 'FUEL_TYPE'

    std_efs = std_efs[~std_efs.index.duplicated()]

    MECS_regions = pd.read_csv(
        os.path.abspath(os.path.join(file_dir, 'US_FIPS_Codes.csv')),
        index_col=['COUNTY_FIPS']
        )

    fac_file_2010 = pd.read_csv(
        os.path.abspath(os.path.join(file_dir, 'fac_table_2010.csv')),
        encoding='latin_1'
        )

    mfips_file = 'found_fips.csv'

    def __init__(self, years, calc_uncertainty):

        if type(years) == tuple:
            self.years = range(years[0], years[1]+1)

        else:
            self.years = [years]

        self.calc_uncertainty = calc_uncertainty

    def format_emissions(self, GHGs):
        """
        Format and correct for odd issues with reported data in subpart C.
        """

        GHGs.dropna(axis=0, subset=['FACILITY_ID'], inplace=True)

        for c in ['FACILITY_ID', 'REPORTING_YEAR']:

            GHGs.loc[:, c] = GHGs[c].astype(int)

        # Adjust multiple reporting of fuel types
        fuel_fix_index = GHGs[(GHGs.FUEL_TYPE.notnull() == True) &
                              (GHGs.FUEL_TYPE_OTHER.notnull() == True)].index

        GHGs.loc[fuel_fix_index, 'FUEL_TYPE_OTHER'] = np.nan

        # Fix errors in reported data.
        if 2014 in self.years:
            # This facility CH4 combustion emissions off by factor of 1000
            i1 = GHGs[(GHGs.FACILITY_ID == 1001143) &
                      (GHGs.REPORTING_YEAR == 2014)].dropna(
                        subset=['T4CH4COMBUSTIONEMISSIONS']
                        ).index
            GHGs.loc[i1, 'T4CH4COMBUSTIONEMISSIONS'] = \
                GHGs.loc[i1, 'T4CH4COMBUSTIONEMISSIONS']/1000
            GHGs.loc[i1, 'ANNUAL_HEAT_INPUT'] = np.nan

            for i in list(GHGs[(GHGs.FACILITY_ID == 1005675) &
                               (GHGs.REPORTING_YEAR == 2014)].index):

                GHGs.loc[i, 'TIER2_CH4_EMISSIONS_CO2E'] = \
                    GHGs.loc[i, 'TIER2_CH4_COMBUSTION_EMISSIONS'] * 25.135135

                GHGs.loc[i, 'TIER2_N2O_EMISSIONS_CO2E'] = \
                    GHGs.loc[i, 'TIER2_N2O_COMBUSTION_EMISSIONS'] * 300

            for i in GHGs[(GHGs.FACILITY_ID == 1001143) & (GHGs.REPORTING_YEAR == 2014)].index:

                for c in ['T4CH4COMBUSTIONEMISSIONS',
                          'T4N2OCOMBUSTIONEMISSIONS']:

                    GHGs.loc[i, c] = GHGs.loc[i, c]/1000

        if 2012 in self.years:

            selection = GHGs.loc[
                (GHGs.FACILITY_ID == 1000415) &
                (GHGs.FUEL_TYPE == 'Bituminous') &
                (GHGs.REPORTING_YEAR == 2012)
                ].index

            GHGs.loc[
                selection,
                ('T4CH4COMBUSTIONEMISSIONS'):('TIER4_N2O_EMISSIONS_CO2E')
                ] = GHGs.loc[
                        selection,
                        ('T4CH4COMBUSTIONEMISSIONS'):('TIER4_N2O_EMISSIONS_CO2E')
                        ] / 10

        total_co2 = pd.DataFrame()

        for tier in ['TIER1_', 'TIER2_', 'TIER3_']:
            for ghg in ['CH4_EMISSIONS_CO2E', 'N2O_EMISSIONS_CO2E',
                        'CO2_COMBUSTION_EMISSIONS']:

                total_co2 = pd.concat([total_co2, GHGs[tier + ghg]], axis=1)

        for ghg in ['TIER4_CH4_EMISSIONS_CO2E', 'TIER4_N2O_EMISSIONS_CO2E']:
            total_co2 = pd.concat([total_co2, GHGs[ghg]], axis=1)

        total_co2.fillna(0, inplace=True)

        GHGs['MTCO2e_TOTAL'] = total_co2.sum(axis=1)

        for c in ['FACILITY_ID', 'REPORTING_YEAR']:

            GHGs[c] = GHGs[c].astype(int)

        return GHGs

    def format_facilities(self, oth_facfile):
        """
        Format csv file of facility information. Requires list of facility
        files for 2010 and for subsequent years.
        Assumes 2010 file has the correct NAICS code for each facilitiy;
        subsequent years default to the code of the first year a facility
        reports.
        """

        def fac_read_fix(ffile):
            """
            Reads and formats facility csv file.
            """

            if type(ffile) == pd.core.frame.DataFrame:
                facdata = ffile.copy(deep=True)

            else:
                facdata = pd.read_csv(ffile)

            # Duplicate entries in facility data query. Remove them to enable a
            # # 1:1 mapping of facility info with ghg data via FACILITY_ID.
            # # First ID facilities that have cogen units.
            fac_cogen = facdata.FACILITY_ID[
                facdata['COGENERATION_UNIT_EMISS_IND'] == 'Y'
                ]

            facdata.dropna(subset=['FACILITY_ID'], inplace=True)

            # Reindex dataframe based on facility ID
            facdata.FACILITY_ID = facdata.FACILITY_ID.astype(int)

            # Correct PRIMARY_NAICS_CODE from 561210 to 324110 for Sunoco
            # Toldeo Refinery (FACILITY_ID == 1001056); correct
            # PRIMARY_NAICS_CODE from 331111 to 324199 for Mountain
            # State Carbon, etc.
            fix_dict = {1001056: {'PRIMARY_NAICS_CODE': 324110},
                        1001563: {'PRIMARY_NAICS_CODE': 324119},
                        1006761: {'PRIMARY_NAICS_CODE': 331221},
                        1001870: {'PRIMARY_NAICS_CODE': 325110},
                        1006907: {'PRIMARY_NAICS_CODE': 424710},
                        1006585: {'PRIMARY_NAICS_CODE': 324199},
                        1002342: {'PRIMARY_NAICS_CODE': 325222},
                        1002854: {'PRIMARY_NAICS_CODE': 322121},
                        1007512: {'SECONDARY_NAICS_CODE': 325199},
                        1004492: {'PRIMARY_NAICS_CODE': 541712},
                        1002434: {'PRIMARY_NAICS_CODE': 322121,
                                  'SECONDARY_NAICS_CODE': 322222},
                        1002440: {'SECONDARY_NAICS_CODE': 221210,
                                  'PRIMARY_NAICS_CODE': 325311},
                        1003006: {'PRIMARY_NAICS_CODE': 424710},
                        1004861: {'PRIMARY_NAICS_CODE': 325193},
                        1005954: {'PRIMARY_NAICS_CODE': 311211},
                        1004098: {'PRIMARY_NAICS_CODE': 322121},
                        1005445: {'PRIMARY_NAICS_CODE': 331524}}

            for k, v in fix_dict.items():

                facdata.loc[facdata[facdata.FACILITY_ID == k].index,
                            list(v)[0]] = list(v.values())[0]

            cogen_index = facdata[facdata.FACILITY_ID.isin(fac_cogen)].index

            # Re-label facilities with cogen units
            facdata.loc[cogen_index, 'COGENERATION_UNIT_EMISS_IND'] = 'Y'

            facdata['MECS_Region'] = ""

            facdata.set_index(['FACILITY_ID'], inplace=True)

            return facdata

        all_fac = fac_read_fix(self.fac_file_2010)

        all_fac = all_fac.append(fac_read_fix(oth_facfile))

        # Drop duplicated facility IDs, keeping first instance (i.e., year).
        all_fac = \
            pd.DataFrame(all_fac[~all_fac.index.duplicated(keep='first')])

        # Identify facilities with missing County FIPS data and fill missing
        # data. Most of these facilities are mines or natural gas/crude oil
        # processing plants.
        ff_index = all_fac[all_fac.COUNTY_FIPS.isnull() == False].index

        all_fac.loc[ff_index, 'COUNTY_FIPS'] = \
            [np.int(x) for x in all_fac.loc[ff_index, 'COUNTY_FIPS']]

    #    Update facility information with new county FIPS data
        missingfips = pd.DataFrame(
                all_fac[all_fac.COUNTY_FIPS.isnull() == True]
                )

        # Check if missing fips in file
        if self.mfips_file in os.listdir(self.file_dir):

            found_fips = pd.read_csv(
                os.path.join(self.file_dir, self.mfips_file)
                )

            missingfips = pd.merge(missingfips, found_fips,
                                   on=['FACILITY_ID']
                                   )

        else:

            missingfips.loc[:, 'COUNTY_FIPS'] = \
                [find_fips.fipfind(
                        self.file_dir, i, missingfips
                        ) for i in missingfips.index]

        all_fac.loc[missingfips.index, 'COUNTY_FIPS'] = missingfips.COUNTY_FIPS

        all_fac['COUNTY_FIPS'].fillna(0, inplace=True)

    #    Assign MECS regions and NAICS codes to facilities and merge location
    #    data with GHGs dataframe.
    #    EPA data for some facilities are missing county fips info
        all_fac.COUNTY_FIPS = all_fac.COUNTY_FIPS.apply(np.int)

        concat_mecs_region = \
            pd.concat(
                [all_fac.MECS_Region, self.MECS_regions.MECS_Region], axis=1,
                join_axes=[all_fac.COUNTY_FIPS]
                )

        all_fac.loc[:, 'MECS_Region'] = concat_mecs_region.iloc[:, 1].values

        all_fac.rename(columns={'YEAR': 'FIRST_YEAR_REPORTED'}, inplace=True)

        all_fac.reset_index(drop=False, inplace=True)

        return all_fac

    # Get data from EPA API if not available
    def import_data(self, subpart):
        """
        Download EPA data via API if emissions data are not saved locally.
        """

        def download_or_read_ghgrp_file(subpart, filename):
            """
            Method for checking for saved file or calling download method
            for all years in instantiated class.
            """

            ghgrp_data = pd.DataFrame()

            table = self.table_dict[subpart]

            for y in self.years:

                filename_y = f'{filename}{y}.csv'

                if filename_y in os.listdir(os.path.abspath(self.ghgrp_file_dir)):

                    data_y = pd.read_csv(
                        os.path.abspath(
                            os.path.join(self.ghgrp_file_dir, filename_y)
                            ),
                        encoding='latin_1', low_memory=False,
                        index_col=0
                        )

                else:

                    data_y = get_GHGRP_data.get_GHGRP_records(y, table)
                    data_y.to_csv(
                        os.path.abspath(
                            os.path.join(self.ghgrp_file_dir, filename_y))
                        )

                ghgrp_data = ghgrp_data.append(data_y, ignore_index=True)

                # API has changed. Columns returned are now all in lowercase.
            if ghgrp_data.columns[0] == ghgrp_data.columns[0].upper():
                pass

            else:
                ghgrp_data.columns = [x.upper() for x in ghgrp_data.columns]

            return ghgrp_data

        if subpart == 'subpartC':

            filename = self.table_dict[subpart][0:7].lower()

            ghgrp_data = download_or_read_ghgrp_file(subpart, filename)

            formatted_ghgrp_data = self.format_emissions(ghgrp_data)

            return formatted_ghgrp_data

        if subpart == 'subpartD':

            filename = self.table_dict[subpart][0:7].lower()

            ghgrp_data = download_or_read_ghgrp_file(subpart, filename)

            for c in ['N2O_EMISSIONS_CO2E', 'CH4_EMISSIONS_CO2E']:

                ghgrp_data[c] = ghgrp_data[c].astype('float32')

            ghgrp_data['MTCO2e_TOTAL'] = ghgrp_data.N2O_EMISSIONS_CO2E.add(
                ghgrp_data.CH4_EMISSIONS_CO2E
                )

            if ghgrp_data[(ghgrp_data.FUEL_TYPE.notnull()) &
                          (ghgrp_data.FUEL_TYPE_OTHER.notnull())].empty !=True:

                fuel_index = ghgrp_data[
                        (ghgrp_data.FUEL_TYPE.notnull()) &
                        (ghgrp_data.FUEL_TYPE_OTHER.notnull())
                        ].index

                ghgrp_data.loc[fuel_index, 'FUEL_TYPE_OTHER'] = np.nan

            formatted_ghgrp_data = pd.DataFrame(ghgrp_data)

            return formatted_ghgrp_data

        if subpart == 'subpartV_fac':
            filename = 'fac_table_'
            ghgrp_data = download_or_read_ghgrp_file(subpart, filename)
            formatted_ghgrp_data = self.format_facilities(ghgrp_data)

            return formatted_ghgrp_data

        if subpart == 'subpartAA_liq':
            filename = 'aa_sl_'
            formatted_ghgrp_data = \
                download_or_read_ghgrp_file(subpart, filename)
            formatted_ghgrp_data['REPORTING_YEAR'] = \
                formatted_ghgrp_data.REPORTING_YEAR.astype(int)
            pre2013_emissions = formatted_ghgrp_data[
                formatted_ghgrp_data.REPORTING_YEAR <= 2012
                ].SPENT_LIQUOR_CH4_EMISSIONS

            # Pre 2013 overestimates of CH4 emissions appear to be ~15.79x
            # greater.
            pre2013_emissions = pre2013_emissions.divide(15.79)

            formatted_ghgrp_data.SPENT_LIQUOR_CH4_EMISSIONS.update(
                    pre2013_emissions
                    )

            return formatted_ghgrp_data

        else:

            if subpart == 'subpartV_emis':
                filename = 'V_GHGs_'

            if subpart == 'subpartAA_ff':
                filename = 'aa_ffuel_'

            formatted_ghgrp_data = \
                download_or_read_ghgrp_file(subpart, filename)

            return formatted_ghgrp_data

    def calc_energy_subC(self, formatted_subC, all_fac):
        """
        Apply MMBTU_calc_CO2 function to EPA emissions table Tier 1, Tier 2,
        and Tier 3 emissions; MMBTU_calc_CH4 for to Tier 4 CH4 emissions.
        Adds heat content of fuels reported under 40 CFR Part 75 (electricity
        generating units and other combustion sources covered under EPA's
        Acid Rain Program).
        """
        energy_subC = formatted_subC.copy(deep=True)

        # Capture energy data reported under Part 75 facilities
        part75_mmbtu = pd.DataFrame(formatted_subC[
                formatted_subC.PART_75_ANNUAL_HEAT_INPUT.notnull()
                ])

        part75_mmbtu.rename(
            columns={'PART_75_ANNUAL_HEAT_INPUT': 'MMBtu_TOTAL'},
            inplace=True
            )

        # Correct for revision in 2013 to Table AA-1 emission factors for kraft
        # pulping liquor emissions. CH4 changed from 7.2g CH4/MMBtu HHV to
        # 1.9g CH4/MMBtu HHV.
        energy_subC.loc[:, 'wood_correction'] = \
            [x in self.wood_facID.index for x in energy_subC.FACILITY_ID] and \
            [f == 'Wood and Wood Residuals' for f in energy_subC.FUEL_TYPE] and \
            [x in [2010, 2011, 2012] for x in energy_subC.REPORTING_YEAR]

        energy_subC.loc[(energy_subC.wood_correction == True),
            'T4CH4COMBUSTIONEMISSIONS'] =\
            energy_subC.loc[
                (energy_subC.wood_correction == True),
                'T4CH4COMBUSTIONEMISSIONS'
                ].multiply(1.9 / 7.2)

        # Separate, additional correction for facilities appearing to have
        # continued reporting with previous CH4 emission factor for kraft
        # liqour combusion (now reported as Wood and Wood Residuals
        # (dry basis).
        wood_fac_add = [1001892, 1005123, 1006366, 1004396]

        energy_subC.loc[:, 'wood_correction_add'] = \
            [x in wood_fac_add for x in energy_subC.FACILITY_ID] and \
            [y == 2013 for y in energy_subC.REPORTING_YEAR]

        energy_subC.loc[(energy_subC.wood_correction_add == True) &
            (energy_subC.FUEL_TYPE == 'Wood and Wood Residuals (dry basis)'),
                'T4CH4COMBUSTIONEMISSIONS'] =\
                energy_subC.loc[(energy_subC.wood_correction_add == True) &
                    (energy_subC.FUEL_TYPE == 'Wood and Wood Residuals (dry basis)'),
                        'T4CH4COMBUSTIONEMISSIONS'].multiply(1.9 / 7.2)

        tier_calcs = ghg_tiers.tier_energy(years=self.years,
                                          std_efs=self.std_efs)

        # New method for calculating energy based on tier methodology
        energy_subC = tier_calcs.calc_all_tiers(energy_subC)

        part75_subC_columns = list(
                energy_subC.columns.intersection(part75_mmbtu.columns)
                )

        energy_subC = energy_subC.append(part75_mmbtu[part75_subC_columns])

        energy_subC['GWh_TOTAL'] = energy_subC['MMBtu_TOTAL']/3412.14
        energy_subC['TJ_TOTAL'] = energy_subC['GWh_TOTAL'] * 3.6

        merge_cols = list(all_fac.columns.difference(energy_subC.columns))
        merge_cols.append('FACILITY_ID')

        energy_subC = pd.merge(
            energy_subC, all_fac[merge_cols], how='left', on='FACILITY_ID'
            )

        return energy_subC


    def calc_energy_subD(self, formatted_subD, all_fac):
        """
        Heat content of fuels reported under 40 CFR Part 75 (electricity
        generating units and other combustion sources covered under EPA's
        Acid Rain Program).
        """

        merge_cols = list(
                all_fac.columns.difference(formatted_subD.columns))

        merge_cols.append('FACILITY_ID')

        energy_subD = pd.merge(
            formatted_subD, all_fac[merge_cols], how='left', on='FACILITY_ID'
            )

    #   First, drop 40 CFR Part 75 energy use for electric utilities

        energy_subD = pd.DataFrame(
                energy_subD.where(energy_subD.PRIMARY_NAICS_CODE !=221112)
                ).dropna(subset=['PRIMARY_NAICS_CODE'], axis=0)

#        energy_subD.loc[
#            energy_subD[energy_subD.PRIMARY_NAICS_CODE == 221112
#            ].index, 'TOTAL_ANNUAL_HEAT_INPUT'] = 0

        energy_subD.rename(columns={'TOTAL_ANNUAL_HEAT_INPUT':'MMBtu_TOTAL'},
                           inplace=True)

        energy_subD['GWh_TOTAL'] = energy_subD['MMBtu_TOTAL']/3412.14

        energy_subD['TJ_TOTAL'] = energy_subD['GWh_TOTAL'] * 3.6

        energy_subD.dropna(axis=1, how='all', inplace=True)

        return energy_subD

    @staticmethod
    def energy_merge(energy_subC, energy_subD, energy_subAA, all_fac):

        merge_cols = list(all_fac.columns.difference(energy_subAA.columns))

        merge_cols.append('FACILITY_ID')

        energy_subAA = pd.merge(
            energy_subAA, all_fac[merge_cols], how='left', on='FACILITY_ID'
            )

        ghgrp_energy = pd.DataFrame()

        for df in [energy_subC, energy_subD, energy_subAA]:

            ghgrp_energy = ghgrp_energy.append(df, ignore_index=True,
                                               sort=True)

        # Drop all facilities that do not have an industrial primary or
        # secondary NAICS code
        ghgrp_energy.dropna(subset=['PRIMARY_NAICS_CODE'], inplace=True)

        ghgrp_energy['NAICS2_p'] = ghgrp_energy.PRIMARY_NAICS_CODE.apply(
                lambda x: int(str(x)[0:2])
                )

        ghgrp_energy['NAICS2_s'] = \
            ghgrp_energy.SECONDARY_NAICS_CODE.dropna().apply(
                lambda x: int(str(x)[0:2])
                )

        ghgrp_energy = pd.DataFrame(
                ghgrp_energy[
                    (ghgrp_energy.NAICS2_p.isin([11, 21, 23, 31, 32, 33])) |
                    (ghgrp_energy.NAICS2_s.isin([11, 21, 23, 31, 32, 33]))
                    ])

        ghgrp_energy.drop(['NAICS2_p', 'NAICS2_s'], axis=1, inplace=True)

        for col in ['FACILITY_ID', 'PRIMARY_NAICS_CODE', 'ZIP',
                    'REPORTING_YEAR']:

            ghgrp_energy[col] = ghgrp_energy[col].astype(int)

        # Drop all MMBtu_TOTAL == Nan
        ghgrp_energy = ghgrp_energy.dropna(subset=['MMBtu_TOTAL'])

        return ghgrp_energy
