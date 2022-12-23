# -*- coding: utf-8 -*-
"""
Created on Wed Aug 30 09:52:53 2017

@author: cmcmilla
"""
import pandas as pd
import itertools
#%%
class subpartAA:

    def __init__(self, aa_ff, aa_sl, std_efs):

        for aa_df in [aa_ff, aa_sl]:

            aa_df.dropna(axis=0, subset=['FACILITY_ID'], inplace=True)

            for c in ['FACILITY_ID', 'REPORTING_YEAR']:

                aa_df.loc[:, c] = aa_df[c].astype(int)

        self.aa_ff = aa_ff

        self.aa_sl = aa_sl

        self.std_efs = std_efs

        # Default CH4 emissions factor (kg CH4/MMBtu) for wood and wood
        # residuals. Note that this value changed from 0.0072 kg CH4/MMBtu,
        # used in 2010 - 2013 to 0.0019 kg CH4/MMBtu for 2014 onwards. An
        # adjustment to reported CH4 emissions is made when the data are
        # imported.
        self.std_ch4 = 0.0019

    def calc_energy_ff(self):
        """
        Calculate MMBtu value based on reported CO2 emissions.
        Does not capture emissions and energy from facilities using Tier 4
        calculation methodology.
        """
        # Drop facilities that do not report a fuel type.
        energy_aa_ff = self.aa_ff.copy(deep=True)

        energy_aa_ff.dropna(subset=['FUEL_TYPE'], inplace=True, axis=0)

        energy_aa_ff['TOTAL_CO2_EMISSIONS'] = \
            energy_aa_ff[['TIER_1_CO2_EMISSIONS', 'TIER_2_CO2_EMISSIONS',
            'TIER_3_CO2_EMISSIONS']].sum(axis=1)

        energy_aa_ff = pd.merge(
            energy_aa_ff,
            self.std_efs.reset_index()[['FUEL_TYPE', 'CO2_kgCO2_per_mmBtu']],
            on='FUEL_TYPE', how='left'
            )

        energy_aa_ff['MMBtu_TOTAL'] = \
            energy_aa_ff.TOTAL_CO2_EMISSIONS.divide(
                    energy_aa_ff.CO2_kgCO2_per_mmBtu
                    )*1000

        co2e_columns = [t+c for t in ['TIER_1', 'TIER_2', 'TIER_3'] for c in
            ['_CO2_EMISSIONS','_CH4_EMISSIONS_CO2E', '_N2O_EMISSIONS_CO2E']]

        energy_aa_ff.loc[:, 'MTCO2e_TOTAL'] = \
            energy_aa_ff[co2e_columns].sum(axis=1)

        return energy_aa_ff


    def calc_energy_sl(self):
        """
        Calculate MMBtu value of spent liquor combustion based on CH4
        emissions reported under Tier 4.
        """
        energy_aa_sl = self.aa_sl.copy(deep=True)

        energy_aa_sl.BIOMASS_CH4_EMISSIONS_FACTOR.fillna(
                value=self.std_ch4, inplace=True
                )

        energy_aa_sl['MMBtu_TOTAL'] = \
            energy_aa_sl.SPENT_LIQUOR_CH4_EMISSIONS.multiply(1000).divide(
                energy_aa_sl.BIOMASS_CH4_EMISSIONS_FACTOR, fill_value=0
                )

        # Calculate CO2e emissions using 100-year GWP from IPCC AR4
        # CH4 and N2O reported in same units as CO2 (metric tons?)
        energy_aa_sl.loc[:, 'CH4_CO2E'] = \
            energy_aa_sl.SPENT_LIQUOR_CH4_EMISSIONS.multiply(25)

        energy_aa_sl.loc[:, 'N2O_CO2E'] = \
            energy_aa_sl.SPENT_LIQUOR_N2O_EMISSIONS.multiply(298)

        # Calculate total CO2e emissions in metric tons
        energy_aa_sl.loc[:, 'MTCO2e_TOTAL'] = energy_aa_sl[
                ['SPENT_LIQUOR_CO2_EMISSIONS', 'CH4_CO2E', 'N2O_CO2E']
                ].sum(axis=1)

        energy_aa_sl.drop([
            'SPENT_LIQUOR_CO2_EMISSIONS', 'SPENT_LIQUOR_CH4_EMISSIONS',
            'SPENT_LIQUOR_N2O_EMISSIONS', 'BIOMASS_CH4_EMISSIONS_FACTOR',
            'BIOMASS_N2O_EMISSIONS_FACTOR'
            ], axis=1, inplace=True)

        energy_aa_sl['FUEL_TYPE'] = 'Wood and Wood Residuals'

        return energy_aa_sl


    def energy_calc(self):
        """
        Merge resuts of Subpart AA energy calculations into a single dataframe.
        """

        energy_AA = pd.concat(
            [self.calc_energy_ff(), self.calc_energy_sl()], ignore_index=True,
            axis=0
            )

        energy_AA['GWh_TOTAL'] = energy_AA['MMBtu_TOTAL']/3412.14

        energy_AA['TJ_TOTAL'] = energy_AA['GWh_TOTAL'] * 3.6

        return energy_AA
