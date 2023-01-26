# -*- coding: utf-8 -*-
"""
Created on Fri Jan 13 13:45:19 2017
@author: ookie
Modified on Thur Feb 9 16:09:00 2017 by colin
"""

import numpy as np
import os
import itertools as itools
import pandas as pd

class IPF:

    def __init__(self, year, table3_2, table3_3):

        self.__location__ =  os.path.join('./', 'calculation_data/')

        self.year = year

        self.naics_df = pd.DataFrame(table3_2.reset_index())
        self.naics_df = self.naics_df.sort_values(by=['region', 'naics'])

        self.emply_df = pd.DataFrame(table3_3)

        self.empsize_dict = {'Under 50': 'n1_49', '50-99': 'n50_99',
                     '100-249': 'n100_249','250-499': 'n250_499',
                     '500-999': 'n500_999','1000 and Over': 'n1000'}

        self.colDict = {'regions': ['Northeast', 'Midwest', 'South', 'West'],
           'energy': ['Net_electricity', 'Residual_fuel_oil', 'Diesel',
                      'Natural_gas', 'LPG_NGL', 'Coal',
                      'Coke_and_breeze','Other'],
           'employment': ['Under 50', '50-99','100-249', '250-499',
                          '500-999', '1000 and Over'],
           'value': ['Under 20', '20-49', '50-99', '100-249', '250-499',
                     '500 and Over']}

        self.colDict['energy'].sort()

        def combine(self, columns):
            """
            Takes values in the dictionary above and creates a list of all
            combinations
            """

            labels = [self.colDict[x] for x in columns]
            labels = list(itools.product(*labels))

            output = []

            for i,label in enumerate(labels):
                output.append('_'.join(label))

            return output

        #create column headings that have combinations of regions and energy
        #carriers
        self.headings = combine(self, ['regions', 'energy'])
        self.headings_all = combine(self, ['regions','energy','employment'])

    @staticmethod
    def calc_ipf2D(seed, col, row):
        """
        Core two-dimensional iterative proportional fitting algorithm.
        col matrix should have dimensions of (m,1)
        row matrix should have dimensions of (1,n)
        seed matrix should have dimensions of (m,n)
        """

        col_dim = col.shape[0]
        row_dim = row.shape[1]

        for n in range(3000): #set maximumn number of iterations
            error = 0.0
            #faster 'pythonic(?)' version
            sub = seed.sum(axis=1,keepdims=True)
            sub = np.divide(col, sub, where=sub>0)
            sub[np.isnan(sub)] = 0.0
            sub = sub.flatten()
            sub = np.repeat(sub[:, np.newaxis],row_dim,axis=1)
            seed = seed*sub
            diff = (seed.sum(axis=1, keepdims=True)-col)
            diff = diff*diff
            error += diff.sum()
            sub = seed.sum(axis=0, keepdims=True)
            sub = np.divide(row, sub, where=sub>0)
            sub[np.isnan(sub)] = 0.0
            sub = sub.flatten()
            sub = np.repeat(sub[:, np.newaxis],col_dim,axis=1)
            sub = sub.transpose()
            seed = seed*sub
            diff = (seed.sum(axis=0, keepdims=True)-row)
            diff = diff*diff
            diff = diff.sum()
            error = np.sqrt(error)
            if error < 1e-15: break
        #report error if max iterations reached
        if error > 1e-13: print("Max Iterations ", error)

        return seed

    def mecs_ipf(self, seed_df):
        """
        Set up and run 2-D IPF to estimate MECS fuel use by industry,
        region, fuel type, and employement size class.
        naics_df == MECS table 3.2
        emply_df == MECS table 3.3
        """

        seed_shop = seed_df.copy(deep=True)
        new_cols = ['region', 'index']
        for naics in np.sort(seed_shop.columns[2:]):
            new_cols.append(naics)
        seed_shop = seed_shop[new_cols]
        seed_shop.set_index(['region', 'index'], inplace=True)
        seed_shop = seed_shop.T
        seed_shop_dict = {}

        # Iterate through all of the fuel types
        first = True
        for r in range(0, len(self.colDict['energy'])):
            counter = 6 * r
            fuel = self.colDict['energy'][r]
            for reg in self.colDict['regions']:
                seed_shop_dict[reg] = \
                    seed_shop[reg].iloc[:, (0+counter):(6+counter)]
                col = self.naics_df[self.naics_df.region==reg][fuel].values
                row = self.emply_df[
                        (self.emply_df.region==reg) &
                        (self.emply_df.Data_cat=='Employment_size')
                        ][fuel].values
                col = np.array([col]).astype(float)
                row = np.array([row]).astype(float)
                col = np.transpose(col)
                seed = np.array(seed_shop_dict[reg].iloc[0:81,:].astype(float))

                if first:
                    # Something awry with previous method of stacking numpy
                    # arrays. Just using Dataframes instead.
#                    naics_emply = self.ipf2D_calc(seed, col, row)
                    naics_emply = pd.DataFrame.from_records(
                             IPF.calc_ipf2D(seed, col, row),
                             columns=self.empsize_dict.values()
                             )

                    naics_emply['MECS_Region'] = reg
                    naics_emply['MECS_FT'] = fuel
                    naics_emply['naics'] = self.naics_df.naics.unique()

                else:
#                    naics_emply = np.hstack(
#                            (naics_emply, self.ipf2D_calc(seed, col, row))
#                            )

                    ipf_results = pd.DataFrame.from_records(
                             IPF.calc_ipf2D(seed, col, row),
                             columns=self.empsize_dict.values()
                             )

                    ipf_results['MECS_Region'] = reg
                    ipf_results['MECS_FT'] = fuel
                    ipf_results['naics'] = self.naics_df.naics.unique()
                    naics_emply = naics_emply.append(ipf_results,
                                                     ignore_index=True)
                first = False

        ipf_results_formatted = pd.melt(
                naics_emply, id_vars=['MECS_Region', 'MECS_FT', 'naics'],
                var_name='Emp_Size'
                )

        # Check energy sum of IPF (in TBtu):
        print('Total IPF energy (TBtu): ', '\n',
              ipf_results_formatted.value.sum())

        ipf_results_formatted.fillna(0, inplace=True)

        ipf_results_formatted = pd.pivot_table(
                ipf_results_formatted, index=['MECS_Region', 'MECS_FT',
                                              'Emp_Size'],
                values='value', columns='naics', aggfunc=sum
                )

        ipf_results_formatted.reset_index(inplace=True)

        filename = 'mecs_' + str(self.year) + \
            '_ipf_results_naics_employment.csv'

        ipf_results_formatted.to_csv(self.__location__+filename)

#    def interpolate_ipf(self, years=(2010, 2014)):
#        """
#        Interpolate between-year IPF results for two MECS years
#        (e.g., 2010 and 2014)
#        """
#
#        ipf_results_y1 = pd.read_csv(
#                self.__location__ + 'mecs_'+str(years[0])+\
#                '_ipf_results_naics_employment.csv'
#                )
#
#        ipf_results_y2 = pd.read_csv(
#                self.__location__ + 'mecs_'+str(years[1])+\
#                '_ipf_results_naics_employment.csv'
#                )
#
#        columns_all_ipf = [x for x in itools.product(
#                range(years), ipf_results_y1.columns
#                )]
#
#        ipf_all = pd.DataFrame(columns=columns_all_ipf)
#
#        for df in [ipf_results_y1, ipf_results_y2]:
#
#
#
#        filename = 'mecs_' + str() + \
#            '_ipf_results_naics_employment.csv'
#
#        ipf_results_formatted.to_csv(self.__location__+filename)
