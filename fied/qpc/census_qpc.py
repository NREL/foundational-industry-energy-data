import pandas as pd
import numpy as np
import os
import urllib
import sys
sys.path.append(os.path.abspath(""))
from fied.tools.naics_matcher import naics_matcher

from fied.datasets import fetch_QPC

class QPC:

    def __init__(self):

        self._data_path = os.path.abspath('./data/QPC/')

    @staticmethod
    def force_format(naics):
        """
        Some NAICS aren't converting from string to int using .astype
        e.g., '31519'

        """

        try:
            naics = int(naics)

            return naics

        except ValueError:

            return naics

    @staticmethod
    def format_naics(df):
        """
        Formats results that aggregate NAICS codes (e.g., "3113, 4")
        """

        for naics in df.NAICS.unique():

            all_naics = []

            if naics == '31-33':

                continue

            if type(naics) != str:

                continue

            elif ',' in naics:

                all_naics.append(int(naics.split(',')[0]))

                for n in naics.split(',')[1:]:

                    n = n.strip()

                    if '-' in n:

                        for m in range(
                            int(naics.split('-')[0][-1])+1,
                                int(naics.split('-')[1])+1
                                ):

                            all_naics.append(
                                int(naics.split(',')[0][:-1]+str(m))
                                )

                    else:

                        all_naics.append(
                            int(naics.split(',')[0][:-1]+n)
                            )

            elif (',' not in naics) & ('-' in naics):

                all_naics.append(int(naics.split('-')[0]))

                for m in range(
                    int(naics.split('-')[0][-1])+1,
                        int(naics.split('-')[1])+1
                    ):

                    all_naics.append(
                        int(naics.split('-')[0][:-1]+str(m))
                        )

            new_rows = pd.DataFrame(
                np.tile(df[df.NAICS == naics].values,
                (len(all_naics), 1)), columns=df.columns
                )

            new_rows['NAICS'] = np.repeat(
                all_naics,
                len(new_rows)/len(all_naics)
                )

            # Delete original data
            df = df[df.NAICS != naics]

            df = df.append(new_rows, ignore_index=True)

        return df

    def get_qpc_data(self, year, include_all=False):
        """
        Quarterly survey began 2008; start with 2010 due to  2007-2009
        recession.
        """
        qpc_data = fetch_QPC(year)

        if not include_all:
            # Don't use the aggregate manufacturing NAICS
            qpc_data = qpc_data.query("NAICS != '31-33'")

        qpc_data.NAICS.update(
            qpc_data.NAICS.apply(lambda x: QPC.force_format(x))
            )

        qpc_data = QPC.format_naics(qpc_data)

        # Drop withheld estimates
        qpc_data = qpc_data[qpc_data.Weekly_op_hours != 'D']

        #Interpolate for single value == 'S'
        qpc_data.replace({'S': np.nan, 'Z': np.nan}, inplace=True)

        qpc_data.Weekly_op_hours.update(
            qpc_data.Weekly_op_hours.interpolate()
            )

        qpc_data['Hours_Standard Error'].update(
            qpc_data['Hours_Standard Error'].interpolate()
            )

        qpc_data.fillna(0, inplace=True)

        qpc_data['Weekly_op_hours'] = \
            qpc_data.Weekly_op_hours.astype(np.float32)

        return qpc_data

    def calc_hours_CI(self, selected_qpc_data, CI=95):
        """
        Calculates confidence interval of average weekly operating hours using
        reported standard error.

        Parameters
        ----------
        selected_qpc_data : pandas.DataFrame
            DataFrame of formatted QPC data for one year

        CI : int; 90, 95, or 99. Default is 95
            Confidence interval to use.

        Returns
        -------
        selected_qpc_data : pandas.DataFrame
            Original DataFRame updated with columns for
        high and low weekly operating hours. 

        """

        #t distribution values for CI probabilities
        ci_dict = {90: 1.65, 95: 1.96, 99: 2.58}

        qpc_data_ci = \
            selected_qpc_data['Hours_Standard Error'].astype(np.float32) * ci_dict[CI]

        selected_qpc_data.loc[:, 'Weekly_op_hours_high'] = \
            selected_qpc_data.Weekly_op_hours.add(qpc_data_ci)

        selected_qpc_data['Weekly_op_hours_low'] = \
            selected_qpc_data.Weekly_op_hours.subtract(qpc_data_ci)
        
        # Constrain to positive operating hours.
        # Assume annual minimum by NAICS; if NaN, min of data set.
        op_hour_min = selected_qpc_data[
            selected_qpc_data.Weekly_op_hours_low > 0
            ].groupby('NAICS').Weekly_op_hours_low.min()
        
        neg_hour = selected_qpc_data[
            selected_qpc_data.Weekly_op_hours_low < 0
            ]
        
        selected_qpc_data.Weekly_op_hours_low.update(
            neg_hour.NAICS.map(op_hour_min.to_dict()).fillna(op_hour_min.min())
            )

        # op_hour_min = selected_qpc_data.where(
        #     selected_qpc_data.Weekly_op_hours_low > 0
        #     ).Weekly_op_hours_low.fillna(np.nan).min()

        # selected_qpc_data.loc[:, 'Weekly_op_hours_low'] = selected_qpc_data.where(
        #     selected_qpc_data.Weekly_op_hours_low > 0
        #     ).Weekly_op_hours_low.fillna(op_hour_min)

        # Constrain to <168 weekly operating hours
        selected_qpc_data.loc[:, 'Weekly_op_hours_high'] = selected_qpc_data.where(
            selected_qpc_data.Weekly_op_hours_high < 168
            ).Weekly_op_hours_high.fillna(168)

        return selected_qpc_data

    def format_foundational(self, qpc_data):
        """
        Reformat QPC data, including CI calculations.

        """
        qpc_data.rename(columns={
            'Weekly_op_hours_low': 'weeklyOpHoursLow',
            'Weekly_op_hours': 'weeklyOpHours',
            'Weekly_op_hours_high': 'weeklyOpHoursHigh'},
            inplace=True
            )

        qpc_data = qpc_data.pivot(
            index='NAICS',
            columns='Q',
            values=['weeklyOpHoursLow', 'weeklyOpHours', 'weeklyOpHoursHigh']
            )

        naics_6d = naics_matcher(
            qpc_data.reset_index().NAICS
            )

        naics_6d.rename(columns={'n6': 'naicsCode'},
                        inplace=True)

        naics_6d.set_index('NAICS', inplace=True)

        qpc_data = pd.merge(
            qpc_data, naics_6d,
            left_index=True, right_index=True, how='inner'
            )

        qpc_data.reset_index(drop=True, inplace=True)

        col_rename = {}

        for c in qpc_data.columns:
            if type(c) is tuple:
                col_rename[c] = '_'.join(c)
            else:
                pass

        qpc_data.rename(columns=col_rename, inplace=True)

        return qpc_data

    def main(self, year):
        qpc_data = self.get_qpc_data(year)
        qpc_data = self.calc_hours_CI(qpc_data)
        qpc_data = self.format_foundational(qpc_data)

        return qpc_data

if __name__ == '__main__':
    qpc = QPC()
    qpc_data = pd.DataFrame()
    qpc_data = qpc.get_qpc_data(2017)
    qpc_data = qpc.calc_hours_CI(qpc_data)