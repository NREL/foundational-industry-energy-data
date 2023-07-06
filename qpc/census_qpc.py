import pandas as pd
import numpy as np
from statsmodels.tsa.stattools import adfuller
from scipy.signal import detrend
from statsmodels.formula.api import ols
import os
import urllib
import tools.naics_matcher

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

    def get_qpc_data(self, year):
        """
        Quarterly survey began 2008; start with 2010 due to  2007-2009
        recession.
        """
        y = str(year)

        if year < 2017:

            excel_ex = '.xls?#'

        else:

            excel_ex = '.xlsx?#'

        qpc_data = pd.DataFrame()

        base_url = 'https://www2.census.gov/programs-surveys/qpc/tables/'

        for q in ['q'+str(n) for n in range(1, 5)]:

            if year >= 2017:

                y_url = '{!s}/{!s}_qtr_table_final_'

            # elif year < 2010:
            #
            #     y_url = \
            #         '{!s}/qpc-quarterly-tables/{!s}_qtr_combined_tables_final_'

            else:

                y_url = '{!s}/qpc-quarterly-tables/{!s}_qtr_table_final_'

            if (year == 2016) & (q == 'q4'):

                url = base_url + y_url.format(y, y) + q + '.xlsx?#'

            else:

                url = base_url + y_url.format(y, y) + q + excel_ex

            #Excel formatting for 2008 is different than all other years.
            #Will need to revise skiprows and usecols.
            try:
                data = pd.read_excel(url, sheet_name=1, skiprows=4,
                                     usecols=range(0, 7), header=0)

            except urllib.error.HTTPError:

                return

            data.drop(data.columns[2], axis=1, inplace=True)

            data.columns = ['NAICS', 'Description', 'Utilization Rate',
                            'UR_Standard Error',
                            'Weekly_op_hours',
                            'Hours_Standard Error']

            data.dropna(inplace=True)

            data['Q'] = q

            data['Year'] = year

            qpc_data = qpc_data.append(data, ignore_index=True)

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

        # if 'qpc_data_allraw.csv' in self._data_path.listdir():

        #     self.qpc_data = pd.read_csv(
        #         os.path.join(self._data_path, 'qpc_data_allraw.csv')
        #         )

        # else:

        #     self.qpc_data = pd.concat(
        #         [get_qpc_data(year) for year in range(2010, 2020)],
        #         axis=0, ignore_index=True
        #         )

        #     self.qpc_data.to_csv('../calculation_data/qpc_data_allraw.csv',
        #                          index=False)

    def test_seasonality(self, qpc_data_naics):
        """
        Test for seasonality between quarters by NAICS using OLS.

        """

        #sort data
        qpc_data_naics.sort_values(by=['Year', 'Q'], ascending=True,
                                   inplace=True)

        # Create annual lag of quarterly weekly operating data (L4)
        qpc_data_naics['L4'] = qpc_data_naics.Weekly_op_hours.shift(4)

        # Test if data are trend stationary.
        # Null hypothesis is that there is a unit root (nonstationary)
        # Returns (test_stat, pvalue, usedlag, nobs, critical_values, icbest,
        # resstore)
        # adf_test = adfuller(
        #     qpc_data_naics['Weekly_op_hours'].values, regression='ct',
        #     )
        #
        #
        # Can't reject null if p-value is > critical value.
        # Remove trend for seasonality testing
        # if adf_test[1] > 0.05:
        #
        #     qpc_data_naics['hours_detrended'] = detrend(
        #         qpc_data_naics['Weekly_op_hours'], type='linear'
        #         )
        #
        #     # Constant is q1 season.
        #     ols_season = ols('hours_detrended ~ C(Q)', data=qpc_data_naics).fit()

        # Regress quarterly dummies and annual lag on weekly operating hours
        ols_season = \
                ols('Weekly_op_hours ~ C(Q)+L4', data=qpc_data_naics).fit()

        ols_final = pd.DataFrame(np.nan, columns=['q1', 'q2', 'q3', 'q4'],
                                 index=[qpc_data_naics.NAICS.unique()[0]])

        if any(ols_season.pvalues < 0.05):

            ols_res = pd.DataFrame(
                np.multiply(ols_season.pvalues < 0.05, ols_season.params),
                )

            ols_res = ols_res[ols_res > 0].T

            ols_res.rename(
                columns={
                    'Intercept': 'q1', 'C(Q)[T.q2]': 'q2',
                    'C(Q)[T.q2]': 'q2',
                    'C(Q)[T.q3]': 'q3',
                    'C(Q)[T.q4]': 'q4'
                    },
                index={0:qpc_data_naics.NAICS.unique()[0]}, inplace=True
                )

            ols_final.update(ols_res)

        ols_final.index.name = 'NAICS'

        return ols_final

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

        op_hour_min = selected_qpc_data.where(
            selected_qpc_data.Weekly_op_hours_low > 0
            ).Weekly_op_hours_low.fillna(np.nan).min()

        # Constrain to positive operating hours.
        # Assume minimum of data set.
        selected_qpc_data.loc[:, 'Weekly_op_hours_low'] = selected_qpc_data.where(
            selected_qpc_data.Weekly_op_hours_low > 0
            ).Weekly_op_hours_low.fillna(op_hour_min)

        # Constrain to <168 weekly operating hours
        selected_qpc_data.loc[:, 'Weekly_op_hours_high'] = selected_qpc_data.where(
            selected_qpc_data.Weekly_op_hours_high < 168
            ).Weekly_op_hours_low.fillna(168)

        return selected_qpc_data

    def calc_quarterly_avgs(self, qpc_data, qpc_seasonality):
        """
        Calculate average weekly operating hours by NAICS from census QPC data.
        Accounts for quarterly seasonality results: NAICS without seasonality
        are average across all quarters in date range.
        """

        qpc_data = self.calc_hours_CI(qpc_data)

        annual = qpc_seasonality.reset_index().melt(
            id_vars='NAICS', var_name='Q'
            )

        annual = annual[(annual.NAICS != '31-33') & (annual.value.isnull())]

        # annual = qpc_seasonality.isnull().apply(lambda x: all(x), axis=1)
        #
        # annual = annual[annual==True]
        #
        # annual.name = 'annual'

        # Calculate annual average using only quarters with no seasonality
        ann_avg = pd.merge(
            qpc_data[qpc_data.NAICS !='31-33'], annual, on=['NAICS', 'Q'],
            how='inner'
            )

        ann_avg = pd.DataFrame(
            ann_avg.set_index('NAICS')[
            ['Weekly_op_hours', 'Weekly_op_hours_low', 'Weekly_op_hours_high']
            ].mean(level=0)
            )

            # qpc_data.set_index('NAICS').join(annual, how='inner')

        # ann_avg.index.name = 'NAICS'

        # ann_avg = ann_avg.reset_index().groupby('NAICS').Weekly_op_hours.mean()

        ann_avg = ann_avg.reindex(index=np.repeat(ann_avg.index, 4))

        ann_avg['Q'] = np.tile(['q1', 'q2', 'q3', 'q4'],
                               int(len(ann_avg)/4))

        ann_avg = ann_avg.reset_index().pivot_table(
            index='NAICS', values=['Weekly_op_hours', 'Weekly_op_hours_low',
                                   'Weekly_op_hours_high'], aggfunc='mean',
            columns='Q'
            )

        seasonal = qpc_seasonality.dropna(thresh=1).reset_index().melt(
            id_vars='NAICS', var_name='Q', value_name='seasonality'
            )

        seasonal['seasonality'] = seasonal.seasonality.notnull()

        # seasonal.rename(columns={'index': 'NAICS'}, inplace=True)

        # seasonal.index.name = 'NAICS'

        # seasonal = pd.merge(
        #     seasonal, qpc_data, on=['NAICS', 'Q'], how='inner'
        #     )

        seasonal = pd.merge(
            seasonal[seasonal.seasonality==True], qpc_data, on=['NAICS', 'Q'],
            how='inner'
            )

        # seasonal_avg = seasonal.groupby(
        #     ['NAICS', 'seasonality']
        #     ).Weekly_op_hours.mean()

        seasonal_avg = seasonal.groupby(['NAICS', 'Q'])[
            ['Weekly_op_hours', 'Weekly_op_hours_low', 'Weekly_op_hours_high']
            ].mean()

        # seasonal_avg = seasonal_avg.reindex(index=seasonal.groupby(
        #     ['NAICS', 'seasonality', 'Q']
        #     ).Weekly_op_hours.mean().index)

        # seasonal_avg.reset_index('seasonality', drop=True, inplace=True)

        seasonal_avg = seasonal_avg.reset_index().pivot_table(
            index='NAICS', values=['Weekly_op_hours', 'Weekly_op_hours_low',
                                   'Weekly_op_hours_high'], aggfunc='mean',
            columns='Q'
            )

        # all_avg = pd.concat([seasonal_avg, ann_avg], axis=0, sort=True)

        ann_avg.update(seasonal_avg)

        return ann_avg

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

        naics_6d = tools.naics_matcher.naics_matcher(
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

        return qpc_data

    def main(self, year):
        qpc_data = self.get_qpc_data(year)
        qpc_data = self.calc_hours_CI(qpc_data)
        qpc_data = self.format_foundational(qpc_data)

        return qpc_data

if __name__ == '__main__':
    qpc = QPC()
    qpc_data = qpc.get_qpc_data(2017)
    qpc_data = qpc.calc_hours_CI(qpc_data)