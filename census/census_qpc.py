import pandas as pd
import numpy as np
from statsmodels.tsa.stattools import adfuller
from scipy.signal import detrend
from statsmodels.formula.api import ols
import pathlib
import os
import urllib
import logging


class QPC:

    def __init__(self):

        self._base_url = 'https://www2.census.gov/programs-surveys/qpc/tables/'
        logging.basicConfig(level=logging.INFO)

    @staticmethod
    def force_format(naics):
        """
        Some NAICS aren't converting from string to int using .astype
        e.g., '31519'

        Parameters
        ----------
        naics : str
            String-formatted NAICS code

        Returns
        -------
        naics : int
            Formatted NAICS code.
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

        Parameters
        ----------
        df : pandas.DataFRmae
            NAICS codes from QPC survey results.

        Returns
        -------
        df : pandas.DataFrame
            Formatted NAICS codes.

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

            new_rows['NAICS'] = np.repeat(all_naics,
                                            len(new_rows)/len(all_naics))

            # Delete original data
            df = df[df.NAICS != naics]

            df = df.append(new_rows, ignore_index=True)

        return df

    def get_qpc_data(self, year):
        """
        Quarterly survey began 2008; start with 2010 due to  2007-2009
        recession.

        Parameters
        ----------
        year : int
            Year of surey data to download.

        Returns
        -------


        """
        y = str(year)

        if year < 2017:
            excel_ex = '.xls?#'

        else:
            excel_ex = '.xlsx?#'

        qpc_data = pd.DataFrame()

        for q in ['q'+str(n) for n in range(1, 5)]:

            if year >= 2017:
                y_url = '{!s}/{!s}_qtr_table_final_'

            else:
                y_url = '{!s}/qpc-quarterly-tables/{!s}_qtr_table_final_'

            if (year == 2016) & (q == 'q4'):
                url = f'{self._base_url}{y_url.format(y, y)}{q}.xlsx?#'

            else:
                url = f'{self._base_url}{y_url.format(y, y)}{q}{excel_ex}'

            # Excel formatting for 2008 is different than all other years.
            # Will need to revise skiprows and usecols.
            try:
                data = pd.read_excel(url, sheet_name=1, skiprows=4,
                                     usecols=[0, 1, 2, 3, 4, 5, 6],
                                     header=0)

            except urllib.error.HTTPError:
                return

            logging.info(f'data : {data}')

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

        # Interpolate for single value == 'S'
        qpc_data.replace({'S':np.nan,'Z':np.nan}, inplace=True)

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

    def test_seasonality(self, qpc_data_naics):
        """
        Test for seasonality between quarters by NAICS using OLS.

        Parameters
        ----------
        qpc_data_naics : pandas.DataFrame

        Returns
        -------
        ols_final : pandas.DataFrame
            Results of seasonality testing.
        """

        # sort data
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
                index={0: qpc_data_naics.NAICS.unique()[0]}, inplace=True
                )

            ols_final.update(ols_res)

        else:
            pass

        ols_final.index.name = 'NAICS'

        return ols_final

    def calc_hours_CI(self, qpc_data, CI=95):
        """
        Calculate the confidence interval (CI) for average
        weekly hours based on reported standard errros.

        Parameters
        ----------
        qpc_data : pandas.DataFrame

        CI : int; 90, 95, or 99
            Confidence interval to calculate. 

        Returns
        -------
        sepected_qpc_data : pandas.DataFrame
            Initial DataFrame updated with confidence intervals for
            weekly operating hours
        """

        # t distribution values for CI probabilities
        ci_dict = {90: 1.65, 95: 1.96, 99: 2.58}

        qpc_data_ci = pd.DataFrame(qpc_data.set_index(
            ['NAICS', 'Year', 'Q']
            )['Hours_Standard Error']).astype(float)*ci_dict[CI]

        selected_qpc_data = pd.DataFrame(
            qpc_data.set_index(['NAICS', 'Year', 'Q'])
            )

        selected_qpc_data['Weekly_op_hours_high'] = \
            selected_qpc_data.Weekly_op_hours.add(
                qpc_data_ci['Hours_Standard Error']
                )

        selected_qpc_data['Weekly_op_hours_low'] = \
            selected_qpc_data.Weekly_op_hours.subtract(
                qpc_data_ci['Hours_Standard Error']
                )

        selected_qpc_data = selected_qpc_data.where(
            selected_qpc_data.Weekly_op_hours_low > 0
            ).fillna(0)

        selected_qpc_data.reset_index(inplace=True)

        return selected_qpc_data

    def calc_quarterly_avgs(self, qpc_data, qpc_seasonality=None):
        """
        Calculate average weekly operating hours by NAICS from census QPC data.
        Accounts for quarterly seasonality results: NAICS without seasonality
        are average across all quarters in date range.

        Parameters
        ----------
        qpc_data : pandas.DataFrame
            QPC data retrieved from downloaded survey excel files.

        qpc_seasonality : pandas.DataFrame; default is None.
            If seasonality testing results are provided, method 
            will adjust calculation of quarterly averages.


        Returns
        -------
        ann_average : pandas.DataFrame

        """

        if qpc_seasonality:
    
            annual = qpc_seasonality.reset_index().melt(
                id_vars='NAICS', var_name='Q'
                )

            annual = annual[(annual.NAICS != '31-33') & (annual.value.isnull())]

            # Calculate annual average using only quarters with no seasonality
            ann_avg = pd.merge(
                qpc_data[qpc_data.NAICS != '31-33'], annual, on=['NAICS', 'Q'],
                how='inner'
                )

        else:
            ann_avg = pd.DataFrame(qpc_data[qpc_data.NAICS != '31-33'])

        ann_avg = pd.DataFrame(ann_avg.set_index('NAICS')[
            ['Weekly_op_hours', 'Weekly_op_hours_low', 'Weekly_op_hours_high']
            ].mean(level=0))

        ann_avg = ann_avg.reindex(index=np.repeat(ann_avg.index, 4))

        ann_avg['Q'] = np.tile(['q1', 'q2', 'q3', 'q4'],
                               int(len(ann_avg)/4))

        ann_avg = ann_avg.reset_index().pivot_table(
            index='NAICS', values=['Weekly_op_hours', 'Weekly_op_hours_low',
                                   'Weekly_op_hours_high'], aggfunc='mean',
            columns='Q'
            )

        if qpc_seasonality: 
            seasonal = qpc_seasonality.dropna(thresh=1).reset_index().melt(
                id_vars='NAICS', var_name='Q', value_name='seasonality'
                )

            seasonal['seasonality'] = seasonal.seasonality.notnull()

            seasonal = pd.merge(
                seasonal[seasonal.seasonality==True], qpc_data, on=['NAICS', 'Q'],
                how='inner'
                )

            seasonal_avg = seasonal.groupby(['NAICS', 'Q'])[
                ['Weekly_op_hours', 'Weekly_op_hours_low',
                 'Weekly_op_hours_high']
                ].mean()

            seasonal_avg = seasonal_avg.reset_index().pivot_table(
                index='NAICS',
                values=['Weekly_op_hours', 'Weekly_op_hours_low',
                        'Weekly_op_hours_high'], aggfunc='mean',
                columns='Q'
                )

            ann_avg.update(seasonal_avg)

        else:
            pass

        return ann_avg

    def main(self):

        year = 2018

        qpc_data = self.get_qpc_data(year)
        qpc_data = self.calc_hours_CI(qpc_data)
        qpc_data = self.calc_quarterly_avgs(qpc_data)

        fpath = os.path.abspath(__file__)
        pfp = pathlib.PurePath(fpath)

        qpc_data.to_csv(
            pathlib.PurePath.joinpath(
                pfp.parents[1],
                f'data/QPC/qpc_results_{year}.csv'
                )
            )


if __name__ == '__main__':
    QPC().main()