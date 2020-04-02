import unittest
import pandas as pd
import pandas.api.types as ptypes

from data import get_daily_deaths_by_country


class TestData(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.TOTAL_DEATHS_DF_COLS = ['country', 'date', 'deaths']
        cls.DEATHS_CHANGE_DF_COLS = ['country', 'date', 'deaths_change']
        data = {
            'Province/State': ['Victoria', 'New South Wales', '', 'Texas', 'Virginia', 'England'],
            'Country/Region': ['Australia', 'Australia', 'Sri Lanka', 'US', 'US', 'UK'],
            'Lat': [2.11, 3.1, 6.1, 9.3, 9.6, 8.5],
            '1/1/2020': [0, 0, 0, 0, 0, 0],
            '1/4/2020': [0, 0, 1, 3, 2, 1],
            '1/15/2020': [3, 1, 0, 5, 5, 4],
            '2/1/2020': [4, 1, 0, 10, 7, 6],
            '2/13/2020': [5, 1, 3, 9, 0, 2]}

        cls.csv_df = pd.DataFrame(data)
        cls.total_deaths_df = get_daily_deaths_by_country(cls.csv_df)

    def test_total_deaths_df_columns(self):
        cols = list(self.total_deaths_df.columns.values)
        self.assertSequenceEqual(cols, self.TOTAL_DEATHS_DF_COLS, 'Transformed data frame must only have ' + str(
            self.TOTAL_DEATHS_DF_COLS) + ' as columns')

    def test_total_deaths_df_rowcount(self):
        dates_count = 5

        country_count = 4
        expected_rows = dates_count * country_count

        self.assertEqual(expected_rows, len(self.total_deaths_df), 'The number of rows in total deaths data frame need '
                                                                   'to be "date_count * country_count')

    def test_total_deaths_df_country_column_type(self):
        self.assertTrue(ptypes.is_string_dtype(self.total_deaths_df['country']))

    def test_total_deaths_df_date_column_type(self):
        self.assertTrue(ptypes.is_datetime64_dtype(self.total_deaths_df['date']))

    def test_total_deaths_df_deaths_column_type(self):
        self.assertTrue(ptypes.is_int64_dtype(self.total_deaths_df['deaths']))

    def test_deaths_sum_by_country_date_pair(self):
        exp_total_deaths = [0, 0, 0, 0, 0, 1, 1, 5, 4, 0, 4, 10, 5, 0, 6, 17, 6, 3, 2,
                            9]  # Sequentially for each country and date pair
        self.assertListEqual(exp_total_deaths, list(self.total_deaths_df['deaths']),
                             'Total deaths by country and date must match')

    def test_total_deaths_sum(self):
        self.assertEqual(73, self.total_deaths_df['deaths'].sum(),
                         'The number of total deaths in all countries needs to match')

    def test_multiple_country_columns(self):
        new_df = self.csv_df.rename(columns={'Province/State': 'Country_state'})
        self.assertRaises(ValueError, get_daily_deaths_by_country, new_df)
