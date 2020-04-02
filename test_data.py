import unittest
import pandas as pd

from data import get_daily_deaths_by_country


class TestData(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        data = {
            'Province/State': ['Victoria', 'New South Wales', '', 'Texas', 'Virginia', 'England'],
            'Country/Region': ['Australia', 'Australia', 'Sri Lanka', 'US', 'US', 'UK'],
            'Lat': [2.11, 3.1,6.1, 9.3, 9.6, 8.5],
            '1/1/2020': [0, 0, 0, 0, 0, 0],
            '1/2/2020': [0, 0, 0, 0, 0, 0],
            '1/3/2020': [0, 0, 1, 0, 2, 0],
            '1/4/2020': [0, 0, 1, 3, 2, 1],
            '1/10/2020': [1, 1, 1, 5, 2, 2],
            '1/13/2020': [1, 2, 2, 7, 3, 3],
            '1/15/2020': [3, 5, 2, 8, 5, 4],
            '2/1/2020': [4, 6, 3, 10, 7, 6],
            '2/6/2020': [6, 8, 3, 17, 12, 9],
            '2/13/2020': [7, 10, 3, 20, 16, 11]}
        cls.csv_df = pd.DataFrame(data)

    def test_df_columns(self):
        df = get_daily_deaths_by_country(self.csv_df)
        cols = df.columns.values
        self.assertEqual(3, len(cols),"Transformed data frame expected to have 3 columns")


