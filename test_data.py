import unittest
import pandas as pd
import pandas.api.types as ptypes

from datahandler import DataHandler
import testutils


class TestData(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.TOTAL_DEATHS_DF_COLS = ['country', 'date', 'deaths']
        cls.DEATHS_CHANGE_DF_COLS = ['country', 'date', 'deaths_change']
        cls.csv_df = testutils.get_dummy_data()
        cls.data = DataHandler()
        cls.total_deaths_df = cls.data.get_total_deaths_per_country_and_day(cls.csv_df)

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
        exp_total_deaths = [0, 0, 0, 0, 0, 1, 1, 5, 4, 1, 4, 10, 5, 1, 6, 17, 8, 3, 8,
                            19]  # Sequentially for each country and date pair
        self.assertListEqual(exp_total_deaths, list(self.total_deaths_df['deaths']),
                             'Total deaths by country and date must match')

    def test_total_deaths_sum(self):
        self.assertEqual(93, self.total_deaths_df['deaths'].sum(),
                         'The number of total deaths in all countries needs to match')

    def test_multiple_country_columns(self):
        new_df = self.csv_df.rename(columns={'Province/State': 'Country_state'})
        self.assertRaises(ValueError, self.data.get_total_deaths_per_country_and_day, new_df)

    def test_filtering_new_data_from_old(self):
        # Represents data currently in the system
        curr_data_df = self.total_deaths_df[self.total_deaths_df['date'] != pd.to_datetime('2/13/2020')].copy()

        diff_rows_ids = self.data.get_changed_rows(self.total_deaths_df, curr_data_df)

        calculated_difference_df = self.total_deaths_df.iloc[diff_rows_ids,]
        calculated_difference_df.reset_index(drop=True, inplace=True)

        actual_difference_df = self.total_deaths_df[self.total_deaths_df['date'] == pd.to_datetime('2/13/2020')].copy()
        actual_difference_df.reset_index(drop=True, inplace=True)

        self.assertTrue(calculated_difference_df.equals(actual_difference_df),
                        "Calculated different data rows between the new data and old data must match the actually different rows")

    def test_filter_retrospectively_modified_data_and_new_data(self):
        # Represents data currently in the system
        curr_data_df = self.total_deaths_df[self.total_deaths_df['date'] != pd.to_datetime('2/13/2020')].copy()

        # New data with retrospective updates
        retro_updated_new_df = self.total_deaths_df.copy()
        retro_updated_new_df.loc[0, 'deaths'] = retro_updated_new_df.loc[0, 'deaths'] + 2
        retro_updated_new_df.loc[8, 'deaths'] = retro_updated_new_df.loc[4, 'deaths'] + 5

        changed_row_ids = [0, 8] + list(
            retro_updated_new_df[retro_updated_new_df['date'] == pd.to_datetime('2/13/2020')].index)

        updated_rows_list = list(self.data.get_changed_rows(retro_updated_new_df, curr_data_df))

        self.assertEqual(changed_row_ids, updated_rows_list, "The output of get_modified_data_rows must match the changed_row_ids ")

    def test_daily_change_calculation(self):
        dummy_data = testutils.get_dummy_data()
        d = DataHandler()
        deaths = d.get_total_deaths_per_country_and_day(dummy_data)
        expected_df = testutils.get_dummy_change_data()

        actual = d.get_daily_change_of_deaths(deaths)
        actual.reset_index(drop=True, inplace=True)

        self.assertTrue(expected_df.equals(actual),
                        "Function generated daily change in deaths must be indentical to the expected")

        deaths.loc[0, 'deaths'] = 1
        actual = d.get_daily_change_of_deaths(deaths)
        self.assertEqual(actual.loc[0, 'deaths_change'], 1, "The first day's change in deaths should be 1")

        deaths.loc[0, 'deaths'] = 4
        actual = d.get_daily_change_of_deaths(deaths)
        self.assertEqual(actual.loc[0, 'deaths_change'], 4, "The first day's change in deaths should be 4")

