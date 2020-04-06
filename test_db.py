import unittest
import pandas as pd

from db import Database
import testutils
from datahandler import DataHandler
import os


class TestDatabase(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        db_name = 'Testing.db'
        if os._exists(db_name):
            os.remove(db_name)
        cls.db = Database(db_name)
        cls.csv_df = testutils.get_dummy_data()

        cls.total_deaths_table = 'deaths_total'
        cls.death_change_python_table = 'deaths_change_python'

    def setUp(self) -> None:
        # Start with a clean slate
        self.total_deaths_df = DataHandler().get_total_deaths_per_country_and_day(self.csv_df)

        with self.db.create_connection() as con:
            cursor = con.cursor()
            cursor.execute('DROP TABLE IF EXISTS ' + self.total_deaths_table + ';')
            cursor.execute('DROP TABLE IF EXISTS ' + self.death_change_python_table + ';')
            con.commit()

    def test_create_connection(self):
        con = self.db.create_connection()
        self.assertIsNotNone(con, "Database connection")

    def test_no_data_duplicates(self):
        self.db.create_total_deaths_table()

        self.db._insert_deaths_data(self.total_deaths_df, self.total_deaths_table)
        with self.db.create_connection() as con:
            cursor = con.cursor()
            cursor.execute('SELECT COUNT(*) FROM ' + self.total_deaths_table + ';')
            row_count = cursor.fetchone()[0]

        # Try to add the same data again.
        self.db._insert_deaths_data(self.total_deaths_df, self.total_deaths_table)

        with self.db.create_connection() as con:
            cursor.execute('SELECT COUNT(*) FROM ' + self.total_deaths_table + ';')
            new_row_count = cursor.fetchone()[0]

        self.assertEqual(row_count, new_row_count)

    def test_function_output_and_table_rows(self):
        self.db.create_total_deaths_table()
        with self.db.create_connection() as con:
            cursor = con.cursor()
            changed_rows = self.db._insert_deaths_data(self.total_deaths_df, self.total_deaths_table)
            cursor.execute('SELECT COUNT(*) FROM ' + self.total_deaths_table + ';')
            row_count = cursor.fetchone()[0]
        self.assertEqual(changed_rows, row_count,
                         "Number of rows in table needs to match add_new_death_data function's changed rows count")

    def test_adding_new_rows_to_table(self):
        self.db.create_total_deaths_table()

        # Create a new df by removing some data
        starting_df = self.total_deaths_df[self.total_deaths_df['date'] != pd.to_datetime('2/13/2020')]
        self.db._insert_deaths_data(starting_df, self.total_deaths_table)  # Add it to the table. This is the
        # starting state.

        # Now we mimic a new update total_deaths_df Df has extra rows with i.e. latest stats
        changed_rows = self.db._insert_deaths_data(self.total_deaths_df, self.total_deaths_table)

        table_len = self.db.execute_query('SELECT COUNT(*) FROM ' + self.total_deaths_table + ';')[0][0]

        self.assertEqual(table_len, len(self.total_deaths_df),
                         "Only missing data should be added and existing data should not duplicate. Expected row count " +
                         str(len(self.total_deaths_df)) + " actual row count " + str(table_len))

    def test_updating_table_with_modified_rows(self):
        self.db.create_total_deaths_table()

        # Create a new df by removing some data
        starting_df = self.total_deaths_df[self.total_deaths_df['date'] != pd.to_datetime('2/13/2020')]
        self.db.insert_to_deaths_total_table(starting_df)  # Add it to the table. This is the
        # starting state.

        # Now we mimic a new total_deaths_df Df that has extra rows  i.e. latest stats and few retrospectively
        # modified rows
        new_df = self.total_deaths_df.copy()
        new_df.loc[0, 'deaths'] = new_df.loc[0, 'deaths'] + 1
        new_df.loc[7, 'deaths'] = new_df.loc[7, 'deaths'] - 2

        changed_rows = self.db.insert_to_deaths_total_table(new_df)

        updates = self.total_deaths_df[self.total_deaths_df['date'] == pd.to_datetime('2/13/2020')].append(
            new_df.iloc[[0, 7], ])

        self.assertIs(changed_rows, len(updates),
                      'Number of rows changed in the ' + self.total_deaths_table + ' must be the same as updated data \
                      rows')

        total_rows = self.db.execute_query('SELECT COUNT(*) FROM ' + self.total_deaths_table)[0][0]
        self.assertIs(total_rows, len(self.total_deaths_df),
                      'The number of total rows in ' + self.total_deaths_table + ' must be the same as \
                                      total rows in data from the repo')

    def test_updating_table_to_correct_values(self):
        # Tests whether db table gets updated with correct values when doing subsequent runs
        self.db.create_total_deaths_table()

        # Create a new df by removing some data
        starting_df = self.total_deaths_df[self.total_deaths_df['date'] != pd.to_datetime('2/13/2020')]
        self.db.insert_to_deaths_total_table(starting_df)  # Add it to the table. This is the starting state.

        # Now we mimic a new update total_deaths_df Df has extra rows with i.e. latest stats and few retrospectively
        # modified rows
        new_df = self.total_deaths_df.copy()
        new_df.loc[1, 'deaths'] = new_df.loc[1, 'deaths'] + 1
        new_df.loc[6, 'deaths'] = new_df.loc[6, 'deaths'] - 2

        changed_rows = self.db.insert_to_deaths_total_table(new_df)

        updates = self.total_deaths_df[self.total_deaths_df['date'] == pd.to_datetime('2/13/2020')].append(
            new_df.iloc[[1, 6],])
        updates = updates.sort_values(['date', 'country'])
        updates.loc[:, 'date'] = updates.loc[:, 'date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        updates.reset_index(drop=True, inplace=True)

        with self.db.create_connection() as con:
            result = pd.read_sql(
                'SELECT * FROM ' + self.total_deaths_table + ' \
                WHERE \
                    date="' + pd.to_datetime('2020-02-13').strftime('%Y-%m-%d %H:%M:%S') + '"\
                     OR (\
                        country="' + new_df.loc[1, 'country'] + '" \
                        AND date="' + new_df.loc[1, 'date'].strftime('%Y-%m-%d %H:%M:%S') + '"\
                        )\
                      OR ( \
                        country="' + new_df.loc[6, 'country'] + '" \
                        AND date="' + new_df.loc[6, 'date'].strftime('%Y-%m-%d %H:%M:%S') + '" \
                        )\
                ORDER BY date, country;',
                con=con)

        self.assertIs(len(result), len(updates), "The number of updated rows in db table needs to match the data rows")
        self.assertTrue(updates.equals(result),
                        " Updated data in the database table must match the changed rows in repo")

    def test_daily_change_sql_query(self):
        # Create and populate deaths_total table
        self.db.create_total_deaths_table()
        self.db._insert_deaths_data(self.total_deaths_df, self.total_deaths_table)
        self.db.execute_query("DROP TABLE IF EXISTS deaths_change_sql;")

        sql = "CREATE TABLE deaths_change_sql AS \
                        SELECT country, date, \
                        deaths - LAG(deaths,1,0) OVER (PARTITION BY country ORDER BY date) AS deaths_change \
                        FROM deaths_total;"

        self.db.execute_query(sql)

        with self.db.create_connection() as con:
            change_df = pd.read_sql('SELECT * FROM deaths_change_sql', con=con)

        expected_df = testutils.get_dummy_change_data()  # Expected deaths change data
        expected_df.loc[:, 'date'] = expected_df.loc[:, 'date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        self.assertTrue(change_df.equals(expected_df))


if __name__ == '__main__':
    unittest.main()
