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
        cls.total_deaths_df = DataHandler().get_total_deaths_per_country_and_day(cls.csv_df)
        cls.total_deaths_table = 'deaths_total'

    def setUp(self) -> None:
        # Start with a clean slate
        with self.db.create_connection() as con:
            cursor = con.cursor()
            cursor.execute('DROP TABLE IF EXISTS ' + self.total_deaths_table + ';')
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
            total_rows, new_rows = self.db._insert_deaths_data(self.total_deaths_df, self.total_deaths_table)
            cursor.execute('SELECT COUNT(*) FROM ' + self.total_deaths_table + ';')
            row_count = cursor.fetchone()[0]
        self.assertEqual(total_rows, row_count, "Number of rows in table needs to match add_new_death_data function "
                                                "total rows output")

    def test_adding_new_rows_to_table(self):
        self.db.create_total_deaths_table()

        # Create a new df by removing some data
        starting_df = self.total_deaths_df[self.total_deaths_df['date'] != pd.to_datetime('2/13/2020')]
        self.db._insert_deaths_data(starting_df, self.total_deaths_table)  # Add it to the table. This is the
        # starting state.

        # Now we mimic a new update
        new_data_df = self.total_deaths_df.copy()  # Df has extra rows with latest stats

        self.db._insert_deaths_data(new_data_df, self.total_deaths_table)

        with self.db.create_connection() as con:
            cursor = con.cursor()
            cursor.execute('SELECT COUNT(*) FROM ' + self.total_deaths_table + ';')
            new_row_count = cursor.fetchone()[0]

        self.assertEqual(new_row_count, len(new_data_df), "Only missing data should be added. Expected row count " +
                         str(len(new_data_df)) + " actual row count " + str(new_row_count))

    def test_daily_change_sql_query(self):
        self.db.create_total_deaths_table()
        self.db._insert_deaths_data(self.total_deaths_df, self.total_deaths_table)

        self.db.execute_query("DROP TABLE IF EXISTS deaths_change_sql;")

        sql = "CREATE TABLE deaths_change_sql AS \
                        SELECT country, date, \
                        deaths - LAG(deaths) OVER (PARTITION BY country ORDER BY date) AS deaths_change \
                        FROM deaths_total;"

        self.db.execute_query(sql)

        with self.db.create_connection() as con:
            change_df = pd.read_sql('SELECT * FROM deaths_change_sql', con=con)

        expected_df = testutils.get_dummy_change_data()
        expected_df['date'] = expected_df['date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        self.assertTrue(change_df.equals(expected_df))


if __name__ == '__main__':
    unittest.main()
