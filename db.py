import sqlite3
import pandas as pd
from datahandler import DataHandler


class Database:

    def __init__(self, dbname):
        self.db = dbname
        self.deaths_table = 'deaths_total'
        self.deaths_change_python_table = 'deaths_change_python'

    def _create_connection(self):
        """
        Creates a database connection to a database

        :return: Connection to the database
        """
        con = None
        try:
            con = sqlite3.connect(self.db)
        except sqlite3.Error as e:
            print(e)
        return con

    def create_total_deaths_table(self):
        """
        Creates a table named 'total_deaths' in the database with country, date and deaths as columns. The
        composite primary keys are country and date

        :return:
        """
        with self._create_connection() as con:
            cursor = con.cursor()
            cursor.execute('CREATE TABLE IF NOT EXISTS deaths_total ( \
                               country TEXT, \
                               date DATE, \
                               deaths INT, \
                               PRIMARY KEY (country, date));'
                           )
            con.commit()

    def create_deaths_change_python_table(self):
        """
        Creates a table named 'deaths_change_python' in the database with country, date and deaths_change as columns.
        The composite primary keys are country and date

        :return:
        """

        with self._create_connection() as con:
            cursor = con.cursor()
            cursor.execute('CREATE TABLE IF NOT EXISTS deaths_change_python ( \
                               country TEXT, \
                               date DATE, \
                               deaths_change INT, \
                               PRIMARY KEY (country, date));'
                           )
            con.commit()

    def insert_to_deaths_total_table(self, total_deaths_df):
        return self._insert_deaths_data(total_deaths_df, self.deaths_table)

    def insert_to_deaths_change_python_table(self, deaths_change_df):
        return self._insert_deaths_data(deaths_change_df, self.deaths_change_python_table)

    def _insert_deaths_data(self, new_data_df, table_name):
        """
        Inserts deaths data to COVID19 deaths data table, denoted by table_name.

        :param new_data_df: Data frame with new data
        :param table_name: Name of the table to insert data
        :return: Total number of rows in table and the number of newly updated rows
        """

        total_rows = -1
        updated_rows = -1
        with self._create_connection() as con:
            cursor = con.cursor()
            sql = "SELECT COUNT(*) FROM'" + table_name + "';"
            row_count = cursor.execute(sql).fetchone()[0]

        if row_count == 0:
            '''
            The table is empty, insert all the data in the data frame
            '''
            with self._create_connection() as con:
                new_data_df.to_sql(con=con, name=table_name, if_exists='replace', index=False)

            total_rows = len(new_data_df)
            updated_rows = len(new_data_df)

        else:
            '''
                If the table is not empty append only the new data rows because re-writing all the 
                data is too expensive
                '''
            # Read the current data from table as a data frame
            with self._create_connection() as con:
                curr_data = pd.read_sql_query('SELECT * FROM ' + table_name + ';', con=con)

            # Filter new country and date combinations
            diff = DataHandler().filter_new_country_date_combinations(new_data_df, curr_data)

            # Update table
            with self._create_connection() as con:
                diff.to_sql(con=con, name=table_name, if_exists='append', index=False)

            total_rows = len(curr_data) + len(diff)
            updated_rows = len(diff)
        return total_rows, updated_rows
