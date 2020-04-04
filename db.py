import sqlite3
import re
import pandas as pd


class Database:

    def __init__(self, dbname):
        self.db = dbname

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

    def add_new_death_data(self, new_data_df, total_deaths_table):
        """
        Inserts new deaths data to the specified total_deaths_table

        :param new_data_df: Data frame with new data
        :param total_deaths_table: Name of the table to insert data
        :return: Total number of rows in table and the number of newly updated rows
        """

        total_rows = -1
        updated_rows = -1
        with self._create_connection() as con:
            cursor = con.cursor()
            sql = "SELECT COUNT(name) FROM sqlite_master WHERE type ='table' AND name ='" + total_deaths_table + "';"
            table_count = cursor.execute(sql).fetchone()[0]

        if table_count == 1:
            '''
            If the table already exists append only the new data because re-writing all the 
            data is too expensive
            '''
            # Read the current data from table as a data frame
            with self._create_connection() as con:
                cursor = con.cursor()
                cursor.execute('SELECT * FROM ' + total_deaths_table + ';')
                curr_data = pd.DataFrame(cursor.fetchall(), columns=['country', 'date', 'deaths'])

            # Filter new country and date combinations
            curr_data['date'] = pd.to_datetime(curr_data['date'], infer_datetime_format=True)
            curr_data = curr_data.set_index(['country', 'date'])
            new_data_df = new_data_df.set_index(['country', 'date'])
            diff = new_data_df[~new_data_df.index.isin(curr_data.index)].dropna()

            # Update table
            with self._create_connection() as con:
                diff.to_sql(con=con, name=total_deaths_table, if_exists='append', index=False)

            total_rows = len(curr_data) +  len(diff)
            updated_rows = len(diff)
        else:
            '''
            If there is no table create it and insert data
            '''
            with self._create_connection() as con:
                cursor = con.cursor()
                cursor.execute('CREATE TABLE IF NOT EXISTS ' + total_deaths_table + ' ( \
                               country TEXT, \
                               date DATE, \
                               deaths INT, \
                               PRIMARY KEY (country, date));'
                               )
                con.commit()
                new_data_df.to_sql(con=con, name=total_deaths_table, if_exists='append', index=False)

            total_rows = len(new_data_df)
            updated_rows = len(new_data_df)
        return total_rows, updated_rows





