import sqlite3
import pandas as pd
from datahandler import DataHandler


class Database:

    def __init__(self, dbname):
        self.db = dbname
        self.deaths_table = 'deaths_total'
        self.deaths_change_python_table = 'deaths_change_python'

    def create_connection(self):
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

    def execute_query(self, sql):
        """
        Executes an SQL query and returns the result
        :param sql: SQL query
        :return: Query result
        """
        with self.create_connection() as con:
            cursor = con.cursor()
            cursor.execute(sql)
            result = cursor.fetchall()
        return result

    def create_total_deaths_table(self):
        """
        Creates a table named 'total_deaths' in the database with country, date and deaths as columns. The
        composite primary keys are country and date

        :return:
        """
        with self.create_connection() as con:
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

        with self.create_connection() as con:
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

    def upsert_to_table(self, data_df, table_name):
        """
        Inserts data into a table. If the data row exists corresponding values are updated
        :param data_df: Data frame
        :param table_name: Name of the table
        :return: The number of changed rows
        """
        sql = 'INSERT INTO ' + table_name + '(' + ','.join(data_df.columns.values) + ') ' \
                                                                                     'VALUES (?, ?, ?) ' \
                                                                                     'ON CONFLICT (country, date) ' \
                                                                                     'DO UPDATE ' \
                                                                                     'SET ' + data_df.columns.values[
                  2] + '=?;'
        data_df = data_df.copy()
        data_df.loc[:, 'date'] = data_df.loc[:, 'date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        data_df.iloc[:, 2] = data_df.iloc[:, 2].astype(str)

        # Copy the deaths/ deaths_change column to a temp column as the fourth parameter
        data_df.loc[:, 'temp'] = data_df.iloc[:, 2]
        param_list = [tuple(x) for x in data_df.values]
        with self.create_connection() as con:
            cursor = con.cursor()
            resp = cursor.executemany(sql, param_list)

        return resp.rowcount

    def _insert_deaths_data(self, new_data_df, table_name):
        """
        Inserts deaths data to COVID19 deaths data table, denoted by table_name. If the new data contain
        retrospectively modified rows, such rows are updated in the table

        :param new_data_df: Data frame with new data
        :param table_name: Name of the table to insert data
        :return: The number of updated rows
        """

        changed_rows = -1

        row_count = self.execute_query('SELECT COUNT(*) FROM ' + table_name + ';')[0][0]

        if row_count == 0:
            '''
            The table is empty, insert all the data in the data frame
            '''
            with self.create_connection() as con:
                new_data_df.to_sql(con=con, name=table_name, if_exists='append', index=False)

            changed_rows = len(new_data_df)

        else:
            '''
            If the table is not empty append only the new data rows because re-writing all the 
            data is too expensive
            '''
            # Read the current data from table as a data frame
            with self.create_connection() as con:
                curr_data = pd.read_sql_query('SELECT * FROM ' + table_name + ';', con=con)

            dh = DataHandler()
            # Filter changed or newly added country and date combinations
            modified_rows = dh.get_changed_rows(new_data_df, curr_data)
            # Update the database
            changed_rows = self.upsert_to_table(new_data_df.iloc[modified_rows, ], table_name)

        return changed_rows
