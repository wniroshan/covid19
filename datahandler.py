import pandas as pd
import re


class DataHandler:

    def get_total_deaths_per_country_and_day(self, csv_as_df):
        """
        Converts the horizontally growing csv table to a vertically growing RDBMS friendly table with one 'date' column

        :param csv_as_df: DataFrame loaded from the csv file
        :return: DataFrame with one row per each Country and Date.
        """

        csv_cols = csv_as_df.columns.values

        # Regex selects the column names that start with the term 'Country'
        country_col = [col for col in csv_cols if re.match('[cC]ountry', col)]
        if len(country_col) != 1:
            raise ValueError(
                "Cannot determine country column. Found " + str(len(country_col)) + " with term 'Country'." + str(
                    country_col))

        # Regex selects the column names that look like a date
        date_cols = [col for col in csv_cols if re.match('(?:[0-9]{1,2}/){1,2}[0-9]{2}', col)]

        stats_df = csv_as_df[country_col + date_cols]
        stats_df = stats_df.rename(columns={country_col[0]: 'country'})  # use a short name for country

        stats_df = stats_df.groupby('country', as_index=False).sum()  # sum of deaths of all states in a country
        stats_df = pd.melt(stats_df, id_vars=['country'], var_name='date', value_name='deaths')  # arrange vertically
        stats_df.loc[:, 'date'] = pd.to_datetime(stats_df['date'], infer_datetime_format=True)

        return stats_df

    def get_daily_change_of_deaths(self, df_total_deaths):
        """
        Calculates the daily change of deaths for each country

        :param df_total_deaths: Total deaths data frame
        :return: DataFrame showing change of deaths per day for each country
        """
        sorted_df = df_total_deaths.sort_values(['country', 'date'], ascending=[True, True], inplace=False)

        # Calculate difference between rows in each Country slice
        diffs = sorted_df.groupby(['country'])['deaths'].diff()
        diffs.loc[diffs.isna()] = sorted_df.loc[diffs.isna(), 'deaths']  # Replace nans with original value
        changes_df = sorted_df.rename(columns={'deaths': 'deaths_change'})
        changes_df.loc[:, 'deaths_change'] = diffs.astype(int)
        return changes_df

    def get_changed_rows(self, new_df, curr_df):
        """
        Filters the new data rows and the retrospectively updated data rows
        :param new_df: New data frame from the repository
        :param curr_df: Data currently in the database
        :return: List of changed row numbers in new_df
        """
        new_df.reset_index(drop=True, inplace=True)
        curr_df.loc[:, 'date'] = pd.to_datetime(curr_df.loc[:, 'date'], infer_datetime_format=True)
        diff = new_df[~new_df.isin(curr_df)]
        indices = diff.loc[~diff.iloc[:, 2].isna()].index
        return indices
