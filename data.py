import pandas as pd
import re


def get_daily_deaths_by_country(csv_as_df):
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
    stats_df['date'] = pd.to_datetime(stats_df['date'], infer_datetime_format=True)

    return stats_df
