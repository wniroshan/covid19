import pandas as pd
import re


def get_daily_deaths_by_country(df):
    """
    Coverts the horizontally growing csv table to a vertically growing RDBMS friendly table with one 'date' column
    :param df: DataFrame loaded from the csv file
    :return: DataFrame with one row per each Country and Date.
    """

    csv_cols = df.columns.values

    # Regex selects the column names that start with the term 'Country'
    country_col = [col for col in csv_cols if re.match('[cC]ountry', col)]
    if len(country_col) != 1:
        raise ValueError(
            "Cannot determine country column. Found " + str(len(country_col)) + " with term 'Country'." + str(
                country_col))

    # Regex selects the column names that look like a date
    date_cols = [col for col in csv_cols if re.match('(?:[0-9]{1,2}/){1,2}[0-9]{2}', col)]

    # Use a common name for the country column and make a copy of the df.
    df_by_country = df[country_col + date_cols].rename(columns={country_col[0]: 'country'})

    df_by_country = df_by_country.groupby('country', as_index=False).sum()
    df_by_country = pd.melt(df_by_country, id_vars=['country'], var_name='date', value_name='deaths')
    df_by_country['date'] = pd.to_datetime(df_by_country['date'], infer_datetime_format=True)

    return df_by_country
