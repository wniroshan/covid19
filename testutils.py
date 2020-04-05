import pandas as pd


def get_dummy_data():
    data = {
        'Province/State': ['Victoria', 'New South Wales', '', 'Texas', 'Virginia', 'England'],
        'Country/Region': ['Australia', 'Australia', 'Sri Lanka', 'US', 'US', 'UK'],
        'Lat': [2.11, 3.1, 6.1, 9.3, 9.6, 8.5],
        '1/1/2020': [0, 0, 0, 0, 0, 0],
        '1/4/2020': [0, 0, 1, 3, 2, 1],
        '1/15/2020': [3, 1, 1, 5, 5, 4],
        '2/1/2020': [4, 1, 1, 10, 7, 6],
        '2/13/2020': [5, 3, 3, 11, 8, 8]}

    return pd.DataFrame(data)


def get_dummy_change_data():
    expected_df = pd.DataFrame(data={
        'country': ['Australia', 'Australia', 'Australia', 'Australia', 'Australia', 'Sri Lanka', 'Sri Lanka',
                    'Sri Lanka', 'Sri Lanka', 'Sri Lanka', 'UK', 'UK', 'UK', 'UK', 'UK', 'US', 'US', 'US', 'US',
                    'US'],
        'date': ['2020-01-01', '2020-01-04', '2020-01-15', '2020-02-01', '2020-02-13', '2020-01-01', '2020-01-04',
                 '2020-01-15', '2020-02-01', '2020-02-13', '2020-01-01', '2020-01-04', '2020-01-15', '2020-02-01',
                 '2020-02-13', '2020-01-01', '2020-01-04', '2020-01-15', '2020-02-01', '2020-02-13'],
        'deaths_change': [None, 0, 4, 1, 3, None, 1, 0, 0, 2, None, 1, 3, 2, 2, None, 5, 5, 7, 2]
    })
    expected_df['date'] = pd.to_datetime(expected_df['date'])

    return expected_df
