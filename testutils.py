import pandas as pd


def get_dummy_data():
    data = {
        'Province/State': ['Victoria', 'New South Wales', '', 'Texas', 'Virginia', 'England'],
        'Country/Region': ['Australia', 'Australia', 'Sri Lanka', 'US', 'US', 'UK'],
        'Lat': [2.11, 3.1, 6.1, 9.3, 9.6, 8.5],
        '1/1/2020': [0, 0, 0, 0, 0, 0],
        '1/4/2020': [0, 0, 1, 3, 2, 1],
        '1/15/2020': [3, 1, 0, 5, 5, 4],
        '2/1/2020': [4, 1, 0, 10, 7, 6],
        '2/13/2020': [5, 1, 3, 9, 0, 2]}

    return pd.DataFrame(data)
