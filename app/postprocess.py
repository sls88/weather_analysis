import datetime
import os

import pandas as pd
import numpy as np
from pandas import DataFrame, Series

from config import Args


def create_df(lst_data, dir_name):
    df = pd.DataFrame(columns=('Country', 'City', 'Date', 'Min_t', 'Max_t'))
    for city_arr in lst_data:
        country = city_arr[0]
        city = city_arr[1]
        day_10_arr = city_arr[2]
        for line in day_10_arr:
            df.loc[len(df)] = [country, city, *line]
    return df, dir_name


def max_temp(df, dir_name):
    df = df.sort_values(["Max_t"], ascending=False).groupby("City").head(1).head(1)
    df = df[["Country", "City", "Date", "Max_t"]]
    df.to_csv(f'{Args.path_out}{dir_name}max_temperature.csv', index=False)
    df = df.reset_index(drop=True).loc[0]
    df.to_json(f'{Args.path_out}{dir_name}max_temperature.json', index=True)


def min_temp(df, dir_name):
    df = df.sort_values(["Min_t"], ascending=True).groupby("City").head(1).head(1)
    df = df[["Country", "City", "Date", "Min_t"]]
    df.to_csv(f'{Args.path_out}{dir_name}min_temperature.csv', index=False)
    df = df.reset_index(drop=True).loc[0]
    df.to_json(f'{Args.path_out}{dir_name}min_temperature.json', index=True)


def min_max_diff(df, dir_name):
    df.loc[:, 'Max_diff_b_max_min_temp'] = abs(df["Min_t"] - df["Max_t"])
    df = df.sort_values(["Max_diff_b_max_min_temp"],
                        ascending=False).groupby("City").head(1).head(1)
    df = df[["Country", "City", "Date", "Max_diff_b_max_min_temp"]]
    df.to_csv(f'{Args.path_out}{dir_name}max_difference_between_max_and_min_temp.csv', index=False)
    df = df.reset_index(drop=True).loc[0]
    df.to_json(f'{Args.path_out}{dir_name}max_difference_between_max_and_min_temp.json', index=True)


def max_diff_max_temp(df, dir_name):
    def get_max_diff(values):
        return abs(max(values) - min(values))

    df = df.groupby(by=["City", "Country"], as_index=False).Max_t.agg(get_max_diff)
    df = df.sort_values(["Max_t"], ascending=False).head(1)
    df = df.rename(columns={"Max_t": "Max_diff_max_temp"})
    df = df[["Country", "City", "Max_diff_max_temp"]]
    df.to_csv(f'{Args.path_out}{dir_name}max_change_in_max_temp.csv', index=False)
    df = df.reset_index(drop=True).loc[0]
    df.to_json(f'{Args.path_out}{dir_name}max_change_in_max_temp.json', index=True)


def create_dframes(data):
    all = (data[0], "10_days_analysis\\")
    fore = (data[1], "5_days_analysis_forecast\\")
    hist = (data[2], "5_days_analysis_historical\\")
    for i in [all, fore, hist]:
        os.mkdir(Args.path_out + i[1])
        yield create_df(*i)


def general_postprocess_func_save(data_arrays):
    for num, df in enumerate(create_dframes(data_arrays)):
        if num == 0:
            df[0].to_csv(f'{Args.path_out}weather_data_array.csv', index=False)
        max_temp(*df)
        min_temp(*df)
        max_diff_max_temp(*df)
        min_max_diff(*df)
