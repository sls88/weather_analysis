import datetime

import pandas as pd
import numpy as np
from pandas import DataFrame, Series

from config import Args


class DF:
    df = pd.DataFrame(columns=('Country', 'City', 'Date', 'Min_t', 'Max_t'))


def create_df(city, day_10_arr):
    country = city[0]
    city = city[1]
    for line in day_10_arr:
        DF.df.loc[len(DF.df)] = [country, city, *line]


def max_temp(df):
    df = df.sort_values(["Max_t"],
                        ascending=False).groupby("City").head(1).max()
    df = df[["Country", "City", "Date", "Max_t"]]
    df.to_json(f'{Args.path_out}max_temperature.json', index=True)
    df.to_csv(f'{Args.path_out}max_temperature.csv', index=True)


def min_temp(df):
    df = df.sort_values(["Min_t"],
                        ascending=True).groupby("City").head(1).min()
    df = df[["Country", "City", "Date", "Min_t"]]
    df.to_json(f'{Args.path_out}min_temperature.json', index=True)
    df.to_csv(f'{Args.path_out}min_temperature.csv', index=True)


def min_max_diff(df):
    df.loc[:, 'Max_diff'] = abs(df["Min_t"] - df["Max_t"])
    df = df.sort_values(["Max_diff"],
                        ascending=False).groupby("City").head(1).max()
    df = df[["Country", "City", "Date", "Max_diff"]]
    df.to_json(f'{Args.path_out}max_max_min_diff.json', index=True)
    df.to_csv(f'{Args.path_out}max_max_min_diff.csv', index=True)


def max_diff_max_temp(df):
    def get_max_diff(values):
        return abs(max(values) - min(values))
    df = df.groupby(by=["City", "Country"],
                    as_index=False).Max_t.agg(get_max_diff).max()
    df.to_json(f'{Args.path_out}max_diff_max_temp.json', index=True)
    df.to_csv(f'{Args.path_out}max_diff_max_temp.csv', index=True)


def general_postprocess_func_save():
    df = DF.df
    max_temp(df)
    min_temp(df)
    max_diff_max_temp(df)
    min_max_diff(df)
