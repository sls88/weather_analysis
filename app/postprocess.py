"""Post-processing module."""

import os
from typing import List, Tuple

import pandas as pd
import numpy as np
from pandas import DataFrame

from data import Data
from config import Args


def max_temp(df: DataFrame, dir_name: str) -> None:
    """Find the city and day of observation with the maximum temperature. Write data to .csv .json files.

    Args:
        df: Data format ('Country', 'City', 'Date', 'Min_t', 'Max_t')
        dir_name: the name of the directory corresponding to the type of analyzed data
                    (historical data, forecast data, historical + forecast)
    """
    df = df.sort_values(["Max_t"], ascending=False).groupby("City").head(1).head(1)
    df = df[["Country", "City", "Date", "Max_t"]]
    df.to_csv(f'{Args.path_out}{dir_name}max_temperature.csv', index=False)
    df = df.reset_index(drop=True).loc[0]
    df.to_json(f'{Args.path_out}{dir_name}max_temperature.json', index=True)


def min_temp(df: DataFrame, dir_name: str) -> None:
    """Find the city and day of observation with the minimum temperature. Write data to .csv .json files.

    Args:
        df: Data format ('Country', 'City', 'Date', 'Min_t', 'Max_t')
        dir_name: the name of the directory corresponding to the type of analyzed data
                    (historical data, forecast data, historical + forecast)
    """
    df = df.sort_values(["Min_t"], ascending=True).groupby("City").head(1).head(1)
    df = df[["Country", "City", "Date", "Min_t"]]
    df.to_csv(f'{Args.path_out}{dir_name}min_temperature.csv', index=False)
    df = df.reset_index(drop=True).loc[0]
    df.to_json(f'{Args.path_out}{dir_name}min_temperature.json', index=True)


def min_max_diff(df: DataFrame, dir_name: str) -> None:
    """Find city with maximum change in maximum temperature . Write data to .csv .json files.

    Args:
        df: Data format ('Country', 'City', 'Date', 'Min_t', 'Max_t')
        dir_name: the name of the directory corresponding to the type of analyzed data
                    (historical data, forecast data, historical + forecast)
    """
    df.loc[:, 'Max_diff_b_max_min_temp'] = abs(df["Min_t"] - df["Max_t"])
    df = df.sort_values(["Max_diff_b_max_min_temp"],
                        ascending=False).groupby("City").head(1).head(1)
    df = df[["Country", "City", "Date", "Max_diff_b_max_min_temp"]]
    df.to_csv(f'{Args.path_out}{dir_name}'
              f'max_difference_between_max_and_min_temp.csv', index=False)
    df = df.reset_index(drop=True).loc[0]
    df.to_json(f'{Args.path_out}{dir_name}'
               f'max_difference_between_max_and_min_temp.json', index=True)


def max_diff_max_temp(df: DataFrame, dir_name: str) -> None:
    """Find the city and day with the maximum difference between the maximum and minimum temperatures.

        Write data to .csv .json files.

    Args:
        df: Data format ('Country', 'City', 'Date', 'Min_t', 'Max_t')
        dir_name: the name of the directory corresponding to the type of analyzed data
                    (historical data, forecast data, historical + forecast)
    """
    def get_max_diff(values):
        return abs(max(values) - min(values))

    df = df.groupby(by=["City", "Country"], as_index=False).Max_t.agg(get_max_diff)
    df = df.sort_values(["Max_t"], ascending=False).head(1)
    df = df.rename(columns={"Max_t": "Max_diff_max_temp"})
    df = df[["Country", "City", "Max_diff_max_temp"]]
    df.to_csv(f'{Args.path_out}{dir_name}max_change_in_max_temp.csv', index=False)
    df = df.reset_index(drop=True).loc[0]
    df.to_json(f'{Args.path_out}{dir_name}max_change_in_max_temp.json', index=True)


def create_df(dcl: Data, dcl_arr: np.ndarray) -> DataFrame:
    """Create pandas dataframe from numpy array.

    Args:
        dcl: City. Country
        dcl_arr: Numpy array

    Returns:
        Pandas dataframe ('Country', 'City', 'Date', 'Min_t', 'Max_t') format
    """
    df = pd.DataFrame(columns=('Country', 'City', 'Date', 'Min_t', 'Max_t'))
    for i in dcl_arr:
        df.loc[len(df)] = [dcl.country, dcl.city, *i]
    return df


def concatenate_arrays(d_classes: List[Data]) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """Combine pandas dataframe arrays (belonging to different cities).

    Args:
        d_classes: List of dataclasses, with data for each city

    Returns:
        1. Combined array (historical data + forecast)
        2. An array of weather forecast data for all cities
        3. An array of historical data for all cities
    """
    df_all = pd.DataFrame(columns=('Country', 'City', 'Date', 'Min_t', 'Max_t'))
    df_hist = pd.DataFrame(columns=('Country', 'City', 'Date', 'Min_t', 'Max_t'))
    df_forc = pd.DataFrame(columns=('Country', 'City', 'Date', 'Min_t', 'Max_t'))
    for d in d_classes:
        df_all = pd.concat([create_df(d, d.sum_arr), df_all], ignore_index=True)
        df_hist = pd.concat([create_df(d, d.hist_arr), df_hist], ignore_index=True)
        df_forc = pd.concat([create_df(d, d.fore_arr), df_forc], ignore_index=True)
    return df_all, df_forc, df_hist


def general_postprocess_func_save(d_classes: List[Data]) -> None:
    """Synchronize the launch of post-processing functions.

    Args:
        d_classes: List of dataclasses, with data for each city
    """
    dframes = concatenate_arrays(d_classes)
    dir_names = ("10_days_analysis\\", "5_days_analysis_forecast\\",
                 "5_days_analysis_historical\\")
    for df, dir_name in zip(dframes, dir_names):
        os.mkdir(Args.path_out + dir_name)
        max_temp(df, dir_name)
        min_temp(df, dir_name)
        max_diff_max_temp(df, dir_name)
        min_max_diff(df, dir_name)
