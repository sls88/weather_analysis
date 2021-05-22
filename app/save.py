import os
from concurrent.futures import ProcessPoolExecutor
from typing import List

import matplotlib.pyplot as plt
from pandas import DataFrame

from config import Args, Config
from data import Data
from treat import weather


def save_hotels_inf(df2: DataFrame) -> None:
    for city, group in df2.groupby('City'):
        country = group.iloc[0].Country
        group = group[["Name", "Geo_address",
                       "Latitude", "Longitude"]].reset_index(drop=True)
        len_arr = group.shape[0]
        p = len_arr // 100 + 2
        for i in range(100, p * 100, 100):
            slice_ = group.loc[i - 100: i - 1]
            slice_.to_csv('{}.csv'.format(
                Args.path_out + country + "\\" + city
                + "\\" + city + f'{i}'), index=False)


def save_weather(dcl: Data) -> Data:
    path = Args.path_out
    dcl = weather(dcl)
    min_temp = dcl.sum_arr[:, 1]
    max_temp = dcl.sum_arr[:, 2]
    date_day = dcl.sum_arr[:, 0]
    fig, ax = plt.subplots()
    ax.plot(date_day, min_temp, "b", label="min day temperature, C")
    ax.plot(date_day, max_temp, "r", label="max day temperature, C")
    plt.scatter(dcl.curr_time, dcl.temp, color='g', s=40,
                marker='o', label=f"current temp. {dcl.temp}, C")
    ax.grid()
    ax.legend()
    plt.xticks(date_day)
    plt.xlabel('Date, 1 day')
    plt.ylabel('Temperature, C')
    plt.title(f'{dcl.city}. Day temperature.')
    plt.axvline(x=dcl.curr_time)
    fig.autofmt_xdate()
    new_path = path + dcl.country + '\\' + dcl.city + '\\'
    if not os.path.isdir(new_path):
        os.makedirs(new_path)
    fig.savefig(new_path + f'weather_{dcl.city}.png')
    return dcl


def save_graphics(centres: List[Data]) -> List[Data]:
    with ProcessPoolExecutor(max_workers=Config.processes) as pool:
        d_classes = pool.map(save_weather, centres)
    return [dcl for dcl in d_classes]
