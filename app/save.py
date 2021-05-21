import os
from concurrent.futures import ProcessPoolExecutor
from typing import Dict, List, Tuple

import matplotlib.pyplot as plt
from pandas import DataFrame

from config import Args
from postprocess import create_df
from treat import Curr, weather


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


def save_weather(center: List) -> None:
    city, coords = center[0], center[1]
    path = Args.path_out
    day_10_arr = weather(coords)
    create_df(city, day_10_arr)  # for postprocessing
    min_temp = day_10_arr[:, 1]
    max_temp = day_10_arr[:, 2]
    date_day = day_10_arr[:, 0]
    fig, ax = plt.subplots()
    ax.plot(date_day, min_temp, "b", label="min day temperature, C")
    ax.plot(date_day, max_temp, "r", label="max day temperature, C")
    plt.scatter(Curr.curr_time, Curr.temp, color='g', s=40,
                marker='o', label=f"current temp. {Curr.temp}, C")
    ax.grid()
    ax.legend()
    plt.xticks(date_day)
    plt.xlabel('Date, 1 day')
    plt.ylabel('Temperature, C')
    plt.title(f'{city[1]}. Day temperature.')
    plt.axvline(x=Curr.curr_time)
    fig.autofmt_xdate()
    new_path = path + city[0] + '\\' + city[1] + '\\'
    if not os.path.isdir(new_path):
        os.makedirs(new_path)
    fig.savefig(new_path + f'weather_{city[1]}.png')


def save_graphics(centres):
    with ProcessPoolExecutor(max_workers=Args.threads) as pool:
        responses = pool.map(save_weather, centres)
    for _ in responses:
        pass
