import json
import os
import numpy as np
from concurrent.futures import ProcessPoolExecutor
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Tuple

import matplotlib.pyplot as plt
import requests

from weather import Data


def forecast(lat: float, lon: float) -> Dict:
    api_key = Data.api_key_forecast
    lat = str(lat)
    lon = str(lon)
    url = "http://api.openweathermap.org/data/2.5/forecast"
    res = requests.get(url, params={"lat": lat, "lon": lon,
                                    "appid": api_key})
    data = json.loads(res.text)
    return data


def historical_weather(coord: List, t_stamp: str) -> np.ndarray:
    api_key = Data.api_key_forecast
    lat = str(coord[0])
    lon = str(coord[1])
    url = "https://api.openweathermap.org/data/2.5/onecall/timemachine"
    res = requests.get(url, params={"lat": lat, "lon": lon, "units": "metric",
                                    "dt": t_stamp, "appid": api_key})
    data = json.loads(res.text)
    min_temp = min(i["temp"] for i in data["hourly"])
    max_temp = max(i["temp"] for i in data["hourly"])
    req_date = date.fromtimestamp(int(t_stamp))
    return np.array([req_date, min_temp, max_temp])


def get_hist_array(coord: List) -> np.ndarray:
    arr = np.empty((0, 3))
    for day in range(1, 6):
        tn = datetime.now(timezone.utc)
        td = timedelta(days = day)
        t_stamp = str((tn - td).timestamp())[:10]
        day_hw = historical_weather(coord, t_stamp)
        arr = np.vstack((arr, day_hw))
    return arr


def save_hist_temp(centres: List) -> None:
    city, coord = centres[0], centres[1]
    path = Data.path_out
    hist_arr = get_hist_array(coord)
    min_temp = hist_arr[:, 1]
    max_temp = hist_arr[:, 2]
    date_day = hist_arr[:, 0]
    fig, ax = plt.subplots()
    ax.plot(date_day, min_temp, "b", label="min day temperature, C")
    ax.plot(date_day, max_temp, "r", label="max day temperature, C")
    ax.legend()
    plt.xticks(date_day)
    plt.xlabel('Date, 1 day')
    plt.ylabel('Temperature, C')
    plt.title('London. Day temperature.')
    new_path = path + city[0] + '\\' + city[1] + '\\'
    if not os.path.isdir(new_path):
        os.makedirs(new_path)
    fig.savefig(new_path + f'hist_{city[1]}.png')


def save_hist_graphics(centres):
    with ProcessPoolExecutor(max_workers=Data.threads) as pool:
        responses = pool.map(save_hist_temp, [*centres])
    for _ in responses:
        pass



# https://api.openweathermap.org/data/2.5/onecall/timemachine?lat=40.64589&lon=-109.729469&dt=1621147451&appid="653e3ab3208c8cb799cce402dd5d7580"
# print(forecast(40.64589, -109.729469, "653e3ab3208c8cb799cce402dd5d7580"))

# print(datetime.fromtimestamp(1621206000))
# # 1621126800
# , params={"lat": lat, "lon": lon, "appid": api}
