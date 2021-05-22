import json
import numpy as np
from datetime import datetime, timedelta, timezone
from typing import Dict, List, NoReturn, Tuple, Union

import requests

from config import Config


class Curr:
    curr_time = None
    temp = None
    mid_time = None


def set_curr_time(data_hist: Dict) -> None:
    if not Curr.curr_time:
        c_time = int(data_hist["current"]["dt"])
        Curr.curr_time = datetime.fromtimestamp(c_time)
        Curr.temp = data_hist["current"]["temp"]
    if not Curr.mid_time:
        Curr.mid_time = Curr.curr_time - timedelta(hours=12)


def forecast(coords: List[float]) -> np.ndarray:
    api_key = Config.api_key_forecast
    lat, lon = coords[0], coords[1]
    url = "http://api.openweathermap.org/data/2.5/forecast"
    res = requests.get(url, params={"lat": lat,
                                    "lon": lon,
                                    "units": "metric",
                                    "appid": api_key})
    data = json.loads(res.text)
    return get_forecast_arr(data)


def get_forecast_arr(data_f: Dict) -> np.ndarray:
    temps = [hour3['main']['temp'] for hour3 in data_f["list"]]
    arr = np.empty((0, 3))
    for i in range(5):
        c_date = Curr.mid_time + timedelta(days=i + 1)
        max_temp = max(temps[:8])
        min_temp = min(temps[:8])
        del temps[:8]
        day = np.array([c_date, min_temp, max_temp])
        arr = np.vstack((arr, day))
    return arr[::-1]


class DayHistWeather:
    def __init__(self, coords: List[float], days: int) -> None:
        self.days = days
        self.coords = coords
        self.lat = coords[0]
        self.lon = coords[1]
        self.t_stamp = None
        self.day_temp = []
        self.count = 0
        self.data = None
        self.day_arr = None

    def historical_weather(self) -> None:
        api_key = Config.api_key_forecast
        url = "https://api.openweathermap.org/data/2.5/onecall/timemachine"
        res = requests.get(url, params={"lat": self.lat,
                                        "lon": self.lon,
                                        "units": "metric",
                                        "dt": self.t_stamp,
                                        "appid": api_key})
        self.data = json.loads(res.text)

    def get_hist_day(self) -> None:
        if len(self.day_temp) >= 24:
            min_temp = min(self.day_temp[:24])
            max_temp = max(self.day_temp[:24])
            mid_date = Curr.mid_time - timedelta(days=self.count - 1)
            self.day_arr = np.array([mid_date, min_temp, max_temp])
            del self.day_temp[:24]

    def treat_data(self) -> None:
        set_curr_time(self.data)
        for i in reversed(self.data["hourly"]):
            self.day_temp.append(i["temp"])

    def change_timestamp(self) -> None:
        tn = datetime.now(timezone.utc)
        td = timedelta(days=self.count)
        self.t_stamp = str((tn - td).timestamp())[:10]

    def __iter__(self) -> 'DayHistWeather':
        return self

    def __next__(self) -> Union[np.ndarray, NoReturn]:
        if self.count < self.days:
            while len(self.day_temp) < 24:
                self.count += 1
                self.change_timestamp()
                self.historical_weather()
                self.treat_data()
            self.get_hist_day()
            return self.day_arr
        else:
            raise StopIteration


def get_hist_array(coords: List[float], days: int):
    arr = np.empty((0, 3))
    for day in DayHistWeather(coords, days):
        arr = np.vstack((arr, day))
    return arr


def weather(coords: List[float]):
    hist_arr = get_hist_array(coords, 5)
    fore_arr = forecast(coords)
    return np.concatenate((fore_arr, hist_arr))
