"""Module for receiving and processing data."""

import json
import numpy as np
from datetime import datetime, timedelta, timezone
from typing import Dict, NoReturn, Union

import requests

from data import Data
from secret import KEY


def set_curr_time(data_hist: Dict, dcl: Data) -> None:
    """Set the current temperature, time, midpoint of the day time zone.

        It is necessary in order not to contact the api for the forecast at the current time.
        Because this information is given in the first request for the current day.
        curr_time - request time (stored to display the point of the current temperature on the graph)
        mid_time - curr_time-12 hours.
    Args:
        data_hist: Historical data received from the weather server
        dcl: Dataclass instance
    """
    if not dcl.curr_time:
        c_time = int(data_hist["current"]["dt"])
        dcl.curr_time = datetime.fromtimestamp(c_time)
        dcl.temp = data_hist["current"]["temp"]
    if not dcl.mid_time:
        dcl.mid_time = dcl.curr_time - timedelta(hours=12)


def forecast(dcl: Data) -> Data:
    """Get weather forecast for 5 days (at 3 hour intervals).

    Args:
        dcl: Dataclass instance

    Returns:
        Dataclass instance
    """
    api_key = KEY.api_key_forecast
    url = "http://api.openweathermap.org/data/2.5/forecast"
    res = requests.get(url, params={"lat": dcl.lat,
                                    "lon": dcl.lon,
                                    "units": "metric",
                                    "appid": api_key})
    data = json.loads(res.text)
    return get_forecast_arr(data, dcl)


def get_forecast_arr(data_f: Dict, dcl: Data) -> Data:
    """Write an array of weather forecast data for 5 days (with a 3 hour interval) to the dataclass.

        Data for each day (8 pieces) are analyzed for maximum and minimum temperatures and filled
        in the corresponding columns of the array

    Args:
        data_f: Forecast data received from the weather server
        dcl: Dataclass instance for writing data.

    Returns:
        Dataclass instance.
    """
    temps = [hour3['main']['temp'] for hour3 in data_f["list"]]
    arr = np.empty((0, 3))
    for i in range(len(temps) // 8):
        c_date = dcl.mid_time + timedelta(days=i + 1)
        max_temp = max(temps[:8])
        min_temp = min(temps[:8])
        del temps[:8]
        day = np.array([c_date, min_temp, max_temp])
        arr = np.vstack((arr, day))
    dcl.fore_arr = arr[::-1]
    return dcl


class DayHistWeather:
    """Get historical weather data from the server, process, return 1 day array.
    Args:
        dcl: Dataclass instance
        days: The number of days for which historical data is needed. Amount must be > 0.
    """
    def __init__(self, dcl: Data, days: int) -> None:
        self.dcl = dcl
        self.days = days + 1
        self.t_stamp = None
        self.day_temp = []
        self.count = 0
        self.j_data = None
        self.day_arr = None
        self.day_output = 0

    def historical_weather(self) -> None:
        """Get historical weather data.
        For the first day, data is returned from the current hour to 3:00 of the previous one,
        then 24 hours in each request. Also, each request contains the temperature at the current
        moment (we only need it from the first request)
        Returns:
            Write data to self.j_data
        """
        api_key = KEY.api_key_forecast
        url = "https://api.openweathermap.org/data/2.5/onecall/timemachine"
        res = requests.get(url, params={"lat": self.dcl.lat,
                                        "lon": self.dcl.lon,
                                        "units": "metric",
                                        "dt": self.t_stamp,
                                        "appid": api_key})
        self.j_data = json.loads(res.text)

    def get_hist_day(self) -> None:
        """Find the maximum and minimum temperature in 24 hours. Write to array.
        Returns:
            Write to self.day_arr
        """
        min_temp = min(self.day_temp[:24])
        max_temp = max(self.day_temp[:24])
        mid_date = self.dcl.mid_time - timedelta(days=self.day_output)
        self.day_arr = np.array([mid_date, min_temp, max_temp])
        del self.day_temp[:24]

    def treat_data(self) -> None:
        """Process the obtained temperature data. Add to the list."""
        set_curr_time(self.j_data, self.dcl)
        for i in reversed(self.j_data["hourly"]):
            self.day_temp.append(i["temp"])

    def change_timestamp(self) -> None:
        """Change timestamp for making a new api request."""
        tn = datetime.now(timezone.utc)
        td = timedelta(days=self.count)
        self.t_stamp = str((tn - td).timestamp())[:10]

    def __iter__(self) -> 'DayHistWeather':
        """Iterator object."""
        return self

    def __next__(self) -> Union[np.ndarray, NoReturn]:
        """Return an array of historical weather data for 1 day.
        Returns:
            Array of historical weather data for 1 day
        """
        if self.day_output < self.days - 1:
            while len(self.day_temp) < 24:
                self.change_timestamp()
                self.count += 1
                self.historical_weather()
                self.treat_data()
            self.get_hist_day()
            self.day_output += 1
            return self.day_arr
        else:
            raise StopIteration


def get_hist_array(dcl: Data, days: int) -> Data:
    """Collect an array of historical weather data from the received data, write it to a dataclass.

    Args:
        dcl: Dataclass instance
        days: The number of days for which the data array is needed

    Returns:
        Dataclass instance
    """
    arr = np.empty((0, 3))
    for day in DayHistWeather(dcl, days):
        arr = np.vstack((arr, day))
    dcl.hist_arr = arr
    return dcl


def weather(dcl: Data) -> Data:
    """Combine arrays of historical and forecast data (for 1 city).

        Write to dataclass instance.

    Args:
        dcl: Dataclass instance

    Returns:
        Dataclass instance.
    """
    get_hist_array(dcl, 5)
    dcl = forecast(dcl)
    dcl.sum_arr = np.concatenate((dcl.fore_arr, dcl.hist_arr))
    return dcl
