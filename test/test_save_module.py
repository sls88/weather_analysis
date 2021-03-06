import datetime
import os
import shutil
import sys
sys.path.append(os.path.abspath('../app'))
from contextlib import contextmanager

import numpy as np
import pandas as pd
import pytest

from app.data import Data
from app.save import SaveData
from app.weather import set_global_arguments


@pytest.fixture()
def output_folder():
    path_output = os.path.abspath(os.path.dirname(__file__))
    set_global_arguments("", path_output)
    return path_output


@contextmanager
def clean_folder(path, name_dir):
    try:
        yield
    finally:
        shutil.rmtree(os.path.join(path, name_dir))


class DataTest:
    def data(self):
        dcl = Data()
        dcl.sum_arr = np.array([[datetime.datetime(2021, 5, 25, 0, 21, 8), 5, 1],
                                [datetime.datetime(2021, 5, 24, 0, 21, 8), 5, 1],
                                [datetime.datetime(2021, 5, 23, 0, 21, 8), 5, 1],
                                [datetime.datetime(2021, 5, 22, 0, 21, 8), 5, 1],
                                [datetime.datetime(2021, 5, 21, 0, 21, 8), 5, 1]])
        dcl.curr_time = datetime.datetime(2021, 5, 23, 0, 21, 8)
        dcl.temp = 20
        dcl.country = "RU"
        dcl.city = "Msk"
        return dcl


def test_save_graphics(output_folder, monkeypatch):
    with clean_folder(output_folder, "RU"):
        monkeypatch.setattr(SaveData, "get_concat_array", DataTest.data)
        SaveData.save_weather(DataTest.data)
        expected_result = os.listdir(os.path.join(output_folder, "RU", "Msk"))[0]
        actual_result = "weather_Msk.png"

    assert actual_result == expected_result


@pytest.fixture()
def pathes():
    path1 = os.path.abspath(os.path.dirname(__file__))
    path_out = os.path.join(path1, '1315')
    set_global_arguments("", path_out)
    city1dir = os.path.join(path_out, 'GB', 'London')
    city2dir = os.path.join(path_out, 'IN', 'Kubik')
    os.makedirs(city1dir)
    os.makedirs(city2dir)
    return path_out, city1dir, city2dir


@pytest.fixture()
def test_dataframe():
    df = pd.DataFrame(columns=("Name", "Geo_address", "Country",
                               "City", "Latitude", "Longitude"))
    for i in range(131):
        df.loc[len(df)] = ["Name", "Baker Str.", "GB",
                           "London", "30", "20"]
    for i in range(74):
        df.loc[len(df)] = ["Name2", "Indiana", "IN",
                           "Kubik", "30", "20"]
    return df


def test_100lines_in_csv(test_dataframe, pathes):
    p = pathes
    path_out, city1dir, city2dir = p[0], p[1], p[2]
    with clean_folder(path_out, ''):
        SaveData.save_hotels_inf(test_dataframe)
        df1 = pd.read_csv(os.path.join(city1dir, "London100.csv"))
        df2 = pd.read_csv(os.path.join(city1dir, "London200.csv"))
        df3 = pd.read_csv(os.path.join(city2dir, "Kubik100.csv"))
        actual_result = [df1.count()[0], df2.count()[0], df3.count()[0]]
        expected_result = [100, 31, 74]

        assert actual_result == expected_result
