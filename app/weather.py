import csv
import os
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from typing import List, Tuple
from zipfile import ZipFile

import argparse
import numpy as np
import pandas as pd
# from geopy import Here
# from geopy.adapters import AioHTTPAdapter
# from geopy.geocoders import Nominatim
from pandas import DataFrame

import treat


class Data:
    centres = []
    threads = 50
    path_inp = 'D:\\weather_analysis\\test\\test_dir\\'
    path_out = 'D:\\weather_analysis\\test\\test_dir\\'
    api_key_forecast = "653e3ab3208c8cb799cce402dd5d7580"
    api_key_geoloc = 'zMvc4lJQRalNBC7yf3x2lXqlwTCM93jLCaCK4nm89cU'


@contextmanager
def unpack_files() -> None:
    path = Data.path_inp
    source_dir = os.listdir(path)
    list_open_files = unpack_zip(source_dir)
    try:
        yield
    finally:
        for file in list_open_files:
            try:
                os.remove(path + file)
            finally:
                continue


def unpack_zip(source_list: List[str]) -> List[str]:
    path = Data.path_inp
    for file in source_list:
        if file.endswith(".zip"):
            with ZipFile(path + file) as myzip:
                myzip.extractall(path)
                return myzip.namelist()


def readline_gen() -> List[str]:
    path_input = Data.path_inp
    source_dir = os.listdir(path_input)
    for file in source_dir:
        if file.endswith(".csv"):
            with open(path_input + file) as f:
                reader = csv.reader(f)
                for line in reader:
                    yield line


def coord_validator(coords: Tuple[str, str]) -> bool:
    latitude = coords[0]
    longitude = coords[1]
    try:
        latitude = float(latitude)
        longitude = float(longitude)
    except ValueError:
        return False
    return -90 <= latitude <= 90 and -180 <= longitude <= 180


def args_parser() -> Tuple[str, int, str]:
    parser = argparse.ArgumentParser(description='Weather analysis.')
    parser.add_argument('path_input', type=Path,
                        help='path to directory with input data')
    parser.add_argument('path_output', type=Path,
                        help='path to the directory for the output')
    parser.add_argument('threads', type=int,
                        help='number of threads for parallel data processing')
    args = parser.parse_args()
    os.chdir(args.path_input)
    return args.path_input, args.path_output, args.threads


def get_correct_df() -> DataFrame:
    with unpack_files():
        df = pd.DataFrame(columns=('Name', 'Country', 'City', 'Latitude', 'Longitude'))
        for num, line in enumerate(readline_gen()):
            val = coord_validator((line[4], line[5]))
            if line[1] and line[2] and line[3] and val:
                df.loc[num] = line[1:]
        return df


def get_address(latitude: float, longitude: float) -> str:
    pass
    # geolocator = Here(apikey=Data.api_key_geoloc) #, adapter_factory=AioHTTPAdapter
    # coord_str = str(latitude)+", "+str(longitude)
    # return str(geolocator.reverse(coord_str))


def get_list_addresses(df2: DataFrame) -> List[str]:
    lat = df2.Latitude.tolist()
    long = df2.Longitude.tolist()
    with ThreadPoolExecutor(max_workers=Data.threads) as pool:
        responses = pool.map(get_address, *(lat, long))
    return list(responses)


def get_cities_centre(df2: DataFrame) -> None:
    df_sort = df2.sort_values("City").reset_index(drop=True)
    hotels_1_city = []
    curr_city = (df_sort.loc[0].Country, df_sort.loc[0].City)
    try:
        for i in df_sort.loc:
            if curr_city == (i.Country, i.City):
                hotels_1_city.append((i.Latitude, i.Longitude))
            else:
                find_center = np.array(hotels_1_city).astype(float).mean(axis=0).tolist()
                Data.centres.append([curr_city, find_center])
                curr_city = (i.Country, i.City)
                hotels_1_city = [(i.Latitude, i.Longitude)]
    except KeyError:
        find_center = np.array(hotels_1_city).astype(float).mean(axis=0).tolist()
        Data.centres.append([curr_city, find_center])


def select_main_cities(df: DataFrame) -> DataFrame:
    df1 = df.groupby(by=["Country", "City"], as_index=False).agg({"Name": "count"})
    df1 = df1.sort_values(["Name"], ascending=False).groupby("Country").head(1)
    df2 = df.merge(df1[["Country", "City"]], how='inner')
    return df2.copy()


def add_geo_address(df2: DataFrame) -> None:
    df2.loc[:, "Geo_address"] = get_list_addresses(df2)
    df2 = df2[["Name", "Geo_address", "Country", "City", "Latitude", "Longitude"]]
    get_cities_centre(df2)


if __name__ == "__main__":
    args = args_parser()
    Data.path_inp = str(args[0]) + "\\"
    Data.threads = int(args[2])
    Data.path_out = str(args[1])
    add_geo_address(select_main_cities(get_correct_df()))
    print(len(Data.centres))
    print(Data.centres)
    treat.save_hist_graphics(Data.centres)
