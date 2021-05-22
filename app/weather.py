import csv
import os
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from typing import List, Tuple
from zipfile import ZipFile

import argparse
import numpy as np
import pandas as pd
# from geopy.adapters import AioHTTPAdapter
# from geopy.geocoders import Nominatim
from geopy import Here
from pandas import DataFrame

from config import Args, Config
from data import Data
from postprocess import general_postprocess_func_save
from save import save_graphics, save_hotels_inf


@contextmanager
def unpack_files() -> None:
    path = Config.path_inp
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
    path = Config.path_inp
    for file in source_list:
        if file.endswith(".zip"):
            with ZipFile(path + file) as myzip:
                myzip.extractall(path)
                return myzip.namelist()


def readline_gen() -> List[str]:
    path_input = Config.path_inp
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
    parser.add_argument('path_input', type=str,
                        help='path to directory with input data')
    parser.add_argument('path_output', type=str,
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
    geolocator = Here(apikey=Config.api_key_geoloc) #, adapter_factory=AioHTTPAdapter
    coord_str = str(latitude)+", "+str(longitude)
    return str(geolocator.reverse(coord_str))


def get_list_addresses(df2: DataFrame) -> List[str]:
    lat = df2.Latitude.tolist()
    long = df2.Longitude.tolist()
    with ThreadPoolExecutor(max_workers=Config.threads) as pool:
        responses = pool.map(get_address, *(lat, long))
    return list(responses)


def get_cities_centre(df2: DataFrame) -> List[Data]:
    centres = []
    for city, group in df2.groupby("City"):
        country = group.iloc[0].Country
        lst = list(zip(group.Latitude.values.tolist(),
                       group.Longitude.values.tolist()))
        center = np.array(lst).astype(float).mean(axis=0).tolist()
        centres.append(Data(country, str(city), center[0], center[1]))
    return centres


def select_main_cities(df: DataFrame) -> DataFrame:
    df1 = df.groupby(by=["Country", "City"], as_index=False).agg({"Name": "count"})
    df1 = df1.sort_values(["Name"], ascending=False).groupby("Country").head(1)
    df2 = df.merge(df1[["Country", "City"]], how='inner')
    return df2.copy()


def add_geo_address(df: DataFrame) -> DataFrame:
    df.loc[:, "Geo_address"] = get_list_addresses(df)
    df = df[["Name", "Geo_address", "Country", "City", "Latitude", "Longitude"]]
    return df


def main() -> None:
    args = args_parser()
    Config.path_inp = str(args[0])
    Config.threads = int(args[2])
    Config.path_out = str(args[1])
    df = add_geo_address(select_main_cities(get_correct_df()))
    centres = get_cities_centre(df)
    lst_d_classes = save_graphics(centres)
    save_hotels_inf(df)
    general_postprocess_func_save(lst_d_classes)


if __name__ == "__main__":
    main()
