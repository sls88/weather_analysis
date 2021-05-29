"""Main module."""

import csv
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from typing import List, Tuple
from zipfile import ZipFile

import click
import numpy as np
import pandas as pd
from geopy import Here
from pandas import DataFrame

from config import Args
from data import Data
from postprocess import general_postprocess_func_save
from save import SaveData
from secret import KEY


@contextmanager
def unpack_files() -> None:
    """Unpack and delete these files after work."""
    path = Args.path_inp
    source_dir = os.listdir(path)
    list_open_files = unpack_zip(source_dir)
    try:
        yield
    finally:
        for file in list_open_files:
            try:
                os.remove(os.path.join(path, file))
            finally:
                continue


def unpack_zip(source_list: List[str]) -> List[str]:
    """Unpack the .zip archive and return the list of files inside.

    Args:
        source_list:

    Returns:
        List of files inside
    """
    path = Args.path_inp
    for file in source_list:
        if file.endswith(".zip"):
            with ZipFile(os.path.join(path, file)) as myzip:
                myzip.extractall(path)
                return myzip.namelist()


def readline_gen() -> List[str]:
    """Generate strings from unpacked .csv files

    Returns:
        Strings from unpacked .csv files
    """
    path_input = Args.path_inp
    source_dir = os.listdir(path_input)
    for file in source_dir:
        if file.endswith(".csv"):
            with open(path_input + file) as f:
                reader = csv.reader(f)
                for line in reader:
                    yield line


def coord_validator(coords: Tuple[str, str]) -> bool:
    """Check format and bounds of coordinate values.

        Limits. -90 <= latitude <= 90 and -180 <= longitude <= 180
    Args:
        coords: latitude, longitude

    Returns:
        True if coordinate value is correct.
    """
    latitude = coords[0]
    longitude = coords[1]
    try:
        latitude = float(latitude)
        longitude = float(longitude)
    except ValueError:
        return False
    return -90 <= latitude <= 90 and -180 <= longitude <= 180


CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])

@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument('path_input')
@click.argument('path_output')
@click.argument('threads', type=click.INT)
def args_parser(path_input: str, path_output: str, threads: int) -> None:
    """Weather analysis.

    \b
    Go to the root folder of the weather_analysis application
    Type the command in the console:
    python PATH_TO_CATALOG\\app\weather.py PATH_INPUT PATH_OUTPUT THREADS

    \b
    PATH_INPUT - the path to the resource file (the trailing slash is not needed)
    PATH_OUTPUT - path to the directory where to put the information after processing
    THREADS - number of threads to process data

    \b
    Please note that the specified directory, with the archive of resources,
    should not contain .CSV files, because automatic unpacking occurs,
    followed by deletion of all .CSV files from this directory
    """
    os.chdir(path_input)
    set_global_arguments(path_input, path_output, threads)
    logging.info("Program started")
    main()


def get_correct_df() -> Tuple[DataFrame, int]:
    """Check if the necessary values and coordinates exist and write a string to the dataframe.

    Returns:
        df: Dataframe object
        no_valid_counter: counter no valid lines
    """
    with unpack_files():
        df = pd.DataFrame(columns=('Name', 'Country', 'City', 'Latitude', 'Longitude'))
        no_valid_counter = 0
        for num, line in enumerate(readline_gen()):
            val = coord_validator((line[4], line[5]))
            if line[1] and line[2] and line[3] and val:
                df.loc[num] = line[1:]
            else:
                no_valid_counter += 1
        return df, no_valid_counter


def get_address(latitude: float, longitude: float) -> str:
    """Get the geographic address of the hotel .

    Args:
        latitude: latitude
        longitude: longitude

    Returns:
        A string with the geographic address of the hotel
    """
    geolocator = Here(apikey=KEY.api_key_geoloc)
    coord_str = str(latitude)+", "+str(longitude)
    return str(geolocator.reverse(coord_str))


def get_list_addresses(df2: DataFrame) -> List[str]:
    """Get data for all hotels with their geographic address.

    Args:
        df2: Hotel coordinates

    Returns:
        List of addresses.
    """
    lat = df2.Latitude.tolist()
    long = df2.Longitude.tolist()
    with ThreadPoolExecutor(max_workers=Args.threads) as pool:
        responses = pool.map(get_address, *(lat, long))
    return list(responses)


def get_cities_centre(df2: DataFrame) -> List[Data]:
    """Calculate the geographic center of a city area equidistant from the outermost hotels.

    Args:
        df2: Coordinates of all hotels. City names.

    Returns:
        List of dataframes with area centers. (grouped by city)
    """
    centres = []
    for city, group in df2.groupby("City"):
        country = group.iloc[0].Country
        lst = list(zip(group.Latitude.values.tolist(),
                       group.Longitude.values.tolist()))
        center = np.array(lst).astype(float).mean(axis=0).tolist()
        centres.append(Data(country, str(city), center[0], center[1]))
    return centres


def select_main_cities(df: DataFrame) -> DataFrame:
    """Select the city containing the maximum number of hotels for each country.

    Args:
        df: Data

    Returns:
        City data (after grouping) with coordinates.
    """
    df1 = df.groupby(by=["Country", "City"], as_index=False).agg({"Name": "count"})
    df1 = df1.sort_values(["Name"], ascending=False).groupby("Country").head(1)
    df2 = df.merge(df1[["Country", "City"]], how='inner')
    return df2.copy()


def add_geo_address(df: DataFrame) -> DataFrame:
    """Enrich data for all hotels with their geographic address.

    Args:
        df: Hotel data, no geographic address

    Returns:
        Hotel data, with their geographic address.
    """
    df.loc[:, "Geo_address"] = get_list_addresses(df)
    df = df[["Name", "Geo_address", "Country", "City", "Latitude", "Longitude"]]
    return df


def set_global_arguments(path_inp: str = "", path_out: str = "",
                         threads: int = 100) -> None:
    """Set arguments into config.Args.

    Args:
        path_inp: Path to directory with input data
        path_out: Path to the directory for the output
        threads: Number of threads for parallel data processing
    """
    Args.path_inp = os.path.join(path_inp, '')
    Args.path_out = os.path.join(path_out, '')
    Args.threads = threads


def main() -> None:
    """Synchronize the sequence of execution of program functions."""
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    logging.info(" Program started")
    df_t = get_correct_df()
    logging.info(" Completed getting the correct dataframe. Deleted incorrect positions: %s" % df_t[1])
    df = select_main_cities(df_t[0])
    logging.info(" The process of enriching data about hotels with their geographic address has begun ... ")
    df = add_geo_address(df)
    logging.info(" The dataframe with the geographic address of the hotels has been successfully created.")
    centres = get_cities_centre(df)
    logging.info(" Coordinates of city centers are calculated.")
    logging.info(" The process of saving charts has begun...")
    lst_d_classes = SaveData.save_graphics(centres)
    logging.info(" Saving charts completed.")
    SaveData.save_hotels_inf(df)
    logging.info(" Hotel information saved successfully.")
    logging.info(" Post processing started...")
    general_postprocess_func_save(lst_d_classes)
    logging.info(" Completed saving information.")
    logging.info(" Data available in the catalog %s" % Args.path_out)

if __name__ == "__main__":
    args_parser()
