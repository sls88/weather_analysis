"""Module saving data."""

import os
from concurrent.futures import ProcessPoolExecutor
from typing import List

import matplotlib.pyplot as plt
from pandas import DataFrame

from config import Args, Config
from data import Data
from treat import weather


class SavePostProc:
    """Save post-processing analysis data."""
    @staticmethod
    def save_max_temp(df: DataFrame, dir_name: str) -> None:
        """Write data to .csv .json files.

            City and day of observation with the maximum temperature.
        Args:
            df: Data format ("Country", "City", "Date", "Max_t")
            dir_name: the name of the directory corresponding to the type of analyzed data
                        (historical data, forecast data, historical + forecast)
        """
        df.to_csv(os.path.join(f'{Args.path_out}{dir_name}',
                               'max_temperature.csv'), index=False)
        df = df.reset_index(drop=True).loc[0]
        df.to_json(os.path.join(f'{Args.path_out}{dir_name}',
                                'max_temperature.json'), index=True)

    @staticmethod
    def save_min_temp(df: DataFrame, dir_name: str) -> None:
        """Write data to .csv .json files.

            City and day of observation with the minimum temperature.
        Args:
            df: Data format ("Country", "City", "Date", "Min_t")
            dir_name: the name of the directory corresponding to the type of analyzed data
                        (historical data, forecast data, historical + forecast)
        """
        df.to_csv(os.path.join(f'{Args.path_out}{dir_name}', 'min_temperature.csv'), index=False)
        df = df.reset_index(drop=True).loc[0]
        df.to_json(os.path.join(f'{Args.path_out}{dir_name}', 'min_temperature.json'), index=True)

    @staticmethod
    def save_min_max_diff(df: DataFrame, dir_name: str) -> None:
        """Write data to .csv .json files.

            City and day with the maximum difference between the maximum and minimum temperatures.
        Args:
            df: Data format ("Country", "City", "Date", "Max_diff_b_max_min_temp")
            dir_name: the name of the directory corresponding to the type of analyzed data
                        (historical data, forecast data, historical + forecast)
        """
        df.to_csv(os.path.join(f'{Args.path_out}{dir_name}',
                               'max_difference_between_max_and_min_temp.csv'), index=False)
        df = df.reset_index(drop=True).loc[0]
        df.to_json(os.path.join(f'{Args.path_out}{dir_name}',
                                'max_difference_between_max_and_min_temp.json'), index=True)

    @staticmethod
    def save_max_diff_max_temp(df: DataFrame, dir_name: str) -> None:
        """Write data to .csv .json files.

            City with maximum change in maximum temperature.
        Args:
            df: Data format ("Country", "City", "Max_diff_max_temp")
            dir_name: the name of the directory corresponding to the type of analyzed data
                        (historical data, forecast data, historical + forecast)
        """
        df.to_csv(os.path.join(f'{Args.path_out}{dir_name}',
                               'max_change_in_max_temp.csv'), index=False)
        df = df.reset_index(drop=True).loc[0]
        df.to_json(os.path.join(f'{Args.path_out}{dir_name}',
                                'max_change_in_max_temp.json'), index=True)


class SaveData:
    """Save in multithreaded mode."""
    @staticmethod
    def save_hotels_inf(df2: DataFrame) -> None:
        """Write the list of hotels to .csv .

            Write (name, address, latitude, longitude) in CSV format in files containing no more than 100 entries each

        Args:
            df2: Dataframe in ["Name", "Geo_address", "Country", "City", "Latitude", "Longitude"] format

        Returns:
            Write to .csv in ["Name", "Geo_address", "Latitude", "Longitude"] format.
        """
        for city, group in df2.groupby('City'):
            country = group.iloc[0].Country
            group = group[["Name", "Geo_address",
                           "Latitude", "Longitude"]].reset_index(drop=True)
            len_arr = group.shape[0]
            p = len_arr // 100 + 2
            for i in range(100, p * 100, 100):
                slice_ = group.loc[i - 100: i - 1]
                slice_.to_csv(os.path.join(f'{Args.path_out}{country}',
                                           f'{city}', f'{city}{i}.csv'), index=False)

    @staticmethod
    def get_concat_array(dcl):
        return weather(dcl)

    @staticmethod
    def save_weather(dcl: Data) -> Data:
        """Build graphs containing day dependencies and write to files.

            - minimum temperature;
            - maximum temperature
            Place all the results obtained in the output directory with the following structure:
            `{output_folder}\{country}\{city}\`

        Args:
            dcl: Dataclass instance.

        Returns:
            Dataclass instance (pass for postprocessing)
        """
        dcl = SaveData.get_concat_array(dcl)
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
        new_path = Args.path_out + os.path.join(f"{dcl.country}", f"{dcl.city}")
        if not os.path.isdir(new_path):
            os.makedirs(new_path)
        fig.savefig(os.path.join(new_path, f'weather_{dcl.city}.png'))
        return dcl

    @staticmethod
    def save_graphics(centres: List[Data]) -> List[Data]:
        """Save all received charts in the catalog with the specified structure for each city.

        Args:
            centres: Dataclass instance with with information about the country, city, center coordinates

        Returns:
            List of instances of a dataclass with information about the city,
            including arrays of weather data(historical, forecast, combine), for post-processing.
        """
        with ProcessPoolExecutor(max_workers=Config.processes) as pool:
            d_classes = pool.map(SaveData.save_weather, centres)
        return [dcl for dcl in d_classes]
