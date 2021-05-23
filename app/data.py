"""Module defining the data structure."""

import datetime
from dataclasses import dataclass

import numpy as np


@dataclass
class Data:
    """Store data, for the convenience of passing them as arguments to functions. """
    country: str = ""
    city: str = ""
    lat: float = 0.0
    lon: float = 0.0
    curr_time: datetime = datetime.timedelta(0)
    mid_time: datetime = datetime.timedelta(0)
    temp: float = 0.0
    hist_arr: np.ndarray = np.empty((0, 3))
    fore_arr: np.ndarray = np.empty((0, 3))
    sum_arr: np.ndarray = np.empty((0, 3))
