import datetime
from dataclasses import dataclass

import numpy as np


@dataclass
class Data:
    country: str
    city: str
    lat: float
    lon: float
    curr_time: datetime = datetime.timedelta(0)
    mid_time: datetime = datetime.timedelta(0)
    temp: float = 0
    hist_arr: np.ndarray = np.empty((0, 3))
    fore_arr: np.ndarray = np.empty((0, 3))
    sum_arr: np.ndarray = np.empty((0, 3))
