import datetime
import os
import sys
sys.path.append(os.path.abspath('../app'))
from collections import namedtuple
from unittest.mock import patch, Mock

import numpy as np
import pytest
import requests

from app.treat import forecast, get_hist_array
from app.data import Data


def test_forecast_array():
    dcl = Data()
    dcl.mid_time = datetime.datetime(2021, 9, 24, 0, 21, 8)
    path1 = os.path.abspath(os.path.dirname(__file__))
    path2 = os.path.join(path1, 'test_files', 'fake_response_forecast_9x3.txt')
    expected_result = np.array([[datetime.datetime(2021, 9, 25, 0, 21, 8), 279.18, 285.84]])
    with open(path2, "r") as f:
        fake_res = f.read()
        with patch("requests.get") as fake_get:
            fake_response = namedtuple("Response", "text status")
            fake_get.return_value = fake_response(text=fake_res, status=200)
            actual_result = forecast(dcl).fore_arr
            np.testing.assert_array_equal(actual_result, expected_result)


@pytest.fixture()
def call2():
    path1 = os.path.abspath(os.path.dirname(__file__))
    path2 = os.path.join(path1, 'test_files', 'fake_res_hist_data_10hours.txt')
    path3 = os.path.join(path1, 'test_files', 'fake_res_hist_data_24hours.txt')
    with open(path2, "r") as f1, open(path3, "r") as f2:
        fake_res1 = f1.read()
        fake_res2 = f2.read()
    return fake_res1, fake_res2


def test_historical_data_1_day_array(monkeypatch, call2):
    dcl = Data()
    m = Mock()
    fake_response = namedtuple("Response", "text status")
    m.side_effect = [fake_response(text=call2[0], status=200),
                     fake_response(text=call2[1], status=200)]
    expected_result = np.array([[datetime.datetime(2021, 5, 24, 0, 6, 26), 10, 30]])
    monkeypatch.setattr(requests, "get", m)
    actual_result = get_hist_array(dcl, 1).hist_arr
    np.testing.assert_array_equal(actual_result, expected_result)
