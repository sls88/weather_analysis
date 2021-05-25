import datetime
import os
import shutil
import sys

sys.path.append(os.path.abspath('../app'))
from contextlib import contextmanager

import numpy as np
import pandas as pd
import pytest

from app.postprocess import concatenate_arrays, max_temp, min_temp
from app.postprocess import max_diff_max_temp, min_max_diff
from app.data import Data
from app.weather import set_global_arguments


def pathes():
    path1 = os.path.abspath(os.path.dirname(__file__))
    path_out = os.path.join(path1, '7777')
    set_global_arguments("", path_out)
    return path_out


@contextmanager
def del_test_folder():
    path_out = pathes()
    os.mkdir(path_out)
    try:
        yield
    finally:
        shutil.rmtree(os.path.join(path_out))


@pytest.fixture()
def dcl_list():
    dcl1 = Data()
    dcl1.sum_arr = np.array([['time1', 1, 1],
                             ['time1', 1, 1]])
    dcl1.fore_arr = np.array([['time2', 2, 2],
                              ['time2', 2, 2]])
    dcl1.hist_arr = np.array([['time3', 3, 3],
                              ['time3', 3, 3]])
    dcl1.country = "RU"
    dcl1.city = "Msk"
    dcl2 = Data()
    dcl2.sum_arr = np.array([['time1', 1, 1],
                             ['time1', 1, 1]])
    dcl2.fore_arr = np.array([['time2', 2, 2],
                              ['time2', 2, 2]])
    dcl2.hist_arr = np.array([['time3', 3, 3],
                              ['time3', 3, 3]])
    dcl2.country = "GE"
    dcl2.city = "Ber"
    return [dcl1, dcl2]


@pytest.fixture()
def f_sum_arr():
    return np.array([['GE', 'Ber', 'time1', 1, 1],
                     ['GE', 'Ber', 'time1', 1, 1],
                     ['RU', 'Msk', 'time1', 1, 1],
                     ['RU', 'Msk', 'time1', 1, 1]])


@pytest.fixture()
def f_fore_arr():
    return np.array([['GE', 'Ber', 'time2', 2, 2],
                     ['GE', 'Ber', 'time2', 2, 2],
                     ['RU', 'Msk', 'time2', 2, 2],
                     ['RU', 'Msk', 'time2', 2, 2]])


@pytest.fixture()
def f_hist_arr():
    return np.array([['GE', 'Ber', 'time3', 3, 3],
                     ['GE', 'Ber', 'time3', 3, 3],
                     ['RU', 'Msk', 'time3', 3, 3],
                     ['RU', 'Msk', 'time3', 3, 3]])


def test_create_concatenate_arrays_from_d_classes(
        dcl_list, f_sum_arr, f_fore_arr, f_hist_arr):
    with del_test_folder():
        sum_arr = concatenate_arrays(dcl_list)[0].to_numpy()
        fore_arr = concatenate_arrays(dcl_list)[1].to_numpy()
        hist_arr = concatenate_arrays(dcl_list)[2].to_numpy()
        actual_result = (sum_arr, fore_arr, hist_arr)
        expected_result = (f_sum_arr, f_fore_arr, f_hist_arr)
        np.testing.assert_array_equal(actual_result, expected_result)


@pytest.fixture()
def max_temp_df():
    df = pd.DataFrame(columns=('Country', 'City', 'Date', 'Max_t'))
    df.loc[len(df)] = ['GE', 'Ber', 'time1', 15]
    df.loc[len(df)] = ['GE', 'Ber', 'time2', 19]
    df.loc[len(df)] = ['GE', 'Ber', 'time3', 11]
    df.loc[len(df)] = ['RU', 'Msk', 'time4', 20]
    df.loc[len(df)] = ['RU', 'Msk', 'time5', 7]
    df.loc[len(df)] = ['MG', 'Mag', 'time6', 13]
    return df


@pytest.fixture()
def min_temp_df():
    df = pd.DataFrame(columns=('Country', 'City', 'Date', 'Min_t'))
    df.loc[len(df)] = ['GE', 'Ber', 'time1', 3]
    df.loc[len(df)] = ['GE', 'Ber', 'time2', 5]
    df.loc[len(df)] = ['GE', 'Ber', 'time3', 1]
    df.loc[len(df)] = ['RU', 'Msk', 'time4', 8]
    df.loc[len(df)] = ['RU', 'Msk', 'time5', 4]
    df.loc[len(df)] = ['MG', 'Mag', 'time6', 10]
    return df


@pytest.fixture()
def min_max_temp_df():
    df = pd.DataFrame(
        columns=('Country', 'City', 'Date', 'Min_t', 'Max_t'))
    df.loc[len(df)] = ['GE', 'Ber', 'time1', 5, 10]
    df.loc[len(df)] = ['GE', 'Ber', 'time2', -3, 3]
    df.loc[len(df)] = ['GE', 'Ber', 'time3', 26, 31]
    df.loc[len(df)] = ['RU', 'Msk', 'time4', 10, 20]
    df.loc[len(df)] = ['RU', 'Msk', 'time5', 10, 20]
    df.loc[len(df)] = ['MG', 'Mag', 'time6', 0, 0]
    return df


@pytest.fixture()
def max_change_max_df():
    df = pd.DataFrame(
        columns=('Country', 'City', 'Date', 'Min_t', 'Max_t'))
    df.loc[len(df)] = ['GE', 'Ber', 'time1', 5, 10]
    df.loc[len(df)] = ['GE', 'Ber', 'time2', -3, 3]
    df.loc[len(df)] = ['GE', 'Ber', 'time3', 26, 11]
    df.loc[len(df)] = ['RU', 'Msk', 'time4', 10, 20]
    df.loc[len(df)] = ['RU', 'Msk', 'time5', 10, 40]
    df.loc[len(df)] = ['MG', 'Mag', 'time6', 0, 100]
    return df


def test_max_temp(max_temp_df):
    with del_test_folder():
        dir_name = "max"
        path_to_file = os.path.join(pathes(), dir_name)
        os.mkdir(path_to_file)
        max_temp(max_temp_df, dir_name)
        df_csv = pd.read_csv(os.path.join(path_to_file,
                                          'max_temperature.csv'))
        ser_js = pd.read_json(os.path.join(
            path_to_file, 'max_temperature.json'), typ='series')
        df_json = pd.DataFrame([ser_js])
        actual_result1 = df_csv.to_numpy()
        actual_result2 = df_json.to_numpy()
        expected_result = np.array([['RU', 'Msk', 'time4', 20]], dtype=object)

        np.testing.assert_array_equal(actual_result1, expected_result)
        np.testing.assert_array_equal(actual_result2, expected_result)


def test_min_temp(min_temp_df):
    with del_test_folder():
        dir_name = "min"
        path_to_file = os.path.join(pathes(), dir_name)
        os.mkdir(path_to_file)
        min_temp(min_temp_df, dir_name)
        df_csv = pd.read_csv(os.path.join(path_to_file,
                                          'min_temperature.csv'))
        ser_js = pd.read_json(os.path.join(
            path_to_file, 'min_temperature.json'), typ='series')
        df_json = pd.DataFrame([ser_js])
        actual_result1 = df_csv.to_numpy()
        actual_result2 = df_json.to_numpy()
        expected_result = np.array([['GE', 'Ber', 'time3', 1]], dtype=object)

        np.testing.assert_array_equal(actual_result1, expected_result)
        np.testing.assert_array_equal(actual_result2, expected_result)


def test_max_difference_between_max_and_min_temp(min_max_temp_df):
    with del_test_folder():
        dir_name = "min_max"
        path_to_file = os.path.join(pathes(), dir_name)
        os.mkdir(path_to_file)
        min_max_diff(min_max_temp_df, dir_name)
        df_csv = pd.read_csv(os.path.join(
            path_to_file, 'max_difference_between_max_and_min_temp.csv'))
        ser_js = pd.read_json(os.path.join(
            path_to_file, 'max_difference_between_max_and_min_temp.json'), typ='series')
        df_json = pd.DataFrame([ser_js])
        actual_result1 = df_csv.to_numpy()
        actual_result2 = df_json.to_numpy()
        expected_result = np.array([['RU', 'Msk', 'time4', 10]], dtype=object)

        np.testing.assert_array_equal(actual_result1, expected_result)
        np.testing.assert_array_equal(actual_result2, expected_result)


def test_max_change_max_temp(max_change_max_df):
    with del_test_folder():
        dir_name = "min_max"
        path_to_file = os.path.join(pathes(), dir_name)
        os.mkdir(path_to_file)
        max_diff_max_temp(max_change_max_df, dir_name)
        df_csv = pd.read_csv(os.path.join(
            path_to_file, 'max_change_in_max_temp.csv'))
        ser_js = pd.read_json(os.path.join(
            path_to_file, 'max_change_in_max_temp.json'), typ='series')
        df_json = pd.DataFrame([ser_js])
        actual_result1 = df_csv.to_numpy()
        actual_result2 = df_json.to_numpy()
        expected_result = np.array([['RU', 'Msk', 20]], dtype=object)

        np.testing.assert_array_equal(actual_result1, expected_result)
        np.testing.assert_array_equal(actual_result2, expected_result)
