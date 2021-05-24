import os
import sys
sys.path.append(os.path.abspath('../app'))
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from app.weather import coord_validator, get_correct_df, select_main_cities
from app.weather import get_cities_centre, get_list_addresses
from app.weather import add_geo_address, set_global_arguments


@pytest.fixture()
def correct_data():
    path1 = os.path.abspath(os.path.dirname(__file__))
    path_input = os.path.join(path1, 'correct_check')
    set_global_arguments(path_input)
    df = pd.DataFrame(columns=('Name', 'Country', 'City', 'Latitude', 'Longitude'))
    df.loc[len(df)] = ['Ibis', 'F', 'Coal', '49.802514', '9.921491']
    df.loc[len(df)] = ['Super', 'F', 'Kast', '35.451305', '-94.805814']
    return df


@pytest.fixture()
def cities_df():
    df = pd.DataFrame(columns=('Name', 'Country', 'City', 'Latitude', 'Longitude'))
    df.loc[len(df)] = ['Name1', 'US', 'Coal', '30', '30']
    df.loc[len(df)] = ['Name2', 'US', 'Yalta', '35', '35']
    df.loc[len(df)] = ['Name3', 'GE', 'Felf', '67', '67']
    df.loc[len(df)] = ['Name4', 'RU', 'Len', '70', '20']
    df.loc[len(df)] = ['Name5', 'RU', 'Ikra', '11', '11']
    df.loc[len(df)] = ['Name6', 'RU', 'Len', '66', '66']
    df.loc[len(df)] = ['Name7', 'IT', 'Rome', '66', '66']
    df.loc[len(df)] = ['Name8', 'IT', 'Rome', '42', '12']
    df2 = pd.DataFrame(columns=('Name', 'Country', 'City', 'Latitude', 'Longitude'))
    df2.loc[len(df2)] = ['Name1', 'US', 'Coal', '30', '30']
    df2.loc[len(df2)] = ['Name3', 'GE', 'Felf', '67', '67']
    df2.loc[len(df2)] = ['Name4', 'RU', 'Len', '70', '20']
    df2.loc[len(df2)] = ['Name6', 'RU', 'Len', '66', '66']
    df2.loc[len(df2)] = ['Name7', 'IT', 'Rome', '66', '66']
    df2.loc[len(df2)] = ['Name8', 'IT', 'Rome', '42', '12']
    return df, df2


@pytest.fixture()
def centre_30_30i25_25():
    df = pd.DataFrame(columns=('Name', 'Country', 'City', 'Latitude', 'Longitude'))
    df.loc[len(df)] = ['Name1', 'RU', 'Len', '20', '40']
    df.loc[len(df)] = ['Name2', 'RU', 'Len', '40', '40']
    df.loc[len(df)] = ['Name3', 'RU', 'Len', '40', '20']
    df.loc[len(df)] = ['Name4', 'RU', 'Len', '20', '20']
    df.loc[len(df)] = ['Name5', 'MS', 'Tet', '50', '50']
    df.loc[len(df)] = ['Name6', 'GE', 'Be', '0', '80']
    df.loc[len(df)] = ['Name7', 'GE', 'Be', '80', '0']
    return df


@pytest.fixture()
def df_adresses():
    df = pd.DataFrame(columns=('Name', 'Country', 'City', 'Latitude', 'Longitude'))
    df.loc[len(df)] = ['Name1', 'RU', 'Len', '20', '40']
    df2 = pd.DataFrame(columns=("Name", "Geo_address", "Country", "City", "Latitude", "Longitude"))
    df2.loc[len(df2)] = ['Name1', "Madisson Av. 210", 'RU', 'Len', '20', '40']
    return df, df2


def test_get_correct_df(correct_data):
    expected_result = correct_data.to_numpy()
    actual_result = get_correct_df().to_numpy()

    np.testing.assert_array_equal(expected_result, actual_result)


def test_select_main_cities(cities_df):
    expected_result = cities_df[1].to_numpy()
    actual_result = select_main_cities(cities_df[0]).to_numpy()

    np.testing.assert_array_equal(expected_result, actual_result)


def test_get_cities_centre(centre_30_30i25_25):
    list_dataclasses = get_cities_centre(centre_30_30i25_25)
    actual_result = set()
    for dcl in list_dataclasses:
        actual_result.add((dcl.country, dcl.city, dcl.lat, dcl.lon))
    expected_result = {('GE', 'Be', 40.0, 40.0), ('RU', 'Len', 30.0, 30.0), ('MS', 'Tet', 50.0, 50.0)}

    assert actual_result == expected_result


@pytest.mark.parametrize(
    ("value", "expected_result"),
    [
        (('', ''), False),
        (('32.359833', '-86.217384a'), False),
        (('38.97034343', '-89.12880845'), True),
        (('388.9703434', '-89.12880845'), False),
        (('42.071304', ''), False),
        ((0, 0), True),
        ((-90, 180), True),
    ],
)
def test_coord_validator(value, expected_result):
    actual_result = coord_validator(value)

    assert actual_result == expected_result


def test_get_list_adressesIadd_geo_address(df_adresses):
    with patch('geopy.Here.reverse') as service_mock:
        service_mock.return_value = "Madisson Av. 210"
        actual_result = get_list_addresses(df_adresses[0])
        expected_result = ["Madisson Av. 210"]
        actual_result2 = add_geo_address(df_adresses[0]).to_numpy()
        expected_result2 = df_adresses[1].to_numpy()

        assert actual_result == expected_result
        assert (expected_result2 == actual_result2).all()
