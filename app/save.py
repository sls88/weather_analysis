from pandas import DataFrame

import weather

def save_hotels_inf(df2: DataFrame) -> None:
    for city, group in df2.groupby('City'):
        country = group.iloc[0].Country
        group = group[["Name", "Geo_address",
                       "Latitude", "Longitude"]].reset_index(drop=True)
        len_arr = group.shape[0]
        p = len_arr // 100 + 2
        for i in range(100, p * 100, 100):
            slice_ = group.loc[i - 101: i]
            slice_.to_csv('{}.csv'.format(
                weather.Data.path_out + country + "\\" + city
                + "\\" + city + f'{i}'), index=False)
