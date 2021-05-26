"""Module, data configuration. """


class Config:
    """Custom configurations.

    The class contains variables about the keys to the API of the weather sites,
    if the keys run out, you will need to go through the registration procedure at
    https://openweathermap.org to get api_key_forecast and at
    https://developer.here.com for api_key_geoloc.
    processes = 8 - The number of processes involved in receiving weather data,
    processing and saving. By default, the number is set to 8, but if your computer
    supports more, you can change this number.
    """
    api_key_forecast = "653e3ab3208c8cb799cce402dd5d7580"
    api_key_geoloc = '2K65icNeCQ-C6YQh09cijJYtBZC_m59HCFq6bmks614'
    processes = 8


class Args:
    """Arguments from command line parsing."""
    threads = 100
    path_inp = ""
    path_out = ""
