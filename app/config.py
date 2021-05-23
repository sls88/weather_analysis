"""Module, data configuration. """

class Config:
    """Custom configurations. """
    api_key_forecast = "653e3ab3208c8cb799cce402dd5d7580"
    api_key_geoloc = '2K65icNeCQ-C6YQh09cijJYtBZC_m59HCFq6bmks614'
    processes = 8


class Args:
    """Arguments from command line parsing."""
    threads = 100
    path_inp = "" #'D:\\weather_analysis\\test\\test_dir\\'
    path_out = "" #'D:\\weather_analysis\\test\\test_dir\\'
