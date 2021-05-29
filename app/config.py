"""Module data configuration. """


class Config:
    """Custom configurations.

    processes = 8 - The number of processes involved in receiving weather data,
    processing and saving. By default, the number is set to 8, but if your computer
    supports more, you can change this number.
    """
    processes = 8


class Args:
    """Arguments from command line parsing."""
    threads = 100
    path_inp = ""
    path_out = ""
