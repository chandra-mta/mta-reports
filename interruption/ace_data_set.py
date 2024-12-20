#!/proj/sot/ska3/flight/bin/python


"""
**ace_data_set.py** Extract ACE data, compute statistics, and plot values.

:Author: W. Aaron (william.aaron@cfa.harvard.edu)
:Last Updated: Dec 17, 2024
:Note: This script is designed to be a submodule of **run_interruption.py**

"""

import os
import numpy as np
from datetime import datetime, timedelta
from kadi.events import rad_zones
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import shutil
from astropy.io import ascii
from astropy.table import vstack, Column, unique
import subprocess

#
# --- Define Directory Pathing
#
WEB_DIR = "/data/mta_www/mta_interrupt"
OUT_WEB_DIR = "/data/mta_www/mta_interrupt"
WEB_DIR2 = "/data/mta4/www/RADIATION_new/mta_interrupt"
OUT_WEB_DIR2 = "/data/mta4/www/RADIATION_new/mta_interrupt"
INTERRUPT_DIR = "/data/mta/Script/Interrupt"

PATHING_DICT = {
    "WEB_DIR": WEB_DIR,
    "OUT_WEB_DIR": OUT_WEB_DIR,
    "WEB_DIR2": WEB_DIR2,
    "OUT_WEB_DIR2": OUT_WEB_DIR2,
    "INTERRUPT_DIR": INTERRUPT_DIR,
}

_ACE_INPUT_DATA_TIME_FORMAT = "%Y %m %d  %H%M" #: Time format for input ACE radiation data archive written INTERRUPT_DIR.
_ACE_DATA_TIME_FORMAT = "%Y:%j:%H:%M:%S"  #: Time format for human-reference files.

#: Column format for input ACE radiation data archive written INTERRUPT_DIR.
#: 
#: **Note:** Status indicators: 0 = nominal, 4,6,7,8 = bad data, unable to process, 9 = no data, -1 = missing data
#: 
#: **Units:** KeV
_INPUT_ACE_COLUMNS = ["year",
                      "month",
                      "day",
                      "hhmm",
                      "mjd",
                      "daysecs",
                      "electron_status",
                      "electron38-53",
                      "electron175-315",
                      "proton_status",
                      "proton47-68",
                      "proton115-195",
                      "proton310-580",
                      "proton795-1193",
                      "proton1060-1900",
                      "aniso"
                     ]

#
# --- File heading information
#
_ACE_CHANNEL_SELECT = ["electron38-53",
                       "electron175-315",
                       "proton47-68",
                       "proton115-195",
                       "proton310-580",
                       "proton795-1193",
                       "proton1060-1900",
                       "aniso"
                      ]  #: Selection of ACE table channels for the human-reference text file
_subhead = "\t\t".join(_ACE_CHANNEL_SELECT)
_ACE_DATA_HEADER = (
    f"Science Run Interruption: #LSTART\n\nTime\t\t{_subhead}\n{'-'*100}\n"
)
_ACE_STAT_HEADER = f"\t\tAvg\t\t\tMax\t\tTime\t\tMin\t\tTime\t\tValue at Start of Interruption\n{'-'*95}\n"


def ace_data_set(event_data, pathing_dict):
    print("ACE Data Set")
    time_start = _round_down(event_data["tstart"]) - timedelta(days=2)
    time_stop = _round_down(event_data["tstop"]) + timedelta(days=2)
    ace_table = fetch_ACE_data_table(time_start, time_stop, pathing_dict)
    write_ace_files(ace_table, event_data, pathing_dict)

def fetch_ACE_data_table(time_start, time_stop, pathing_dict):
    """Fetch ACE data from the ``INTERRUPT_DIR/Data`` archive files and format into an astropy table.

    :param time_start: Starting time for data fetch, defaults to two days before the start of the interruption event.
    :type time_start: ``DateTime``
    :param time_stop: Stopping time for data fetch, defaults to two days after the start of the interruption event.
    :type time_stop: ``DateTime``
    :param pathing_dict: A dictionary of file paths for storing file input and output.
    :type pathing_dict: dict(str, str)
    :return: Table of unique ACE data points spanning interruption event.
    :rtype: ``astropy.table.Table``
    :Note: While algorithmically very similar to the data fetch performed in the :mod:`~interruption.goes_data_set.py` script,
        this table stores ``Time`` as a ``DateTime`` object rather than a string to reduce computation due to how ACE archive data files are stored.

    """
    #
    # --- Check if the interruption spans over the new year
    # --- and therefore need to construct ACE data table from two files.
    #
    if time_start.year == time_stop.year: # One Year File
        ace_table = _single_file_fetch(time_start, time_stop, pathing_dict)
    elif time_start.year != time_stop.year:  # Two Year Files
        ace_table = _double_file_fetch(time_start, time_stop, pathing_dict)
    #
    # --- Reformat columns for easier processing
    #
    for i, col in enumerate(_INPUT_ACE_COLUMNS):
        ace_table.rename_column(f"col{i+1}", col)
    #
    # --- Format complete time column
    #
    datetime_col = Column(_convert_time_format(ace_table['year'], ace_table['month'], ace_table['day'], ace_table['hhmm']), name="datetime")
    ace_table.add_column(datetime_col)
    
    return unique(ace_table, keys='datetime')

def write_ace_files(ace_table, event_data, pathing_dict):
    """Write ACE data and statistics to human-reference text file.

    :param ace_table: ACE data table read from ``INTERRUPT_DIR/Data``.
    :type ace_table: astropy.table.Table
    :param event_data: A dictionary which stores interruption data.
    :type event_data: dict(str, datetime or float or str)
    :param pathing_dict: A dictionary of file paths for storing file input and output.
    :type pathing_dict: dict(str, str)
    :raises ValueError: If the starting or stopping time line of data cannot be found in the data archive.
    :raises FileNotFoundError: If the data archive file cannot be found.
    :File Out: Writes the ``<event_name>_ace.txt`` data table to the two ``OUT_WEB_DIR/Data_dir`` directories,
        and writes the ``<event_name>_ace_stat`` statistics table to the two ``OUT_WEB_DIR/Stat_dir`` directories.

    """
    #
    # --- Write Data File
    #
    line = _ACE_DATA_HEADER.replace(
        "#LSTART", event_data["tstart"].strftime("%Y:%m:%d:%H:%M")
    )
    for row in ace_table:
        substring = f"{row['Time']}\t\t"
        for channel in _ACE_CHANNEL_SELECT:
            if channel == _ACE_CHANNEL_SELECT[-1]:
                substring += f"{row[channel]}"
            else:
                substring += f"{row[channel]:.3e}\t\t"
        line += f"{substring}\n"
    
    ifile = os.path.join(
        pathing_dict["OUT_WEB_DIR"], "Data_dir", f"{event_data['name']}_ace.txt"
    )
    ifile2 = os.path.join(
        pathing_dict["OUT_WEB_DIR2"], "Data_dir", f"{event_data['name']}_ace.txt"
    )
    os.makedirs(os.path.dirname(ifile), exist_ok=True)
    os.makedirs(os.path.dirname(ifile2), exist_ok=True)
    with open(ifile, "w") as f:
        f.write(line)
    if ifile != ifile2:
        shutil.copy(ifile, ifile2)

    #
    # --- Write Stat File.
    #
    sel = ace_table["Time"] == event_data["tstart"]
    interrupt_row = ace_table[sel][0]
    line = _ACE_STAT_HEADER
    for channel in _ACE_CHANNEL_SELECT:
        avg = np.mean(ace_table[channel].data)
        std = np.std(ace_table[channel].data)

        maxidx = np.argmax(ace_table[channel].data)
        max = ace_table[channel][maxidx]
        maxtime = ace_table["Time"][maxidx].strftime(_ACE_DATA_TIME_FORMAT)

        minidx = np.argmin(ace_table[channel].data)
        min = ace_table[channel][minidx]
        mintime = ace_table["Time"][minidx].strftime(_ACE_DATA_TIME_FORMAT)

        val_intt = interrupt_row[channel]

        line += f"{channel}\t\t{avg:.3e}+/-{std:.3e}\t{max:.3e}\t{maxtime}\t{min:.3e}\t{mintime}\t{val_intt}\n"
        
    ifile = os.path.join(
        pathing_dict["OUT_WEB_DIR"], "Stat_dir", f"{event_data['name']}_ace_stat"
    )
    ifile2 = os.path.join(
        pathing_dict["OUT_WEB_DIR2"], "Stat_dir", f"{event_data['name']}_ace_stat"
    )
    os.makedirs(os.path.dirname(ifile), exist_ok=True)
    os.makedirs(os.path.dirname(ifile2), exist_ok=True)
    with open(ifile, "w") as f:
        f.write(line)
    if ifile != ifile2:
        shutil.copy(ifile, ifile2)
#
# --- Internal functions to assist cleanly formatting the input ACE table from text to astropy table
#
def _round_down(dt):
    """Round a DateTime object down to the nearest five minutes. For use in fetching from data files.

    :param dt: A ``DateTime`` object of any kind
    :type dt: DateTime
    :return: The input ``DateTime`` object rounded down to the nearest five minutes.
    :rtype: DateTime

    """
    delta_min = dt.minute % 5
    return datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute - delta_min)

@np.vectorize
def _convert_time_format(year,month,day,hhmm):
    """Converts separated ``numpy.ndarray`` containing date information into an array of ``DateTime`` objects.

    :param year: Four digit year
    :type year: int
    :param month: Month
    :type month: int
    :param day: Day
    :type day: int
    :param hhmm: Integer Combining Hours and Minutes
    :type hhmm: int
    :return: ``numpy.ndarray`` of ``DateTime`` objects.
    :rtype: ``numpy.ndarray(dtype = 'object')``

    """
    hh = hhmm // 100 #: hours in hundreds and thousands place
    mm = hhmm % 100 #: minutes in tens and ones place
    return datetime.strptime(f"{year:04}:{month:02}:{day:02}:{hh:02}:{mm:02}", "%Y:%m:%d:%H:%M")

def _single_file_fetch(time_start, time_stop, pathing_dict):
    """Fetch ACE data from a single archive file.

    :param time_start: Starting time for data fetch, defaults to two days before the start of the interruption event.
    :type time_start: ``DateTime``
    :param time_stop: Stopping time for data fetch, defaults to two days after the start of the interruption event.
    :type time_stop: ``DateTime``
    :param pathing_dict: A dictionary of file paths for storing file input and output.
    :type pathing_dict: dict(str, str)
    :raises ValueError: If the starting or stopping time line of data cannot be found in the data archive.
    :raises FileNotFoundError: If the data archive file cannot be found.
    :return: Table of unique ACE data points spanning interruption event.
    :rtype: ``astropy.table.Table``

    """
    data_start = None
    data_stop = None
    data_file = os.path.join(
        pathing_dict["INTERRUPT_DIR"], "Data", f"rad_data{time_start.year}"
    )
    #
    # --- Find data line for start
    #
    while data_start is None:
        try:
            data_start_search = f"grep -in '{time_start.strftime(_ACE_INPUT_DATA_TIME_FORMAT)}' {data_file}"
            data_start = int(
                subprocess.check_output(
                    data_start_search, shell=True, executable="/bin/csh"
                )
                .decode()
                .split(":")[0]
            )
        except subprocess.CalledProcessError as error:
            if error.returncode == 1:
                #
                # --- Could not find the data with that specific time
                #
                time_start += timedelta(minutes=5)
            elif error.returncode == 2:
                raise FileNotFoundError(f"{data_file}")
        if time_start > time_stop:
            raise ValueError(f"Cannot find start time line in {data_file}.")
    #
    # --- Find data line for stop
    #
    while data_stop is None:
        try:
            data_stop_search = f"grep -in '{time_stop.strftime(_ACE_INPUT_DATA_TIME_FORMAT)}' {data_file}"
            data_stop = int(
                subprocess.check_output(
                    data_stop_search, shell=True, executable="/bin/csh"
                )
                .decode()
                .split(":")[0]
            )
        except subprocess.CalledProcessError as error:
            if error.returncode == 1:
                #
                # --- Could not find the data with that specific time
                #
                time_stop -= timedelta(minutes=5)
            elif error.returncode == 2:
                raise FileNotFoundError(f"{data_file}")
        if time_stop < time_start:
            raise ValueError(f"Cannot find stop time line in {data_file}.")        
    ace_table = ascii.read(data_file, data_start = data_start, data_end = data_stop)
    return ace_table

def _double_file_fetch(time_start, time_stop, pathing_dict):
    """Fetch ACE data from a two archive files. For use in the event an interruption spans across the new year.

    :param time_start: Starting time for data fetch, defaults to two days before the start of the interruption event.
    :type time_start: ``DateTime``
    :param time_stop: Stopping time for data fetch, defaults to two days after the start of the interruption event.
    :type time_stop: ``DateTime``
    :param pathing_dict: A dictionary of file paths for storing file input and output.
    :type pathing_dict: dict(str, str)
    :raises ValueError: If the starting or stopping time line of data cannot be found in the data archive.
    :raises FileNotFoundError: If the data archive file cannot be found.
    :return: Table of unique ACE data points spanning interruption event.
    :rtype: ``astropy.table.Table``

    """
    data_start = None
    data_stop = None
    #
    # --- Interruption occurred during the new year.Therefore fetch from two data files.
    #
    data_file_start = os.path.join(
        pathing_dict["INTERRUPT_DIR"], "Data", f"rad_data{time_start.year}"
    )
    data_file_stop = os.path.join(
        pathing_dict["INTERRUPT_DIR"], "Data", f"rad_data{time_stop.year}"
    )
    #
    # --- Find data line for start
    #
    while data_start is None:
        try:
            data_start_search = f"grep -in '{time_start.strftime(_ACE_INPUT_DATA_TIME_FORMAT)}' {data_file_start}"
            data_start = int(subprocess.check_output(data_start_search, shell = True, executable = '/bin/csh').decode().split(":")[0])
        except subprocess.CalledProcessError as error:
            if error.returncode == 1:
                #
                # --- Could not find the data with that specific time
                #
                time_start += timedelta(minutes=5)
            elif error.returncode == 2:
                raise FileNotFoundError(f"{data_file_start}")
        if time_start.year == time_stop.year:
            raise ValueError(f"Cannot find start time line in {data_file_start}.")
    #
    # --- Find data line for stop
    #
    while data_stop is None:
        try:
            data_stop_search = f"grep -in '{time_stop.strftime(_ACE_INPUT_DATA_TIME_FORMAT)}' {data_file_stop}"
            data_stop = int(subprocess.check_output(data_stop_search, shell = True, executable = '/bin/csh').decode().split(":")[0])
        except subprocess.CalledProcessError as error:
            if error.returncode == 1:
                #
                # --- Could not find the data with that specific time
                #
                time_stop -= timedelta(minutes=5)
            elif error.returncode == 2:
                raise FileNotFoundError(f"{data_file_stop}")
        if time_stop.year == time_start.year:
            raise ValueError(f"Cannot find start time line in {data_file_stop}.")
    a = ascii.read(data_file_start, data_start = data_start)
    b = ascii.read(data_file_stop, data_end = data_stop)
    ace_table = vstack([a,b])
    return ace_table