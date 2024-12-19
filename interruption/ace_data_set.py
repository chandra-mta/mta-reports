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
from astropy.table import vstack
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

#: Column format for input ACE radiation data archive written INTERRUPT_DIR.
#: 
#: **Note:** Status indicators: 0 = nominal, 4,6,7,8 = bad data, unable to process, 9 = no data, -1 = missing data
#: **Units:** KeV
_INPUT_ACE_COLUMNS = ["year",
                      "month",
                      "day",
                      "hhmm",
                      "mjd",
                      "daysecs",
                      "e_status",
                      "e_38-53",
                      "e_175-315",
                      "p_status",
                      "p_47-68",
                      "p_115-195",
                      "p_310-580",
                      "p_795-1193",
                      "p_1060-1900",
                      "aniso"
                     ]

def ace_data_set(event_data, pathing_dict):
    print("ACE Data Set")
    time_start = _round_down(event_data["tstart"]) - timedelta(days=2)
    time_stop = _round_down(event_data["tstop"]) + timedelta(days=2)
    ace_table = fetch_ACE_data_table(time_start, time_stop, pathing_dict)

def fetch_ACE_data_table(time_start, time_stop, pathing_dict):
    #
    # --- Check if interruption spans over the new year
    # --- and therefore need to construct ACE data table from two files.
    #
    if time_start.year == time_stop.year: # One Year File
        ace_table = _single_file_fetch(time_start, time_stop, pathing_dict)
    elif time_start.year != time_stop.year:  # Two Year Files
        ace_table = _double_file_fetch(time_start, time_stop, pathing_dict)
    #
    # --- Reformat Columns for easier processing
    #
    for i, col in enumerate(_INPUT_ACE_COLUMNS):
        ace_table.rename_column(f"col{i+1}", col)
    
    return ace_table

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
    #
    # --- Numbers in hundreds and thousands place is hours
    # --- while numbers in tens and ones place is 
    #
    hh = hhmm // 100
    mm = hhmm % 100
    return f"{year:04}:{month:02}:{day:02}:{hh:02}:{mm:02}"

def _single_file_fetch(time_start, time_stop, pathing_dict):
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