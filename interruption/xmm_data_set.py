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
from cxotime import CxoTime
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
SPACE_WEATHER_DIR = "/data/mta4/Space_Weather"

_PATHING_DICT = {
    "WEB_DIR": WEB_DIR,
    "OUT_WEB_DIR": OUT_WEB_DIR,
    "WEB_DIR2": WEB_DIR2,
    "OUT_WEB_DIR2": OUT_WEB_DIR2,
    "SPACE_WEATHER_DIR": SPACE_WEATHER_DIR,
}  #: Dictionary of input and output file paths for collecting XMM interruption data.
_FETCH_INTERVAL = 2 #: Number of days before and after interruption period to fetch trending data from.
_XMM_FETCH_LINE_SIZE = 10000

#: Column format for input XMM radiation data archive.
#: 
#: **Weblink:** https://www.cosmos.esa.int/web/xmm-newton/radmon-details
_INPUT_XMM_COLUMNS = [
    "cxotime",
    "LE-0",
    "LE-1",
    "LE-2",
    "HES-0",
    "HES-1",
    "HES-2",
    "HES-C",
]

def xmm_data_set(event_data, pathing_dict):
    """Intakes data from the Space Weather XMM data archive in ``SPACE_WEATHER_DIR`` into an ``astropy.table`` and uses data for plotting and statistics.

    :param event_data: A dictionary which stores interruption data.
    :type event_data: dict(str, datetime or float or str)
    :param pathing_dict: A dictionary of file paths for storing file input and output.
    :type pathing_dict: dict(str, str)

    """
    print("XMM Data Set")
    time_start = event_data["tstart"] - timedelta(days=_FETCH_INTERVAL)
    time_stop = event_data["tstop"] + timedelta(days=_FETCH_INTERVAL)
    xmm_table = fetch_XMM_data(time_start, time_stop, pathing_dict)
    write_xmm_files(xmm_table, event_data, pathing_dict)
    plot_xmm_data(xmm_table, event_data, pathing_dict)

def fetch_XMM_data(time_start, time_stop, pathing_dict):
    """Fetch XMM data from the ``SPACE_WEATHER_DIR/XMM/Data/xmm.archive`` archive file and format into an astropy table.

    :param time_start: Starting time for data fetch, defaults to two days before the start of the interruption event.
    :type time_start: ``DateTime``
    :param time_stop: Stopping time for data fetch, defaults to two days after the start of the interruption event.
    :type time_stop: ``DateTime``
    :param pathing_dict: A dictionary of file paths for storing file input and output.
    :type pathing_dict: dict(str, str)
    :raises ValueError: If the ``event_data['tstart']`` starting time or ``event_data['tstop']`` stopping time data entires cannot be found in the Space Weather GOES data archive.
    :raises FileNotFoundError: If the ``SPACE_WEATHER_DIR/XMM/Data/xmm.archive`` file cannot be found.
    :return: Table of unique XMM data points spanning interruption event.
    :rtype: ``astropy.table.Table``

    """
    data_file = os.path.join(
        pathing_dict["SPACE_WEATHER_DIR"], "XMM", "Data", "xmm.archive"
    )
    #
    #--- Only read in the end of the file for the most recent data
    #
    contents = subprocess.check_output(f"tail -n {_XMM_FETCH_LINE_SIZE} {data_file}", shell=True, executable="/bin/csh").decode()
    xmm_table = ascii.read(contents, names = _INPUT_XMM_COLUMNS)
    sel = np.logical_and(xmm_table['cxotime']  >= CxoTime(time_start).secs, xmm_table['cxotime']  <= CxoTime(time_stop).secs)
    xmm_table = unique(xmm_table[sel])
    xmm_table['cxotime'] = Column([CxoTime(x) for x in xmm_table['cxotime'].data], name='cxotime')
    
    return xmm_table
    

def write_xmm_files(xmm_table, event_data, pathing_dict):
    pass

def plot_xmm_data(xmm_table, event_data, pathing_dict):
    pass

#
# --- Internal functions to assist cleanly formatting the input GOES table
#
