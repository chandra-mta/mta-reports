#!/proj/sot/ska3/flight/bin/python

"""
**goes_data_set.py** Extract GOES data, compute statistics, and plot values.

:Author: W. Aaron (william.aaron@cfa.harvard.edu)
:Last Updated: Dec 17 , 2024
:Note: This script is designed to be a submodule of **run_interruption.py**

"""

import os
import numpy as np
from datetime import datetime, timedelta
from cxotime import CxoTime, convert_time_format
from cheta import fetch
from kadi import events
from Ska.Matplotlib import plot_cxctime
import matplotlib.pyplot as plt
import shutil
from astropy.io import ascii
import subprocess
#
# --- Define Directory Pathing
#
WEB_DIR = "/data/mta_www/mta_interrupt"
OUT_WEB_DIR = "/data/mta_www/mta_interrupt"
WEB_DIR2 = "/data/mta4/www/RADIATION_new/mta_interrupt"
OUT_WEB_DIR2 = "/data/mta4/www/RADIATION_new/mta_interrupt"
SPACE_WEATHER_DIR = "/data/mta4/Space_Weather"

PATHING_DICT = {
    "WEB_DIR": WEB_DIR,
    "OUT_WEB_DIR": OUT_WEB_DIR,
    "WEB_DIR2": WEB_DIR2,
    "OUT_WEB_DIR2": OUT_WEB_DIR2,
    "SPACE_WEATHER_DIR": SPACE_WEATHER_DIR
}

GOES_DATA_TIME_FORMAT = "%Y:%j:%H:%M:%S"
GOES_CHANNEL_SELECT = ["P4", "P5", "P6", "HRC_Proxy"]

subhead = '\t\t'.join(GOES_CHANNEL_SELECT)
GOES_DATA_HEADER = f"Science Run Interruption: #LSTART\n\nTime\t\t{subhead}\n{'-'*67}\n"
GOES_STAT_HEADER = f"\t\tAvg\t\t\tMax\t\tTime\t\tMin\t\tTime\t\tValue at Interruption Started\n{'-'*95}\n"
TIME_FORMAT = "%Y:%m:%d:%H:%M"

def goes_data_set(event_data, pathing_dict):
    """Intakes data from the Space Weather GOES data archive in ``SPACE_WEATHER_DIR`` into an ``astropy.table`` and uses data for plotting and statistics.

    :param event_data: A dictionary which stores interruption data.
    :type event_data: dict(str, datetime or float or str)
    :param pathing_dict: A dictionary of file paths for storing file input and output.
    :type pathing_dict: dict(str, str)
    :raises ValueError: If the ``event_data['tstart']`` starting time or ``event_data['tstop']`` stopping time data entires cannot be found in the Space Weather GOES data archive.

    """
    data_file = os.path.join(pathing_dict['SPACE_WEATHER_DIR'], 'GOES', 'Data', 'goes_data_r.txt')
    #
    # --- Search the data_file via grep for the interruption time interval
    #
    time_start = (event_data['tstart'] - timedelta(days=2))
    time_stop = (event_data['tstop'] + timedelta(days=2))
    data_start = 0
    data_end = 0
    #
    # --- Find data line for start
    #
    while data_start == 0:
        try:
            data_start_search = f"grep -in '{time_start.strftime(GOES_DATA_TIME_FORMAT)}' {data_file}"
            data_start = int(subprocess.check_output(data_start_search, shell = True, executable = '/bin/csh').decode().split(":")[0])
        except subprocess.CalledProcessError:
            time_start += timedelta(minutes=5)
        if time_start > time_stop:
            raise ValueError(f"Cannot find start time line in {data_file}.")
    #
    # --- Find data line for stop
    #
    while data_end == 0:
        try:
            data_end_search = f"grep -in '{time_stop.strftime(GOES_DATA_TIME_FORMAT)}' {data_file}"
            data_end = int(subprocess.check_output(data_end_search, shell = True, executable = '/bin/csh').decode().split(":")[0])
        except subprocess.CalledProcessError:
            time_stop -= timedelta(minutes=5)
        if time_stop < time_start:
            raise ValueError(f"Cannot find stop time line in {data_file}.")
    #
    # --- Once the data indices have been found, load that selection into an astropy table
    #
    goes_table = ascii.read(data_file, data_start = data_start - 3, data_end = data_end - 2)
    write_goes_files(goes_table, event_data, pathing_dict)

def write_goes_files(goes_table, event_data, pathing_dict):
    """Write GOES data and statistics to human-reference text file.

    :param goes_table: GOES data table read from ``SPACE_WEATHER_DIR/GOES/Data``.
    :type goes_table: astropy.table.Table
    :param event_data: A dictionary which stores interruption data.
    :type event_data: dict(str, datetime or float or str)
    :param pathing_dict: A dictionary of file paths for storing file input and output.
    :type pathing_dict: dict(str, str)
    :File Out: Writes the ``<event_name>_hrc.txt`` data table to the two ``OUT_WEB_DIR/Data_dir`` directories, 
        and writes the ``<event_name>_hrc_stat`` statistics table to the two ``OUT_WEB_DIR/Stat_dir`` directories.

    """
    #
    # --- Write Data File
    #
    line = GOES_DATA_HEADER.replace("#LSTART",event_data['tstart'].strftime(TIME_FORMAT))
    for row in goes_table:
        substring = f"{row['Time']}\t\t"
        subset = []
        for channel in GOES_CHANNEL_SELECT:
            if channel == 'HRC_Proxy':
                substring += f"{row[channel]}"
            else:
                substring += f"{row[channel]:.3e}\t\t"
        line += f"{substring}\n"
    
    ifile = os.path.join(pathing_dict['OUT_WEB_DIR'], 'Data_dir', f"{event_data['name']}_goes.txt")
    ifile2 = os.path.join(pathing_dict['OUT_WEB_DIR2'], 'Data_dir', f"{event_data['name']}_goes.txt")
    os.makedirs(os.path.dirname(ifile), exist_ok= True)
    os.makedirs(os.path.dirname(ifile2), exist_ok= True)
    with open(ifile,'w') as f:
        f.write(line)
    if ifile != ifile2:
        shutil.copy(ifile,ifile2)
    
    #
    # --- Write Stat File.
    #
    sel = goes_table['Time'] == event_data['tstart'].strftime(GOES_DATA_TIME_FORMAT)
    interrupt_row = goes_table[sel][0]
    line = GOES_STAT_HEADER
    for channel in GOES_CHANNEL_SELECT:
        avg = np.mean(goes_table[channel].data)
        std = np.std(goes_table[channel].data)

        maxidx = np.argmax(goes_table[channel].data)
        max = goes_table[channel][maxidx]
        maxtime = goes_table['Time'][maxidx]

        minidx = np.argmin(goes_table[channel].data)
        min = goes_table[channel][minidx]
        mintime = goes_table['Time'][minidx]
        
        val_intt = interrupt_row[channel]
        
        line += f"{channel}\t\t{avg:.3e}+/-{std:.3e}\t{max:.3e}\t{maxtime}\t{min:.3e}\t{mintime}\t{val_intt}\n"

    ifile = os.path.join(pathing_dict['OUT_WEB_DIR'], 'Stat_dir', f"{event_data['name']}_goes_stat")
    ifile2 = os.path.join(pathing_dict['OUT_WEB_DIR2'], 'Stat_dir', f"{event_data['name']}_goes_stat")
    os.makedirs(os.path.dirname(ifile), exist_ok= True)
    os.makedirs(os.path.dirname(ifile2), exist_ok= True)
    with open(ifile,'w') as f:
        f.write(line)
    if ifile != ifile2:
        shutil.copy(ifile,ifile2)
