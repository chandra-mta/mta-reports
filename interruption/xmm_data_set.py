#!/proj/sot/ska3/flight/bin/python
"""
**xmm_data_set.py** Extract XMM data, compute statistics, and plot values.

:Author: W. Aaron (william.aaron@cfa.harvard.edu)
:Last Updated: Jan 25, 2025
:Note: This script is designed to be a submodule of **run_interruption.py**

"""
import os
import numpy as np
from datetime import datetime, timedelta
from kadi.events import rad_zones
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from cxotime import CxoTime
from Ska.Matplotlib import plot_cxctime
import shutil
from astropy.io import ascii
from astropy.table import Column, unique
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
_XMM_DATA_TIME_FORMAT = "%Y:%j:%H:%M:%S"  #: Time format for human-reference files.

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
#
# --- File heading information
#
_XMM_CHANNEL_SELECT = [
    "LE-0",
    "LE-1",
    "LE-2",
    "HES-0",
    "HES-1",
    "HES-2",
    "HES-C",
]  #: Selection of XMM table channels for the human-reference text file

_subhead = "\t\t".join(_XMM_CHANNEL_SELECT)
_XMM_DATA_HEADER = (
    f"Science Run Interruption: #LSTART\n\nTime\t\t{_subhead}\n{'-'*100}\n"
)
_XMM_STAT_HEADER = f"\t\tAvg\t\t\tMax\t\tTime\t\tMin\t\tTime\t\tValue at Start of Interruption\n{'-'*95}\n"

#
# --- Plotting Globals
#
_LOW_ENERGY_CHANNEL_SELECT = [
    "LE-0",
    "LE-1",
    "LE-2",
] #: Selection of XMM table channels for the low energy plot
_HIGH_ENERGY_CHANNEL_SELECT = [
    "HES-0",
    "HES-1",
    "HES-2",
    "HES-C",
] #: Selection of XMM table channels for the high energy plot

_LOW_ENERGY_PLOT_KWARGS = [
    {'color':'red'},
    {'color':'blue'},
    {'color':'green'},
]

_HIGH_ENERGY_PLOT_KWARGS = [
    {'color':'red'},
    {'color':'blue'},
    {'color':'green'},
    {'color':'aqua'},
]

def xmm_data_set(event_data, pathing_dict):
    """Intakes data from the Space Weather XMM data archive in ``SPACE_WEATHER_DIR`` into an ``astropy.table`` and uses data for plotting and statistics.

    :param event_data: A dictionary which stores interruption data.
    :type event_data: dict(str, cxotime or float or str)
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
    :type time_start: ``CxoTime``
    :param time_stop: Stopping time for data fetch, defaults to two days after the start of the interruption event.
    :type time_stop: ``CxoTime``
    :param pathing_dict: A dictionary of file paths for storing file input and output.
    :type pathing_dict: dict(str, str)
    :raises ValueError: If the ``event_data['tstart']`` starting time or ``event_data['tstop']`` stopping time data entires cannot be found in the Space Weather XMM data archive.
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
    sel = np.logical_and(xmm_table['cxotime']  >= time_start.secs, xmm_table['cxotime']  <= time_stop.secs)
    xmm_table = unique(xmm_table[sel])
    xmm_table['cxotime'] = Column([CxoTime(x) for x in xmm_table['cxotime'].data], name='cxotime')
    
    return xmm_table
    

def write_xmm_files(xmm_table, event_data, pathing_dict):
    """Write XMM data and statistics to human-reference text file.

    :param xmm_table: XMM data table read from :func:`~interruption.xmm_data_set.fetch_xmm_data`.
    :type xmm_table: astropy.table.Table
    :param event_data: A dictionary which stores interruption data.
    :type event_data: dict(str, cxotime or float or str)
    :param pathing_dict: A dictionary of file paths for storing file input and output.
    :type pathing_dict: dict(str, str)
    :File Out: Writes the ``<event_name>_xmm.txt`` data table to the two ``OUT_WEB_DIR/Data_dir`` directories,
        and writes the ``<event_name>_xmm_stat`` statistics table to the two ``OUT_WEB_DIR/Stat_dir`` directories.
    
    """
    #
    # --- Write Data File
    #
    line = _XMM_DATA_HEADER.replace(
        "#LSTART", event_data["tstart"].strftime("%Y:%m:%d:%H:%M")
    )
    for row in xmm_table:
        substring = f"{str(row['cxotime'].date).split('.')[0]}\t\t"
        for channel in _XMM_CHANNEL_SELECT:
            if channel == _XMM_CHANNEL_SELECT[-1]:
                substring += f"{row[channel]}"
            else:
                substring += f"{row[channel]:.3e}\t\t"
        line += f"{substring}\n"
    ifile = os.path.join(
        pathing_dict["OUT_WEB_DIR"], "Data_dir", f"{event_data['name']}_xmm.txt"
    )
    ifile2 = os.path.join(
        pathing_dict["OUT_WEB_DIR2"], "Data_dir", f"{event_data['name']}_xmm.txt"
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
    sel, closest_value = _find_closest_time(xmm_table["cxotime"].data, event_data['tstart'])
    interrupt_row = xmm_table[sel]
    line = _XMM_STAT_HEADER
    for channel in _XMM_CHANNEL_SELECT[:-1]:
        avg = np.mean(xmm_table[channel].data)
        std = np.std(xmm_table[channel].data)

        maxidx = np.argmax(xmm_table[channel].data)
        max = xmm_table[channel][maxidx]
        maxtime = xmm_table["cxotime"][maxidx].strftime(_XMM_DATA_TIME_FORMAT)

        sel = xmm_table[channel].data >= 0
        minidx = np.argmin(xmm_table[channel].data[sel])
        min = xmm_table[channel][minidx]
        mintime = xmm_table["cxotime"][minidx].strftime(_XMM_DATA_TIME_FORMAT)

        val_intt = interrupt_row[channel]

        line += f"{channel}\t\t{avg:.3e}+/-{std:.3e}\t{max:.3e}\t{maxtime}\t{min:.3e}\t{mintime}\t{val_intt}\n"

    ifile = os.path.join(
        pathing_dict["OUT_WEB_DIR"], "Stat_dir", f"{event_data['name']}_xmm_stat"
    )
    ifile2 = os.path.join(
        pathing_dict["OUT_WEB_DIR2"], "Stat_dir", f"{event_data['name']}_xmm_stat"
    )
    os.makedirs(os.path.dirname(ifile), exist_ok=True)
    os.makedirs(os.path.dirname(ifile2), exist_ok=True)
    with open(ifile, "w") as f:
        f.write(line)
    if ifile != ifile2:
        shutil.copy(ifile, ifile2)

def plot_xmm_data(xmm_table, event_data, pathing_dict):
    """Create a plot of XMM channel data.

    :param xmm_table: XMM data table read from :func:`~interruption.xmm_data_set.fetch_xmm_data`.
    :type xmm_table: astropy.table.Table
    :param event_data: A dictionary which stores interruption data.
    :type event_data: dict(str, cxotime or float or str)
    :param pathing_dict: A dictionary of file paths for storing file input and output.
    :type pathing_dict: dict(str, str)
    :File Out: Writes the ``<event_name>_xmm.png`` plots to the two ``OUT_WEB_DIR/XMM_plot`` directories.

    """
    plt.close('all')
    zones = rad_zones.filter(
        start=event_data["tstart"] - timedelta(days=2),
        stop=event_data["tstop"] + timedelta(days=2),
    ).table
    fig = plt.figure(figsize=(10, 6.7))
    fig.clf()

    times = xmm_table["cxotime"]
    #
    # --- Reference Information
    #
    date_format = mdates.DateFormatter("%j")
    deltatime = event_data["tstop"].datetime - event_data["tstart"].datetime
    ymin = 0.05
    ymax = 50000
    int_label = 60000
    plt.subplots_adjust(hspace=0.25)
    #
    # --- Low Energy (LE) proton and electron unit
    #
    ax1 = fig.add_subplot(2, 1, 1)
    ax1.xaxis.set_major_formatter(date_format)
    ax1.set_ylim(ymin, ymax, auto=False)
    ax1.set_ylabel(f"counts/sec", fontsize=10)
    ax1.set_yscale('log')
    ax1.grid()
    for label in ax1.get_xticklabels():
                label.set_visible(False)
    for i, channel in enumerate(_LOW_ENERGY_CHANNEL_SELECT):
        plot_cxctime(times, xmm_table[channel].data, lw = 1, **_LOW_ENERGY_PLOT_KWARGS[i])
    #
    #--- Legend
    #
    leg1 = plt.legend(_LOW_ENERGY_CHANNEL_SELECT, loc='upper left', fontsize=8)
    leg1.get_frame().set_alpha(0.5)
    #
    # --- Plot Indicator Lines
    #
    plt.axvline(event_data["tstart"].datetime, color="red", lw=2)  #: Event Start
    plt.axvline(event_data["tstop"].datetime, color="red", lw=2)  #: Event Ending
    #
    # --- Plot Labels
    #
    plt.text(
        event_data["tstart"].datetime + (0.025 * deltatime),
        int_label,
        r"Interruption",
        color="red",
    )  #: Interruption Marker
    #
    # --- Plot of radiation zones
    #
    for row in zones:
        start = datetime.strptime(str(row["start"]).split(".")[0], "%Y:%j:%H:%M:%S")
        stop = datetime.strptime(str(row["stop"]).split(".")[0], "%Y:%j:%H:%M:%S")
        plt.plot(
            [start, stop], [ymin, ymin], color="purple", lw=8
        )  #: Radiation Zone
    #
    # --- High Energy (HE) particle unit
    #
    ax2 = fig.add_subplot(2, 1, 2, sharex=ax1)
    ax2.xaxis.set_major_formatter(date_format)
    ax2.set_xlabel("Day of Year", fontsize=9)
    ax2.set_ylim(ymin, ymax, auto=False)
    ax2.set_ylabel(f"counts/sec", fontsize=9)
    ax2.set_yscale('log')
    ax2.grid()
    for i, channel in enumerate(_HIGH_ENERGY_CHANNEL_SELECT):
        plot_cxctime(times, xmm_table[channel].data, lw = 1, **_HIGH_ENERGY_PLOT_KWARGS[i])
    #
    #--- Legend
    #
    leg2 = plt.legend(_HIGH_ENERGY_CHANNEL_SELECT, loc='upper left', fontsize=8)
    leg2.get_frame().set_alpha(0.5)
    #
    # --- Plot Indicator Lines
    #
    plt.axvline(event_data["tstart"].datetime, color="red", lw=2)  #: Event Start
    plt.axvline(event_data["tstop"].datetime, color="red", lw=2)  #: Event Ending
    #
    # --- Plot Labels
    #
    plt.text(
        event_data["tstart"].datetime + (0.025 * deltatime),
        int_label,
        r"Interruption",
        color="red",
    )  #: Interruption Marker
    #
    # --- Plot of radiation zones
    #
    for row in zones:
        start = datetime.strptime(str(row["start"]).split(".")[0], "%Y:%j:%H:%M:%S")
        stop = datetime.strptime(str(row["stop"]).split(".")[0], "%Y:%j:%H:%M:%S")
        plt.plot(
            [start, stop], [ymin, ymin], color="purple", lw=8
        )  #: Radiation Zone
    ofile = os.path.join(
        pathing_dict["OUT_WEB_DIR"], "XMM_plot", f"{event_data['name']}_xmm.png"
    )
    ofile2 = os.path.join(
        pathing_dict["OUT_WEB_DIR2"], "XMM_plot", f"{event_data['name']}_xmm.png"
    )
    os.makedirs(os.path.dirname(ofile), exist_ok=True)
    os.makedirs(os.path.dirname(ofile2), exist_ok=True)
    plt.savefig(ofile, format="png", dpi=300)
    plt.savefig(ofile2, format="png", dpi=300)

#
# --- Internal functions to assist cleanly formatting the input GOES table
#
def _find_closest_time(array, value):
    """Finds the closest time value in the array to the given value.

    :param array: Array for finding closest time.
    :type array: np.ndarray of cxotime
    :param value: Target Value.
    :type value: np.ndarray.dtype
    :return: Index and value of closest value.
    :rtype: (int, np.ndarray.dtype)
    """
    check = np.zeros(len(array))
    for i in range(len(array)):
        check[i] = np.abs((array[i].datetime - value.datetime).seconds)
    idx = check.argmin()
    return idx , array[idx]