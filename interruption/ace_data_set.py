#!/proj/sot/ska3/flight/bin/python


"""
**ace_data_set.py** Extract ACE data, compute statistics, and plot values.

:Author: W. Aaron (william.aaron@cfa.harvard.edu)
:Last Updated: Dec 17, 2024
:Note: This script is designed to be a submodule of **run_interruption.py**

"""

import os
import numpy as np
from cxotime import CxoTime
from datetime import datetime, timedelta
from kadi.events import rad_zones
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from Ska.Matplotlib import plot_cxctime
import shutil
from astropy.io import ascii
from astropy.table import vstack, Column, unique
import subprocess

#
# --- Define Directory Pathing
#
WEB_DIR = "/data/mta_www/mta_interrupt"
OUT_WEB_DIR = "/data/mta_www/mta_interrupt"
INTERRUPT_DIR = "/data/mta/Script/Interrupt"
ACE_DIR = "/data/mta4/Space_Weather/ACE"

PATHING_DICT = {
    "WEB_DIR": WEB_DIR,
    "OUT_WEB_DIR": OUT_WEB_DIR,
    "INTERRUPT_DIR": INTERRUPT_DIR,
    "ACE_DIR": ACE_DIR
}
_FETCH_INTERVAL = 2 #: Number of days before and after interruption period to fetch trending data from.
_ACE_INPUT_DATA_TIME_FORMAT = "%Y %m %d  %H%M"  #: Time format for input ACE radiation data archive written INTERRUPT_DIR.
_ACE_DATA_TIME_FORMAT = "%Y:%j:%H:%M:%S"  #: Time format for human-reference files.

#: Column format for input ACE radiation data archive written INTERRUPT_DIR.
#:
#: **Note:** Status indicators: 0 = nominal, 4,6,7,8 = bad data, unable to process, 9 = no data, -1 = missing data
#:
#: **Units:** KeV
_INPUT_ACE_COLUMNS = [
    "year",
    "month",
    "day",
    "hhmm",
    "mjd",
    "daysecs",
    "e_status",
    "e38-53",
    "e175-315",
    "p_status",
    "p47-68",
    "p115-195",
    "p310-580",
    "p795-1193",
    "p1060-1900",
    "aniso",
    "intp112_187",
    'flu_112_187'
]

#
# --- File heading information
#
_ACE_CHANNEL_SELECT = [
    "e38-53",
    "e175-315",
    "p47-68",
    "p115-195",
    "p310-580",
    "p795-1193",
    "p1060-1900",
    "aniso",
]  #: Selection of ACE table channels for the human-reference text file
_subhead = "\t\t".join(_ACE_CHANNEL_SELECT)
_ACE_DATA_HEADER = (
    f"Science Run Interruption: #LSTART\n\nTime\t\t{_subhead}\n{'-'*100}\n"
)
_ACE_STAT_HEADER = f"\t\tAvg\t\t\tMax\t\tTime\t\tMin\t\tTime\t\tValue at Start of Interruption\n{'-'*95}\n"

#
# --- Plotting Globals
#
_ELECTRON_CHANNEL_SELECT = [
    "e38-53",
    "e175-315",
]
_PROTON_CHANNEL_SELECT = [
    "p47-68",
    "p115-195",
    "p310-580",
    "p795-1193",
    "p1060-1900",
]
_ELECTRON_PLOT_KWARGS = [
    {'color':'red'},
    {'color':'blue'},
]

_PROTON_PLOT_KWARGS = [
    {'color':'red'},
    {'color':'blue'},
    {'color':'green'},
    {'color':'aqua'},
    {'color':'teal'},
]

def ace_data_set(event_data, pathing_dict):
    """Intakes data from the ACE data archive in ``INTERRUPT_DIR`` into an ``astropy.table`` and uses data for plotting and statistics.

    :param event_data: A dictionary which stores interruption data.
    :type event_data: dict(str, cxotime or float or str)
    :param pathing_dict: A dictionary of file paths for storing file input and output.
    :type pathing_dict: dict(str, str)

    """
    print("ACE Data Set")
    time_start = _round_down(event_data["tstart"]) - timedelta(days=_FETCH_INTERVAL)
    time_stop = _round_down(event_data["tstop"]) + timedelta(days=_FETCH_INTERVAL)
    ace_table = fetch_ACE_data_table(time_start, time_stop, pathing_dict)
    write_ace_files(ace_table, event_data, pathing_dict)
    plot_ace_data(ace_table, event_data, pathing_dict)


def fetch_ACE_data_table(time_start, time_stop, pathing_dict):
    """Fetch ACE data from the ``ACE_DIR/Data`` archive files and format into an astropy table.

    :param time_start: Starting time for data fetch, defaults to two days before the start of the interruption event.
    :type time_start: ``CxoTime``
    :param time_stop: Stopping time for data fetch, defaults to two days after the start of the interruption event.
    :type time_stop: ``CxoTime``
    :param pathing_dict: A dictionary of file paths for storing file input and output.
    :type pathing_dict: dict(str, str)
    :return: Table of unique ACE data points spanning interruption event.
    :rtype: ``astropy.table.Table``
    :Note: While algorithmically very similar to the data fetch performed in the :mod:`~interruption.goes_data_set` script,
        this table stores the time column as a ``CxoTime`` object rather than a string to reduce computation due to how ACE archive data files are stored.

    """
    #
    # --- Check if the interruption spans over the new year
    # --- and therefore need to construct ACE data table from two files.
    #
    ace_table = _single_file_fetch(time_start, time_stop, pathing_dict)
    #
    # --- Format complete time column
    #
    datetime_col = Column(
        _convert_time_format(
            ace_table["year"], ace_table["month"], ace_table["day"], ace_table["hhmm"]
        ),
        name="cxotime",
    )
    ace_table.add_column(datetime_col)
    ace_table = unique(ace_table, keys="cxotime")
    ace_table.reverse() #: Reversed as the ace.archive file is stored in reverse.
    return ace_table


def write_ace_files(ace_table, event_data, pathing_dict):
    """Write ACE data and statistics to human-reference text file.

    :param ace_table: ACE data table read from :func:`~interruption.ace_data_set.fetch_ace_data`.
    :type ace_table: astropy.table.Table
    :param event_data: A dictionary which stores interruption data.
    :type event_data: dict(str, cxotime or float or str)
    :param pathing_dict: A dictionary of file paths for storing file input and output.
    :type pathing_dict: dict(str, str)
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
        substring = f"{str(row['cxotime'].date).split('.')[0]}\t\t"
        for channel in _ACE_CHANNEL_SELECT:
            if channel == _ACE_CHANNEL_SELECT[-1]:
                substring += f"{row[channel]}"
            else:
                substring += f"{row[channel]:.3e}\t\t"
        line += f"{substring}\n"

    ifile = os.path.join(
        pathing_dict["OUT_WEB_DIR"], "Data_dir", f"{event_data['name']}_ace.txt"
    )
    os.makedirs(os.path.dirname(ifile), exist_ok=True)
    with open(ifile, "w") as f:
        f.write(line)

    #
    # --- Write Stat File.
    #
    sel = ace_table["cxotime"] == _round_down(event_data["tstart"])
    interrupt_row = ace_table[sel][0]
    line = _ACE_STAT_HEADER
    #
    # --- Channel Stats
    #
    for channel in _ACE_CHANNEL_SELECT[:-1]:
        avg = np.mean(ace_table[channel].data)
        std = np.std(ace_table[channel].data)

        maxidx = np.argmax(ace_table[channel].data)
        max = ace_table[channel][maxidx]
        maxtime = ace_table["cxotime"][maxidx].strftime(_ACE_DATA_TIME_FORMAT)

        sel = ace_table[channel].data >= 0
        minidx = np.argmin(ace_table[channel].data[sel])
        min = ace_table[channel][sel][minidx]
        mintime = ace_table["cxotime"][sel][minidx].strftime(_ACE_DATA_TIME_FORMAT)

        val_intt = interrupt_row[channel]

        line += f"{channel}\t\t{avg:.3e}+/-{std:.3e}\t{max:.3e}\t{maxtime}\t{min:.3e}\t{mintime}\t{val_intt}\n"
    #
    # --- Ratio Stats
    # --- define ratios by picking the largest / last selected proton and electron channels
    #
    e_chan = _ELECTRON_CHANNEL_SELECT[-1]
    p_chan = _PROTON_CHANNEL_SELECT[-1]
    for channel in _ELECTRON_CHANNEL_SELECT[:-1]:
        #
        # --- Compute ratio, but map inf values to nan values due to division by zero
        # --- Then run numpy functions to ignore nan values
        #
        with np.errstate(divide='ignore'):
            data_set = ace_table[channel].data / ace_table[e_chan].data
            data_set = np.where(np.isinf(data_set), np.nan, data_set)

            avg = np.nanmean(data_set)
            std = np.nanstd(data_set)

            maxidx = np.nanargmax(data_set)
            max = data_set[maxidx]
            maxtime = ace_table["cxotime"][maxidx].strftime(_ACE_DATA_TIME_FORMAT)

            sel = data_set >= 0
            minidx = np.nanargmin(data_set[sel])
            min = data_set[sel][minidx]
            mintime = ace_table["cxotime"][sel][minidx].strftime(_ACE_DATA_TIME_FORMAT)

            val_intt = interrupt_row[channel] / interrupt_row[e_chan]

            line += f"{channel}/{e_chan}\t\t{avg:.3e}+/-{std:.3e}\t{max:.3e}\t{maxtime}\t{min:.3e}\t{mintime}\t{val_intt}\n"

    for channel in _PROTON_CHANNEL_SELECT[:-1]:
        #
        # --- Compute ratio, but map inf values to nan values due to division by zero
        # --- Then run numpy functions to ignore nan values
        #
        with np.errstate(divide='ignore'):
            data_set = ace_table[channel].data / ace_table[e_chan].data
            data_set = np.where(np.isinf(data_set), np.nan, data_set)

            avg = np.nanmean(data_set)
            std = np.nanstd(data_set)

            maxidx = np.nanargmax(data_set)
            max = data_set[maxidx]
            maxtime = ace_table["cxotime"][maxidx].strftime(_ACE_DATA_TIME_FORMAT)

            sel = data_set >= 0
            minidx = np.nanargmin(data_set[sel])
            min = data_set[sel][minidx]
            mintime = ace_table["cxotime"][sel][minidx].strftime(_ACE_DATA_TIME_FORMAT)

            val_intt = interrupt_row[channel] / interrupt_row[p_chan]

            line += f"{channel}/{p_chan}\t\t{avg:.3e}+/-{std:.3e}\t{max:.3e}\t{maxtime}\t{min:.3e}\t{mintime}\t{val_intt}\n"

    ifile = os.path.join(
        pathing_dict["OUT_WEB_DIR"], "Stat_dir", f"{event_data['name']}_ace_stat"
    )
    os.makedirs(os.path.dirname(ifile), exist_ok=True)
    with open(ifile, "w") as f:
        f.write(line)

def plot_ace_data(ace_table, event_data, pathing_dict):
    """Create a plot of ACE channel data.

    :param ace_table: ACE data table read from :func:`~interruption.ace_data_set.fetch_ace_data`.
    :type ace_table: astropy.table.Table
    :param event_data: A dictionary which stores interruption data.
    :type event_data: dict(str, cxotime or float or str)
    :param pathing_dict: A dictionary of file paths for storing file input and output.
    :type pathing_dict: dict(str, str)
    :File Out: Writes the ``<event_name>_ace.png`` plots to the two ``OUT_WEB_DIR/ACE_plot`` directories.

    """
    plt.close('all')
    zones = rad_zones.filter(
        start=event_data["tstart"] - timedelta(days=2),
        stop=event_data["tstop"] + timedelta(days=2),
    ).table
    fig = plt.figure(figsize=(10, 6.7))
    fig.clf()

    times = ace_table["cxotime"]
    #
    # --- Reference Information
    #
    ylab = "Log$_{10}$"
    date_format = mdates.DateFormatter("%j")
    deltatime = event_data["tstop"].datetime - event_data["tstart"].datetime
    ymin = 1
    ymax = 6
    int_label = 6.2
    plt.subplots_adjust(hspace=0.25)
    #
    # --- Electron set
    #
    ax1 = fig.add_subplot(2, 1, 1)
    ax1.xaxis.set_major_formatter(date_format)
    ax1.set_ylim(ymin, ymax, auto=False)
    ax1.set_ylabel(f"{ylab}(#e/cm2-q-sr-KeV) Rate)", fontsize=9)
    ax1.grid()
    
    for label in ax1.get_xticklabels():
                label.set_visible(False)
    for i, channel in enumerate(_ELECTRON_CHANNEL_SELECT):
        #
        # --- Map values to a logarithmic scale, deselecting missing or values of zero
        #
        m = ace_table[channel].data
        mapped_vals = np.log10(m, out=np.zeros_like(m, dtype=float), where=(m > 0))
        sel = mapped_vals != 0
        plot_cxctime(times[sel], mapped_vals[sel], lw = 1, **_ELECTRON_PLOT_KWARGS[i])
    #
    #--- Legend
    #
    leg1 = plt.legend([x.capitalize() for x in _ELECTRON_CHANNEL_SELECT], loc='upper left', fontsize=8)
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
    # --- Proton set
    #
    ax2 = fig.add_subplot(2, 1, 2, sharex=ax1)
    ax2.xaxis.set_major_formatter(date_format)
    ax2.set_xlabel("Day of Year", fontsize=9)
    ax2.set_ylim(ymin, ymax, auto=False)
    ax2.set_ylabel(f"{ylab}(#p/cm2-q-sr-KeV) Rate)", fontsize=9)
    ax2.grid()
    for i, channel in enumerate(_PROTON_CHANNEL_SELECT):
        #
        # --- Map values to a logarithmic scale, deselecting missing or values of zero
        #
        m = ace_table[channel].data
        mapped_vals = np.log10(m, out=np.zeros_like(m, dtype=float), where=(m > 0))
        sel = mapped_vals != 0
        plot_cxctime(times[sel], mapped_vals[sel], lw = 1, **_PROTON_PLOT_KWARGS[i])
    #
    #--- Legend
    #
    leg2 = plt.legend([x.capitalize() for x in _PROTON_CHANNEL_SELECT], loc='upper left', fontsize=8)
    leg2.get_frame().set_alpha(0.5)
    #
    # --- Plot Indicator Lines
    #
    plt.axvline(event_data["tstart"].datetime, color="red", lw=2)  #: Event Start
    plt.axvline(event_data["tstop"].datetime, color="red", lw=2)  #: Event Ending
    plt.axhline(np.log10(3.6e8), color = 'black', ls='--') #: ACE P3 alert limit
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
        pathing_dict["OUT_WEB_DIR"], "ACE_plot", f"{event_data['name']}_ace.png"
    )
    os.makedirs(os.path.dirname(ofile), exist_ok=True)
    plt.savefig(ofile, format="png", dpi=300)

#
# --- Internal functions to assist cleanly formatting the input ACE table from text to astropy table
#
def _round_down(dt):
    """Round a CxoTime object down to the nearest five minutes. For use in fetching from data files.

    :param dt: A ``CxoTime`` object of any kind
    :type dt: CxoTime
    :return: The input ``CxoTime`` object rounded down to the nearest five minutes.
    :rtype: CxoTime

    """
    dt = dt.datetime
    delta_min = dt.minute % 5
    return CxoTime(datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute - delta_min))


@np.vectorize
def _convert_time_format(year, month, day, hhmm):
    """Converts separated ``numpy.ndarray`` containing date information into an array of ``CxoTime`` objects.

    :param year: Four digit year
    :type year: int
    :param month: Month
    :type month: int
    :param day: Day
    :type day: int
    :param hhmm: Integer Combining Hours and Minutes
    :type hhmm: int
    :return: ``numpy.ndarray`` of ``CxoTime`` objects.
    :rtype: ``numpy.ndarray(dtype = 'object')``

    """
    hh = hhmm // 100  #: hours in hundreds and thousands place
    mm = hhmm % 100  #: minutes in tens and ones place
    return CxoTime(datetime.strptime(
        f"{year:04}:{month:02}:{day:02}:{hh:02}:{mm:02}", "%Y:%m:%d:%H:%M"
    ))


def _single_file_fetch(time_start, time_stop, pathing_dict):
    """Fetch ACE data from a single archive file.

    :param time_start: Starting time for data fetch, defaults to two days before the start of the interruption event.
    :type time_start: ``CxoTime``
    :param time_stop: Stopping time for data fetch, defaults to two days after the start of the interruption event.
    :type time_stop: ``CxoTime``
    :param pathing_dict: A dictionary of file paths for storing file input and output.
    :type pathing_dict: dict(str, str)
    :raises ValueError: If the starting or stopping time line of data cannot be found in the data archive.
    :raises FileNotFoundError: If the data archive file cannot be found.
    :return: Table of unique ACE data points spanning interruption event.
    :rtype: ``astropy.table.Table``

    """
    line_num_start = None
    line_num_stop = None
    data_file = os.path.join(
        pathing_dict["ACE_DIR"], "Data", "ace.archive"
    )
    #
    # --- Find data line for start
    #
    while line_num_start is None:
        try:
            line_num_start_search = f"grep -in '{time_start.strftime(_ACE_INPUT_DATA_TIME_FORMAT)}' {data_file}"
            line_num_start = int(
                subprocess.check_output(
                    line_num_start_search, shell=True, executable="/bin/csh"
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
    while line_num_stop is None:
        try:
            line_num_stop_search = f"grep -in '{time_stop.strftime(_ACE_INPUT_DATA_TIME_FORMAT)}' {data_file}"
            line_num_stop = int(
                subprocess.check_output(
                    line_num_stop_search, shell=True, executable="/bin/csh"
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
    #
    # --- ace.archive stores data in reverse time order. So 'start' is a later line number
    #
    ace_table = ascii.read(data_file, data_start=line_num_stop, data_end=line_num_start, names = _INPUT_ACE_COLUMNS)
    return ace_table


def _double_file_fetch(time_start, time_stop, pathing_dict):
    """Fetch ACE data from a two archive files. For use in the event an interruption spans across the new year.

    :param time_start: Starting time for data fetch, defaults to two days before the start of the interruption event.
    :type time_start: ``CxoTime``
    :param time_stop: Stopping time for data fetch, defaults to two days after the start of the interruption event.
    :type time_stop: ``CxoTime``
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
        pathing_dict["INTERRUPT_DIR"], "Data", f"rad_data{time_start.datetime.year}"
    )
    data_file_stop = os.path.join(
        pathing_dict["INTERRUPT_DIR"], "Data", f"rad_data{time_stop.datetime.year}"
    )
    #
    # --- Find data line for start
    #
    while data_start is None:
        try:
            data_start_search = f"grep -in '{time_start.strftime(_ACE_INPUT_DATA_TIME_FORMAT)}' {data_file_start}"
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
                raise FileNotFoundError(f"{data_file_start}")
        if time_start.datetime.year == time_stop.datetime.year:
            raise ValueError(f"Cannot find start time line in {data_file_start}.")
    #
    # --- Find data line for stop
    #
    while data_stop is None:
        try:
            data_stop_search = f"grep -in '{time_stop.strftime(_ACE_INPUT_DATA_TIME_FORMAT)}' {data_file_stop}"
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
                raise FileNotFoundError(f"{data_file_stop}")
        if time_stop.datetime.year == time_start.datetime.year:
            raise ValueError(f"Cannot find start time line in {data_file_stop}.")
    a = ascii.read(data_file_start, data_start=data_start, names = _INPUT_ACE_COLUMNS)
    b = ascii.read(data_file_stop, data_end=data_stop, names = _INPUT_ACE_COLUMNS)
    ace_table = vstack([a, b])
    return ace_table
