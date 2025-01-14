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
from cxotime import CxoTime
from Ska.Matplotlib import plot_cxctime
from kadi.events import rad_zones
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
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

_PATHING_DICT = {
    "WEB_DIR": WEB_DIR,
    "OUT_WEB_DIR": OUT_WEB_DIR,
    "WEB_DIR2": WEB_DIR2,
    "OUT_WEB_DIR2": OUT_WEB_DIR2,
    "SPACE_WEATHER_DIR": SPACE_WEATHER_DIR,
}  #: Dictionary of input and output file paths for collecting GOES interruption data.
_FETCH_INTERVAL = 2 #: Number of days before and after interruption period to fetch trending data from.
_GOES_DATA_TIME_FORMAT = "%Y:%j:%H:%M:%S"  #: Conversion format between file archive time to ``cxotime`` objects.
_GOES_CHANNEL_SELECT = [
    "P4",
    "P5",
    "P6",
    "HRC_Proxy",
]  #: Selection of GOES-R channels of interest.

#
# --- File heading information
#
_subhead = "\t\t".join(_GOES_CHANNEL_SELECT)
_GOES_DATA_HEADER = (
    f"Science Run Interruption: #LSTART\n\nTime\t\t{_subhead}\n{'-'*67}\n"
)
_GOES_STAT_HEADER = f"\t\tAvg\t\t\tMax\t\tTime\t\tMin\t\tTime\t\tValue at Start of Interruption\n{'-'*95}\n"

#
# --- Plot Keyword Arguments
#
_PLOT_KWARGS = {
    "linestyle": "",
    "marker": ".",
    "markersize": 0.5,
    "color": "black",
}


def goes_data_set(event_data, pathing_dict):
    """Intakes data from the Space Weather GOES data archive in ``SPACE_WEATHER_DIR`` into an ``astropy.table`` and uses data for plotting and statistics.

    :param event_data: A dictionary which stores interruption data.
    :type event_data: dict(str, cxotime or float or str)
    :param pathing_dict: A dictionary of file paths for storing file input and output.
    :type pathing_dict: dict(str, str)

    """
    print("GOES Data Set")
    time_start = _round_down(event_data["tstart"]) - timedelta(days=_FETCH_INTERVAL)
    time_stop = _round_down(event_data["tstop"]) + timedelta(days=_FETCH_INTERVAL)
    goes_table = fetch_GOES_data(time_start, time_stop, pathing_dict)
    write_goes_files(goes_table, event_data, pathing_dict)
    plot_goes_data(goes_table, event_data, pathing_dict)

def fetch_GOES_data(time_start, time_stop, pathing_dict):
    """Fetch GOES data from the ``SPACE_WEATHER_DIR/GOES/Data/goes_data_r.txt`` archive file and format into an astropy table.

    :param time_start: Starting time for data fetch, defaults to two days before the start of the interruption event.
    :type time_start: ``CxoTime``
    :param time_stop: Stopping time for data fetch, defaults to two days after the start of the interruption event.
    :type time_stop: ``CxoTime``
    :param pathing_dict: A dictionary of file paths for storing file input and output.
    :type pathing_dict: dict(str, str)
    :raises ValueError: If the ``event_data['tstart']`` starting time or ``event_data['tstop']`` stopping time data entires cannot be found in the Space Weather GOES data archive.
    :raises FileNotFoundError: If the ``SPACE_WEATHER_DIR/GOES/Data/goes_data_r.txt`` file cannot be found.
    :return: Table of unique GOES data points spanning interruption event.
    :rtype: ``astropy.table.Table``
    :Note: While algorithmically very similar to the data fetch performed in the :mod:`~interruption.ace_data_set` script,
        this table stores the time column as a string rather than a ``CxoTime`` object to reduce computation due to how GOES archive data files are stored.

    """
    data_file = os.path.join(
        pathing_dict["SPACE_WEATHER_DIR"], "GOES", "Data", "goes_data_r.txt"
    )
    #
    # --- Search the data_file via grep for the interruption time interval
    #
    data_start = None
    data_stop = None
    #
    # --- Find data line for start
    #
    while data_start is None:
        try:
            data_start_search = (
                f"grep -in '{time_start.strftime(_GOES_DATA_TIME_FORMAT)}' {data_file}"
            )
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
            data_stop_search = (
                f"grep -in '{time_stop.strftime(_GOES_DATA_TIME_FORMAT)}' {data_file}"
            )
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
    #
    # --- Once the data indices have been found, load that selection into an astropy table
    #
    goes_table = ascii.read(
        data_file, data_start=data_start - 3, data_end=data_stop - 2
    )
    return goes_table

def write_goes_files(goes_table, event_data, pathing_dict):
    """Write GOES data and statistics to human-reference text file.

    :param goes_table: GOES data table read from :func:`~interruption.goes_data_set.fetch_goes_data`.
    :type goes_table: astropy.table.Table
    :param event_data: A dictionary which stores interruption data.
    :type event_data: dict(str, cxotime or float or str)
    :param pathing_dict: A dictionary of file paths for storing file input and output.
    :type pathing_dict: dict(str, str)
    :File Out: Writes the ``<event_name>_goes.txt`` data table to the two ``OUT_WEB_DIR/Data_dir`` directories,
        and writes the ``<event_name>_goes_stat`` statistics table to the two ``OUT_WEB_DIR/Stat_dir`` directories.

    """
    #
    # --- Write Data File
    #
    line = _GOES_DATA_HEADER.replace(
        "#LSTART", event_data["tstart"].strftime("%Y:%m:%d:%H:%M")
    )
    for row in goes_table:
        substring = f"{row['Time']}\t\t"
        for channel in _GOES_CHANNEL_SELECT:
            if channel == "HRC_Proxy":
                substring += f"{row[channel]}"
            else:
                substring += f"{row[channel]:.3e}\t\t"
        line += f"{substring}\n"

    ifile = os.path.join(
        pathing_dict["OUT_WEB_DIR"], "Data_dir", f"{event_data['name']}_goes.txt"
    )
    ifile2 = os.path.join(
        pathing_dict["OUT_WEB_DIR2"], "Data_dir", f"{event_data['name']}_goes.txt"
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
    sel = goes_table["Time"] == _round_down(event_data["tstart"]).strftime(_GOES_DATA_TIME_FORMAT)
    interrupt_row = goes_table[sel][0]
    line = _GOES_STAT_HEADER
    for channel in _GOES_CHANNEL_SELECT:
        avg = np.mean(goes_table[channel].data)
        std = np.std(goes_table[channel].data)

        maxidx = np.argmax(goes_table[channel].data)
        max = goes_table[channel][maxidx]
        maxtime = goes_table["Time"][maxidx]

        sel = goes_table[channel].data >= 0
        minidx = np.argmin(goes_table[channel].data[sel])
        min = goes_table[channel][minidx]
        mintime = goes_table["Time"][minidx]

        val_intt = interrupt_row[channel]

        line += f"{channel}\t\t{avg:.3e}+/-{std:.3e}\t{max:.3e}\t{maxtime}\t{min:.3e}\t{mintime}\t{val_intt}\n"

    ifile = os.path.join(
        pathing_dict["OUT_WEB_DIR"], "Stat_dir", f"{event_data['name']}_goes_stat"
    )
    ifile2 = os.path.join(
        pathing_dict["OUT_WEB_DIR2"], "Stat_dir", f"{event_data['name']}_goes_stat"
    )
    os.makedirs(os.path.dirname(ifile), exist_ok=True)
    os.makedirs(os.path.dirname(ifile2), exist_ok=True)
    with open(ifile, "w") as f:
        f.write(line)
    if ifile != ifile2:
        shutil.copy(ifile, ifile2)


def plot_goes_data(goes_table, event_data, pathing_dict):
    """Create a plot of GOES channel data and HRC proxy.

    :param goes_table: GOES data table read from :func:`~interruption.goes_data_set.fetch_goes_data`.
    :type goes_table: astropy.table.Table
    :param event_data: A dictionary which stores interruption data.
    :type event_data: dict(str, cxotime or float or str)
    :param pathing_dict: A dictionary of file paths for storing file input and output.
    :type pathing_dict: dict(str, str)
    :File Out: Writes the ``<event_name>_goes.png`` plots to the two ``OUT_WEB_DIR/GOES_plot`` directories.

    """
    zones = rad_zones.filter(
        start=event_data["tstart"] - timedelta(days=_FETCH_INTERVAL),
        stop=event_data["tstop"] + timedelta(days=_FETCH_INTERVAL),
    ).table
    fig = plt.figure(figsize=(10, 6.7))
    fig.clf()

    times = CxoTime(goes_table["Time"])
    #
    # --- Reference Information
    #
    n_axes = len(_GOES_CHANNEL_SELECT)
    ylab = "Log$_{10}$"
    date_format = mdates.DateFormatter("%j")
    deltatime = event_data["tstop"].datetime - event_data["tstart"].datetime

    for i, channel in enumerate(_GOES_CHANNEL_SELECT):
        if i == 0:
            ax1 = fig.add_subplot(n_axes, 1, 1)
            ax = ax1
        else:
            ax = fig.add_subplot(n_axes, 1, i + 1, sharex=ax1)
        ax.xaxis.set_major_formatter(date_format)
        #
        # --- if not the last plot, turn of visibility of xtick labels
        #
        if i != n_axes - 1:
            ymin = -3
            ymax = 5
            int_label = 4
            for label in ax.get_xticklabels():
                label.set_visible(False)
        else:
            ymin = 1.5
            ymax = 6.5
            int_label = 5.5
            ax.set_xlabel("Day of Year", fontsize=9)

        ax.set_ylim(ymin, ymax, auto=False)
        ax.grid()
        #
        # --- Map values to a logarithmic scale, deselecting values of zero
        #
        m = goes_table[channel].data
        mapped_vals = np.log10(m, out=np.zeros_like(m, dtype=float), where=(m > 0))
        sel = mapped_vals != 0
        #plt.plot(times[sel], mapped_vals[sel], **_PLOT_KWARGS) #: Alternative
        plot_cxctime(times[sel], mapped_vals[sel], **_PLOT_KWARGS)
        #
        # --- Plot Indicator Lines
        #
        plt.axvline(event_data["tstart"].datetime, color="red", lw=2)  # Event Start
        plt.axvline(event_data["tstop"].datetime, color="red", lw=2)  # Event Ending
        #
        # --- Plot Labels
        #
        ax.set_ylabel(f"{ylab}({channel.replace('_', ' ')} Rate)", fontsize=9)
        plt.text(
            event_data["tstart"].datetime + (0.025 * deltatime),
            int_label,
            r"Interruption",
            color="red",
        )  # Interruption Marker
        #
        # --- Plot of radiation zones
        #
        for row in zones:
            start = datetime.strptime(str(row["start"]).split(".")[0], "%Y:%j:%H:%M:%S")
            stop = datetime.strptime(str(row["stop"]).split(".")[0], "%Y:%j:%H:%M:%S")
            plt.plot(
                [start, stop], [ymin, ymin], color="purple", lw=8
            )  # Radiation Zone

    ofile = os.path.join(
        pathing_dict["OUT_WEB_DIR"], "GOES_plot", f"{event_data['name']}_goes.png"
    )
    ofile2 = os.path.join(
        pathing_dict["OUT_WEB_DIR2"], "GOES_plot", f"{event_data['name']}_goes.png"
    )
    os.makedirs(os.path.dirname(ofile), exist_ok=True)
    os.makedirs(os.path.dirname(ofile2), exist_ok=True)
    plt.savefig(ofile, format="png", dpi=300)
    plt.savefig(ofile2, format="png", dpi=300)


#
# --- Internal functions to assist cleanly formatting the input GOES table
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
