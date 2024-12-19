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

PATHING_DICT = {
    "WEB_DIR": WEB_DIR,
    "OUT_WEB_DIR": OUT_WEB_DIR,
    "WEB_DIR2": WEB_DIR2,
    "OUT_WEB_DIR2": OUT_WEB_DIR2,
    "SPACE_WEATHER_DIR": SPACE_WEATHER_DIR,
}

GOES_DATA_TIME_FORMAT = "%Y:%j:%H:%M:%S"
GOES_CHANNEL_SELECT = ["P4", "P5", "P6", "HRC_Proxy"]

subhead = "\t\t".join(GOES_CHANNEL_SELECT)
GOES_DATA_HEADER = f"Science Run Interruption: #LSTART\n\nTime\t\t{subhead}\n{'-'*67}\n"
GOES_STAT_HEADER = f"\t\tAvg\t\t\tMax\t\tTime\t\tMin\t\tTime\t\tValue at Interruption Started\n{'-'*95}\n"
TIME_FORMAT = "%Y:%m:%d:%H:%M"

#
# --- Plot Keyword Arguments
#
PLOT_KWARGS = {
    "linestyle": "",
    "marker": ".",
    "markersize": 0.5,
    "color": "black",
}


def goes_data_set(event_data, pathing_dict):
    """Intakes data from the Space Weather GOES data archive in ``SPACE_WEATHER_DIR`` into an ``astropy.table`` and uses data for plotting and statistics.

    :param event_data: A dictionary which stores interruption data.
    :type event_data: dict(str, datetime or float or str)
    :param pathing_dict: A dictionary of file paths for storing file input and output.
    :type pathing_dict: dict(str, str)
    :raises ValueError: If the ``event_data['tstart']`` starting time or ``event_data['tstop']`` stopping time data entires cannot be found in the Space Weather GOES data archive.

    """
    print("GOES Data Set")
    data_file = os.path.join(
        pathing_dict["SPACE_WEATHER_DIR"], "GOES", "Data", "goes_data_r.txt"
    )
    #
    # --- Search the data_file via grep for the interruption time interval
    #
    time_start = _round_down(event_data["tstart"]) - timedelta(days=2)
    time_stop = _round_down(event_data["tstop"]) + timedelta(days=2)
    data_start = None
    data_stop = None
    #
    # --- Find data line for start
    #
    while data_start is None:
        try:
            data_start_search = (
                f"grep -in '{time_start.strftime(GOES_DATA_TIME_FORMAT)}' {data_file}"
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
                f"grep -in '{time_stop.strftime(GOES_DATA_TIME_FORMAT)}' {data_file}"
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
    goes_table = ascii.read(data_file, data_start = data_start - 3, data_end = data_stop - 2)
    write_goes_files(goes_table, event_data, pathing_dict)
    plot_goes_data(goes_table, event_data, pathing_dict)


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
    line = GOES_DATA_HEADER.replace(
        "#LSTART", event_data["tstart"].strftime(TIME_FORMAT)
    )
    for row in goes_table:
        substring = f"{row['Time']}\t\t"
        for channel in GOES_CHANNEL_SELECT:
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
    sel = goes_table["Time"] == event_data["tstart"].strftime(GOES_DATA_TIME_FORMAT)
    interrupt_row = goes_table[sel][0]
    line = GOES_STAT_HEADER
    for channel in GOES_CHANNEL_SELECT:
        avg = np.mean(goes_table[channel].data)
        std = np.std(goes_table[channel].data)

        maxidx = np.argmax(goes_table[channel].data)
        max = goes_table[channel][maxidx]
        maxtime = goes_table["Time"][maxidx]

        minidx = np.argmin(goes_table[channel].data)
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

    :param goes_table: GOES data table read from ``SPACE_WEATHER_DIR/GOES/Data``.
    :type goes_table: astropy.table.Table
    :param event_data: A dictionary which stores interruption data.
    :type event_data: dict(str, datetime or float or str)
    :param pathing_dict: A dictionary of file paths for storing file input and output.
    :type pathing_dict: dict(str, str)
    :File Out: Writes the ``<event_name>_goes.png`` plots to the two ``OUT_WEB_DIR/GOES_plot`` directories.

    """
    zones = rad_zones.filter(
        start=event_data["tstart"] - timedelta(days=2),
        stop=event_data["tstop"] + timedelta(days=2),
    ).table
    fig = plt.figure(figsize=(10, 6.7))
    fig.clf()

    times = _convert_time_format(goes_table["Time"].data)
    #
    # --- Reference Information
    #
    n_axes = len(GOES_CHANNEL_SELECT)
    ylab = "Log$_{10}$"
    date_format = mdates.DateFormatter("%j")
    deltatime = event_data["tstop"] - event_data["tstart"]

    for i, channel in enumerate(GOES_CHANNEL_SELECT):
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
        m = goes_table[GOES_CHANNEL_SELECT[i]].data
        mapped_vals = np.log10(m, out=np.zeros_like(m, dtype=float), where=(m > 0))
        sel = mapped_vals != 0
        plt.plot(times[sel], mapped_vals[sel], **PLOT_KWARGS)
        #
        # --- Plot Indicator Lines
        #
        plt.axvline(event_data["tstart"], color="red", lw=2)  # Event Start
        plt.axvline(event_data["tstop"], color="red", lw=2)  # Event Ending
        #
        # --- Plot Labels
        #
        ax.set_ylabel(
            f"{ylab}({GOES_CHANNEL_SELECT[i].replace('_', ' ')} Rate)", fontsize=9
        )
        plt.text(
            event_data["tstart"] + (0.025 * deltatime),
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
    """Round a DateTime object down to the nearest five minutes. For use in fetching from data files.

    :param dt: A ``DateTime`` object of any kind
    :type dt: DateTime
    :return: The input ``DateTime`` object rounded down to the nearest five minutes.
    :rtype: DateTime

    """
    delta_min = dt.minute % 5
    return datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute - delta_min)

@np.vectorize
def _convert_time_format(string):
    return datetime.strptime(string, GOES_DATA_TIME_FORMAT)