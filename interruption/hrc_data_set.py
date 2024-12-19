#!/proj/sot/ska3/flight/bin/python

"""
**hrc_data_set.py** Extract HRC data, compute statistics, and plot values.

:Author: W. Aaron (william.aaron@cfa.harvard.edu)
:Last Updated: Nov 04, 2024
:Note: As of 2020, EPHIN data in the interruption report was replaced with records of HRC shield event rates. This script is designed to be a submodule of **run_interruption.py**

"""

import os
import numpy as np
from datetime import datetime, timedelta
from cxotime import CxoTime, convert_time_format
from cheta import fetch
from kadi.events import rad_zones
from Ska.Matplotlib import plot_cxctime
import matplotlib.pyplot as plt
import shutil

#
# --- Define Directory Pathing
#
WEB_DIR = "/data/mta_www/mta_interrupt"
OUT_WEB_DIR = "/data/mta_www/mta_interrupt"
WEB_DIR2 = "/data/mta4/www/RADIATION_new/mta_interrupt"
OUT_WEB_DIR2 = "/data/mta4/www/RADIATION_new/mta_interrupt"

_PATHING_DICT = {
    "WEB_DIR": WEB_DIR,
    "OUT_WEB_DIR": OUT_WEB_DIR,
    "WEB_DIR2": WEB_DIR2,
    "OUT_WEB_DIR2": OUT_WEB_DIR2,
}  #: Dictionary of input and output file paths for collecting HRC interruption data.


_MSIDS = [
    "2SHEV2RT"
]  #: MSID Selection. Allow for multiple if we so choose to plot and record multiple HRC-related msids in report

#
# --- File Header Globals
#
_HRC_DATA_HEADER = f"Science Run Interruption: #LSTART\n\nTime\t\tHRC\n{'-'*67}\n"
_HRC_STAT_HEADER = f"\t\tAvg\t\t\tMax\t\tTime\t\tMin\t\tTime\t\tValue at Interruption Started\n{'-'*95}\n"
_HEADER_TIME_FORMAT = "%Y:%m:%d:%H:%M"

#
# --- Plot Keyword Arguments
#
_PLOT_KWARGS = {
    "linestyle": "",
    "marker": ".",
    "markersize": 0.5,
    "color": "black",
}


def hrc_data_set(event_data, pathing_dict):
    """Collect together HRC data for plotting, statistics, and storage by using ``cheta.fetch``.

    :param event_data: A dictionary which stores interruption data.
    :type event_data: dict(str, datetime or float or str)
    :param pathing_dict: A dictionary of file paths for storing file input and output.
    :type pathing_dict: dict(str, str)
    :Note: The HRC shield only actively takes data during usage of the HRC instrument.
        Therefore, the engineering archive might have no record of HRC shield rates the time of interruption.
        See :func:`~interruption.goes_data_set.plot_goes_data` for details on the GOES plotting of the HRC shield rate proxy.

    """
    print("HRC Data Set")
    fetch_result = fetch.MSIDset(
        _MSIDS,
        start=event_data["tstart"] - timedelta(days=2),
        stop=event_data["tstop"] + timedelta(days=2),
        stat="5min",
    )
    write_hrc_files(fetch_result, event_data, pathing_dict)
    plot_hrc_data(fetch_result, event_data, pathing_dict)


def write_hrc_files(fetch_result, event_data, pathing_dict):
    """Write HRC shield data and statistics to human-reference text file.

    :param fetch_result: ``cheta.fetch.MSIDset()`` result for the 2SHEV2RT MSID.
    :type fetch_result: cheta.fetch.MSIDset
    :param event_data: A dictionary which stores interruption data.
    :type event_data: dict(str, datetime or float or str)
    :param pathing_dict: A dictionary of file paths for storing file input and output.
    :type pathing_dict: dict(str, str)
    :File Out: Writes the ``<event_name>_hrc.txt`` data table to the two ``OUT_WEB_DIR/Data_dir`` directories,
        and writes the `<event_name>_hrc_stat` statistics table to the two ``OUT_WEB_DIR/Stat_dir`` directories.

    """
    #
    # --- Write Data File
    #
    line = _HRC_DATA_HEADER.replace(
        "#LSTART", event_data["tstart"].strftime(_HEADER_TIME_FORMAT)
    )
    times = convert_time_format(
        fetch_result[_MSIDS[0]].times, fmt_in="secs", fmt_out="date"
    )
    vals_group = []
    for msid in _MSIDS:
        vals_group.append(fetch_result[msid].vals)
    for data_tuple in np.nditer([times] + vals_group):
        substring = f"{data_tuple[0]}\t\t"
        substring += "\t\t".join([f"{x:.3e}" for x in data_tuple[1:]])
        line += f"{substring}\n"

    ifile = os.path.join(
        pathing_dict["OUT_WEB_DIR"], "Data_dir", f"{event_data['name']}_hrc.txt"
    )
    ifile2 = os.path.join(
        pathing_dict["OUT_WEB_DIR2"], "Data_dir", f"{event_data['name']}_hrc.txt"
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
    line = _HRC_STAT_HEADER
    for msid in _MSIDS:
        avg = np.mean(fetch_result[msid].vals)
        std = np.std(fetch_result[msid].vals)

        maxidx = np.argmax(fetch_result[msid].vals)
        max = fetch_result[msid].vals[maxidx]
        maxtime = CxoTime(fetch_result[msid].times[maxidx]).date

        minidx = np.argmin(fetch_result[msid].vals)
        min = fetch_result[msid].vals[minidx]
        mintime = CxoTime(fetch_result[msid].times[minidx]).date

        a = CxoTime(event_data["tstart"]).secs
        timeidx = np.abs(fetch_result[msid].times - a).argmin()
        val_intt = CxoTime(fetch_result[msid].times[timeidx]).date

        line += f"{msid}\t\t{avg:.3e}+/-{std:.3e}\t{max:.3e}\t{maxtime}\t{min:.3e}\t{mintime}\t{val_intt}\n"

    ifile = os.path.join(
        pathing_dict["OUT_WEB_DIR"], "Stat_dir", f"{event_data['name']}_hrc_stat"
    )
    ifile2 = os.path.join(
        pathing_dict["OUT_WEB_DIR2"], "Stat_dir", f"{event_data['name']}_hrc_stat"
    )
    os.makedirs(os.path.dirname(ifile), exist_ok=True)
    os.makedirs(os.path.dirname(ifile2), exist_ok=True)
    with open(ifile, "w") as f:
        f.write(line)
    if ifile != ifile2:
        shutil.copy(ifile, ifile2)


def plot_hrc_data(fetch_result, event_data, pathing_dict):
    """Create a plot of HRC shield data.

    :param fetch_result: ``cheta.fetch.MSIDset()`` result for the 2SHEV2RT MSID.
    :type fetch_result: cheta.fetch.MSIDset
    :param event_data: A dictionary which stores interruption data.
    :type event_data: dict(str, datetime or float or str)
    :param pathing_dict: A dictionary of file paths for storing file input and output.
    :type pathing_dict: dict(str, str)
    :File Out: Writes the ``<event_name>_hrc.png`` plots to the two ``OUT_WEB_DIR/HRC_plot`` directories.

    """
    zones = rad_zones.filter(
        start=event_data["tstart"] - timedelta(days=2),
        stop=event_data["tstop"] + timedelta(days=2),
    ).table
    m = fetch_result["2SHEV2RT"].vals
    times = fetch_result["2SHEV2RT"].times
    mapped_vals = np.log10(m, out=np.zeros_like(m), where=(m >= 1))
    #
    # --- Plotting
    #
    plt.rcParams["figure.figsize"] = [8, 4]
    tixs, fig, ax = plot_cxctime(times, mapped_vals, **_PLOT_KWARGS)
    plt.ylim([3, 5])
    plt.grid()
    #
    # --- Indicator Lines
    #
    plt.axhline(4.80, color="red", linestyle="--", lw=1.0)  # Violation Threshold
    plt.axvline(event_data["tstart"], color="red", lw=2)  # Event Start
    plt.axvline(event_data["tstop"], color="red", lw=2)  # Event Ending
    for row in zones:
        start = datetime.strptime(str(row["start"]).split(".")[0], "%Y:%j:%H:%M:%S")
        stop = datetime.strptime(str(row["stop"]).split(".")[0], "%Y:%j:%H:%M:%S")
        plt.plot([start, stop], [3, 3], color="purple", lw=8)  # Radiation Zone
    #
    # --- Labels
    #
    plt.ylabel("Log$_{10}$ (HRC Shield Event Rate)", fontsize=9)  # Y Label
    deltatime = event_data["tstop"] - event_data["tstart"]
    plt.text(
        event_data["tstart"] + (0.01 * deltatime), 4.6, r"Interruption", color="red"
    )  # Interruption Marker
    ofile = os.path.join(
        pathing_dict["OUT_WEB_DIR"], "HRC_plot", f"{event_data['name']}_hrc.png"
    )
    ofile2 = os.path.join(
        pathing_dict["OUT_WEB_DIR2"], "HRC_plot", f"{event_data['name']}_hrc.png"
    )
    os.makedirs(os.path.dirname(ofile), exist_ok=True)
    os.makedirs(os.path.dirname(ofile2), exist_ok=True)
    plt.savefig(ofile, format="png", dpi=300)
    plt.savefig(ofile2, format="png", dpi=300)
