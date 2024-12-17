#!/proj/sot/ska3/flight/bin/python

"""
**run_interruption.py**: Run all science interruption scripts.

:Author: W. Aaron (william.aaron@cfa.harvard.edu)
:Last Updated: Dec 17, 2024

"""

import sys
import os
import numpy as np
from cxotime import CxoTime
from kadi.events import rad_zones
from datetime import datetime
import argparse
import getpass

#
# --- Define Directory Pathing
#
BIN_DIR = "/data/mta/Script/Interrupt/Scripts"
DATA_DIR = "/data/mta/Script/Interrupt/Data"
OUT_DATA_DIR = "/data/mta/Script/Interrupt/Data"
WEB_DIR = "/data/mta_www/mta_interrupt"
OUT_WEB_DIR = "/data/mta_www/mta_interrupt"
WEB_DIR2 = "/data/mta4/www/RADIATION_new/mta_interrupt"
OUT_WEB_DIR2 = "/data/mta4/www/RADIATION_new/mta_interrupt"
SPACE_WEATHER_DIR = "/data/mta4/Space_Weather"

PATHING_DICT = {
    "BIN_DIR": BIN_DIR,
    "DATA_DIR": DATA_DIR,
    "OUT_DATA_DIR": OUT_DATA_DIR,
    "WEB_DIR": WEB_DIR,
    "OUT_WEB_DIR": OUT_WEB_DIR,
    "WEB_DIR2": WEB_DIR2,
    "OUT_WEB_DIR2": OUT_WEB_DIR2,
    "SPACE_WEATHER_DIR": SPACE_WEATHER_DIR,
}  #: Dictionary of input and output file paths for collecting all interruption data.

TIME_FORMATS = [
    '%Y:%j:%H:%M:%S',
    '%Y:%m:%d:%H:%M:%S',
]  #: Allowable time formats for recording an interruption from the command line.

#
# --- extracting formatted data sets, compute statistics, then plot for each data category
#
import hrc_data_set as hrc  # noqa: E402

import goes_data_set as goes  # noqa: E402

# import extract_data as edata  # noqa: E402
# import extract_ephin  # EPHIN/HRC data extraction  # noqa: E402
# import extract_goes  # GOES DATA extraction  # noqa: E402
# import extract_ace_data  # ACE (NOAA) data extraction  # noqa: E402
# import compute_ace_stat  # ACE (NOAA) Statistics  # noqa: E402

#
# --- Ephin ploting routines
#
# import plot_ephin as ephin  # noqa: E402

#
# ---- GOES ploting routiens
#
# import plot_goes as goes  # noqa: E402

#
# ---- ACE plotting routines
#
# import plot_ace_rad as ace  # noqa: E402

#
# ---- extract xmm data and plot
#
# import compute_xmm_stat_plot_for_report as xmm  # noqa: E402

#
# ---- update html page
#
# import sci_run_print_html as srphtml  # noqa: E402


# -------------------------------------------------------------------------------------
# -- generate_event_dict: process stat / stop time arguments                         --
# -------------------------------------------------------------------------------------
def generate_event_dict(start, stop, name=None):
    """Intake string-formatted time and output interruption data values. Uses ``kadi.events.rad_zones`` to filter out radiation zone information and compute the lost science time.

    :param start: Start time argument from the command line.
        See :data:`~interruption.run_interruption.TIME_FORMATS` for accepted formats.
    :type start: str
    :param stop: Stop time argument from the command line.
        See :data:`~interruption.run_interruption.TIME_FORMATS` for accepted formats.
    :type stop: str
    :param name: Name of interruption event, parameter defaults to ``None`` which sets the name to the start time.
    :type name: str, optional
    :raises ValueError: If the provided starting or stopping times are not one of the formats listed in :data:`~interruption.run_interruption.TIME_FORMATS`
    :return: **event_data** - A dictionary which stores interruption data.
    :rtype: dict(str, datetime or float or str)

    """
    tstart = None
    tstop = None
    for form in TIME_FORMATS:
        try:
            tstart = datetime.strptime(start, form)
            break
        except ValueError:
            pass
    for form in TIME_FORMATS:
        try:
            tstop = datetime.strptime(stop, form)
            break
        except ValueError:
            pass
    if tstart is None:
        raise ValueError(
            f"Start time must be in one of the following formats: {TIME_FORMATS}"
        )
    if tstop is None:
        raise ValueError(
            f"Stop time must be in one of the following formats: {TIME_FORMATS}"
        )
    if name is None:
        name = tstart.strftime("%Y%m%d")

    chandra_start = CxoTime(tstart.strftime("%Y:%j:%H:%M:%S"))
    chandra_stop = CxoTime(tstop.strftime("%Y:%j:%H:%M:%S"))

    zones = rad_zones.filter(start=chandra_start, stop=chandra_stop).table
    zones_duration_secs = np.sum(zones["dur"])
    science_time_lost_secs = (
        chandra_stop.secs - chandra_start.secs - zones_duration_secs
    )

    out = {
        "name": name,
        "tstart": tstart,
        "tstop": tstop,
        "tlost": f"{(science_time_lost_secs / 1000.):.2f}",
    }
    return out


# -------------------------------------------------------------------------------------
# -- supplemental_files: write relevant info to a few supplemental files             --
# -------------------------------------------------------------------------------------
def supplemental_files(event_data, pathing_dict):
    """Write supplemental radiation event information to additional files with locations determined by :data:`~interruption.run_interruption.PATHING_DICT`.

    :param event_data: A dictionary which stores interruption data.
    :type event_data: dict(str, datetime or float or str)
    :param pathing_dict: A dictionary of file paths for storing file input and output.
    :type pathing_dict: dict(str, str)
    :File Out: Writes the ``rad_zone_list`` file containing radiation zone information to the ``DATA_DIR`` directory.

    """
    zones_shutdown = {}
    #
    # --- Pulls current list to avoid re-recording the information
    #
    ifile = os.path.join(pathing_dict["DATA_DIR"], "rad_zone_list")
    os.makedirs(os.path.dirname(ifile), exist_ok=True)
    if os.path.exists(ifile):
        with open(ifile) as f:
            data = [line.strip().split() for line in f.readlines()]
        for entry in data:
            zones_shutdown[entry[0]] = entry[1].split(":")

    chandra_start = CxoTime(event_data["tstart"].strftime("%Y:%j:%H:%M:%S"))
    chandra_stop = CxoTime(event_data["tstop"].strftime("%Y:%j:%H:%M:%S"))
    table = rad_zones.filter(
        start=chandra_start.secs - (3 * 86400.0), stop=chandra_stop.secs + (5 * 86400.0)
    ).table
    event_zones = []
    for row in table:
        event_zones.append((round(row["tstart"], 0), round(row["tstop"], 0)))
    zones_shutdown[event_data["name"]] = event_zones

    #
    # --- Write out rad_zone_list
    #
    string = ""
    for k, v in sorted(zones_shutdown.items()):
        string += f"{k}\t{':'.join([str(y) for y in v])}\n"
    with open(ifile, "w") as file:
        file.write(string)


# -------------------------------------------------------------------------------------
# -- run_interrupt: run all sci run plot routines                                    --
# -------------------------------------------------------------------------------------


def run_interrupt(event_data, pathing_dict):
    """Run all interruption data set collection and plotting scripts, then write content to a single interruption report.

    :param event_data: A dictionary which stores interruption data.
    :type event_data: dict(str, datetime or float or str)
    :param pathing_dict: A dictionary of file paths for storing file input and output.
    :type pathing_dict: dict(str, str)
    :File Out:

    """
    print(f"Generating: {event_data['name']}")
    supplemental_files(event_data, pathing_dict)
    #
    # --- HRC data set
    #
    print("HRC Data Set")
    hrc.hrc_data_set(event_data, pathing_dict)
    #
    # ---- GOES data set
    #
    print("GOES")
    goes.goes_data_set(event_data, pathing_dict)
    #
    # ---- plot other radiation data (from NOAA)
    #
    print("NOAA")
    # ace.start_ace_plot(event_data, pathing_dict)
    #
    # ---- extract and plot XMM data
    #
    print("XMM")
    # xmm.read_xmm_and_process(event_data, pathing_dict)
    #
    # ---- create individual html page and main html page
    #
    print("HTML UPDATE")
    # srphtml.print_each_html_control(event_data, pathing_dict)


# -------------------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-m",
        "--mode",
        choices=["flight", "test"],
        required=True,
        help="Determine running mode.",
    )
    parser.add_argument(
        "-p",
        "--path",
        required=False,
        help="Directory path to determine output location.",
    )
    parser.add_argument(
        "--start",
        required=True,
        help="Start time of radiation shutdown.",
    )
    parser.add_argument(
        "--stop",
        required=True,
        help="Stop time of radiation shutdown.",
    )
    parser.add_argument(
        "-n",
        "--name",
        required=False,
        help="Custom name for event (defaults to start date in YYYYMMDD format).",
    )
    parser.add_argument(
        "-r",
        "--run",
        choices=["auto", "manual"],
        required=True,
        help="Determine SCS-107 run version.",
    )
    args = parser.parse_args()

    event_data = generate_event_dict(args.start, args.stop, name=args.name)
    event_data["mode"] = args.run

    if args.mode == "test":
        #
        # --- Change default pathing for test run
        #
        BIN_DIR = f"{os.getcwd()}"
        os.makedirs(f"{BIN_DIR}/test/outTest", exist_ok=True)
        pathing_dict = {
            "BIN_DIR": BIN_DIR,
            "DATA_DIR": f"{BIN_DIR}/test/outTest",
            "OUT_DATA_DIR": f"{BIN_DIR}/test/outTest",
            "WEB_DIR": f"{BIN_DIR}/test/outTest",
            "OUT_WEB_DIR": f"{BIN_DIR}/test/outTest",
            "WEB_DIR2": f"{BIN_DIR}/test/outTest",
            "OUT_WEB_DIR2": f"{BIN_DIR}/test/outTest",
            "SPACE_WEATHER_DIR": SPACE_WEATHER_DIR,
        }
        run_interrupt(event_data, pathing_dict)

    elif args.mode == "flight":
        #
        # --- Send warning if not running on machine with mta_www access
        #
        import platform

        machine = platform.node().split(".")[0]
        if machine not in ["boba-v", "luke-v", "r2d2-v", "c3po-v"]:
            parser.error(
                f"Need virtual machine (boba, luke, r2d2, c3po) to view /data/mta_www. Current machine: {machine}"
            )
        #
        # --- Create a lock file and exit strategy in case of race conditions
        #
        name = os.path.basename(__file__).split(".")[0]
        user = getpass.getuser()
        if os.path.isfile(f"/tmp/{user}/{name}.lock"):
            sys.exit(
                f"Lock file exists as /tmp/{user}/{name}.lock. Process already running/errored out. Check calling scripts/cronjob/cronlog."
            )
        else:
            os.system(f"mkdir -p /tmp/{user}; touch /tmp/{user}/{name}.lock")

        run_interrupt(event_data, PATHING_DICT)
        #
        # --- Remove lock file once process is completed
        #
        os.system(f"rm /tmp/{user}/{name}.lock")
