#!/proj/sot/ska3/flight/bin/python

#
# --- run_interruption.py: run all sci run interrupt scripts
# --- author w. aaron (william.aaron@cfa.harvard.edu)
# --- last update: Nov 04 2024
#
import sys
import os
import numpy as np
from cxotime import CxoTime
from kadi import events
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
    "SPACE_WEATHER_DIR": SPACE_WEATHER_DIR
}

#Time formats for stat / stop arguments
#TODO reformat accepted times for compute gap as it's confusing DOY format with seconds for month and day without seconds.
TIME_FORMATS = ["%Y:%j:%H:%M:%S", "%Y:%j:%H:%M", "%Y:%m:%d:%H:%M:%S", "%Y:%m:%d:%H:%M"]

#
# --- extracting formatted data sets, compute statistics, then plot for each data category
#
import hrc_data_set as hrc  # noqa: E402

import goes_data_set as goes # noqa: E402 

#import extract_data as edata  # noqa: E402
#import extract_ephin  # EPHIN/HRC data extraction  # noqa: E402
#import extract_goes  # GOES DATA extraction  # noqa: E402
#import extract_ace_data  # ACE (NOAA) data extraction  # noqa: E402
#import compute_ace_stat  # ACE (NOAA) Statistics  # noqa: E402

#
# --- Ephin ploting routines
#
#import plot_ephin as ephin  # noqa: E402

#
# ---- GOES ploting routiens
#
#import plot_goes as goes  # noqa: E402

#
# ---- ACE plotting routines
#
#import plot_ace_rad as ace  # noqa: E402

#
# ---- extract xmm data and plot
#
#import compute_xmm_stat_plot_for_report as xmm  # noqa: E402

#
# ---- update html page
#
#import sci_run_print_html as srphtml  # noqa: E402


# -------------------------------------------------------------------------------------
# -- compute_gap: process stat / stop time arguments                                 --
# -------------------------------------------------------------------------------------
def compute_gap(start, stop, name=None):
    """
    Intake string-formatted time and output interruption data values
    """
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
    if name is None:
        name = tstart.strftime("%Y%m%d")

    chandra_start = CxoTime(tstart.strftime("%Y:%j:%H:%M:%S"))
    chandra_stop = CxoTime(tstop.strftime("%Y:%j:%H:%M:%S"))

    rad_zones = events.rad_zones.filter(start=chandra_start, stop=chandra_stop).table
    rad_zones_duration_secs = np.sum(rad_zones["dur"])
    science_time_lost_secs = (
        chandra_stop.secs - chandra_start.secs - rad_zones_duration_secs
    )

    out = {
        "name": name,
        "tstart": tstart,
        "tstop": tstop,
        "tlost": f"{(science_time_lost_secs / 1000.):.2f}",
    }
    return out
# -------------------------------------------------------------------------------------
# -- supplemental_files: write relevant info to a few supplamental files             --
# -------------------------------------------------------------------------------------
def supplemental_files(event_data, pathing_dict):
    """
    Write supplemental radiation event information
    input:  event_data ---  dictionary of radiation shutdown event values
            pathing_dict --- dictionary of pathing variables
    output: none but write to
            <data_dir>/rad_zone_list
    """
    rad_zones_shutdown = {}
    #
    # --- Pulls current list to avoid re-recording the information
    #
    ifile = os.path.join(pathing_dict['DATA_DIR'], 'rad_zone_list')
    os.makedirs(os.path.dirname(ifile), exist_ok = True)
    if os.path.exists(ifile):
        with open(ifile) as f:
            data = [line.strip().split() for line in f.readlines()]
        for entry in data:
            rad_zones_shutdown[entry[0]] = entry[1].split(":")
    
    chandra_start = CxoTime(event_data['tstart'].strftime("%Y:%j:%H:%M:%S"))
    chandra_stop = CxoTime(event_data['tstop'].strftime("%Y:%j:%H:%M:%S"))
    table = events.rad_zones.filter(start=chandra_start.secs - (3 * 86400.0), stop=chandra_stop.secs + (5 * 86400.0)).table
    event_zones = []
    for row in table:
        event_zones.append((round(row['tstart'],0), round(row['tstop'],0)))
    rad_zones_shutdown[event_data['name']] = event_zones

    #
    # --- Write out rad_zone_list
    #
    string = ''
    for k,v in sorted(rad_zones_shutdown.items()):
        string += f"{k}\t{':'.join([str(y) for y in v])}\n"
    with open(ifile,'w') as file:
        file.write(string)


# -------------------------------------------------------------------------------------
# -- run_interrupt: run all sci run plot routines                                    --
# -------------------------------------------------------------------------------------


def run_interrupt(event_data, pathing_dict):
    """
    run all sci run plot routines
    input:  event_data ---  dictionary of radiation shutdown event values
            pathing_dict --- dictionary of pathing variables
    output: <plot_dir>/*.png    --- ace data plot
            <ephin_dir>/*.png   --- ephin data plot
            <goes_dir>/*.png    --- goes data plot
            <xmm_dir>/*.png     --- xmm data plot
            <html_dir>/*.html   --- html page for that interruption
            <web_dir>/rad_interrupt.html    --- main page
    """
    print(f"Generating: {event_data['name']}")
    #supplemental_files(event_data, pathing_dict)
    
    #
    # --- HRC data set
    #
    print("HRC Data Set")
    #hrc.hrc_data_set(event_data, pathing_dict)
    
    #
    # ---- GOES data set
    #
    print("GOES")
    goes.goes_data_set(event_data, pathing_dict)
    #
    # ---- plot other radiation data (from NOAA)
    #
    print("NOAA")
    #ace.start_ace_plot(event_data, pathing_dict)
    #
    # ---- extract and plot XMM data
    #
    print("XMM")
    #xmm.read_xmm_and_process(event_data, pathing_dict)
    #
    # ---- create individual html page and main html page
    #
    print("HTML UPDATE")
    #srphtml.print_each_html_control(event_data, pathing_dict)


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
        "--start", required=True, help="Start time of radiation shutdown."
    )
    parser.add_argument(
        "--stop", required=True, help="Stop time of radiation shutdown."
    )
    parser.add_argument(
        "-n",
        "--name",
        required=False,
        help="Custom name for event (defaults to start date in <YYYY><MM><DD> format).",
    )
    parser.add_argument(
        "-r",
        "--run",
        choices=["auto", "manual"],
        required=True,
        help="Determine SCS-107 run version.",
    )
    args = parser.parse_args()

    event_data = compute_gap(args.start, args.stop, name=args.name)
    event_data["mode"] = args.run

    if args.mode == "test":
        #
        # --- Change default pathings for test run
        #
        BIN_DIR = f"{os.getcwd()}"
        os.makedirs(f"{BIN_DIR}/test/outTest", exist_ok=True)
        PATHING_DICT = {
            "BIN_DIR": BIN_DIR,
            "DATA_DIR": f"{BIN_DIR}/test/outTest",
            "OUT_DATA_DIR": f"{BIN_DIR}/test/outTest",
            "WEB_DIR": f"{BIN_DIR}/test/outTest",
            "OUT_WEB_DIR": f"{BIN_DIR}/test/outTest",
            "WEB_DIR2": f"{BIN_DIR}/test/outTest",
            "OUT_WEB_DIR2": f"{BIN_DIR}/test/outTest",
            "SPACE_WEATHER_DIR": SPACE_WEATHER_DIR
        }
        run_interrupt(event_data, PATHING_DICT)

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
