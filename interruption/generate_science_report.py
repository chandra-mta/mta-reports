#!/proj/sot/ska3/flight/bin/python
"""
**generate_science_report.py** Pull together interruption data sets into an interruption html report.

:Author: W. Aaron (william.aaron@cfa.harvard.edu)
:Last Updated: Jan 27, 2025
:Note: This script is designed to be a submodule of **run_interruption.py**

"""
import os
import shutil
import re
from datetime import timedelta
import requests
import json

_ACIS_GIF_SOURCE = "http://acisweb.mit.edu/asc/txgif/gifs/"

_TIME_HEADER = '<a href="time_order.html" style="font-weight:bold;font-size:120%">\nTime Ordered List</a>\n</td><td>\n'
_AUTO_HEADER = '<a href="auto_shut.html" style="font-weight:bold;font-size:120%">\nAuto Shutdown List</em>\n</td><td>\n'
_MANUAL_HEADER = '<a href="manual_shut.html" style="font-weight:bold;font-size:120%">\nManually Shutdown List</a>\n</td><td>\n'
_HARDNESS_HEADER = '<a href="hardness_order.html" style="font-weight:bold;font-size:120%">\nHardness Ordered List</a>\n</td><td>\n'
_DESELECT_HEADER = '<em class="lime" style="font-weight:bold;font-size:120%">\n#TYPE#</em>\n</td><td>\n'


def generate_science_report(event_data, pathing_dict):
    """Generate the science report web pages.

    :param event_data: A dictionary which stores interruption data.
    :type event_data: dict(str, cxotime or float or str)
    :param pathing_dict: A dictionary of file paths for storing file input and output.
    :type pathing_dict: dict(str, str)

    """
    generate_event_report(event_data, pathing_dict)
    generate_shutdown_pages(event_data, pathing_dict)


def generate_event_report(event_data, pathing_dict):
    #
    # --- Read in template file and replace tags with shutdown data
    #
    print("Generating Event HTML")
    name = event_data["name"]
    with open(f"{pathing_dict['BIN_DIR']}/template/event_html_template") as f:
        event_template = f.read()
    event_template = re.sub("#header_title#", name, event_template)
    event_template = re.sub("#main_title#", name, event_template)
    event_template = re.sub(
        "#tstart#", event_data["tstart"].strftime("%Y:%m:%d:%H:%M:%S"), event_template
    )
    event_template = re.sub(
        "#tstop#", event_data["tstop"].strftime("%Y:%m:%d:%H:%M:%S"), event_template
    )
    event_template = re.sub("#tlost#", event_data["tlost"], event_template)
    event_template = re.sub("#mode#", event_data["mode"], event_template)
    event_template = re.sub("#note_name#", f"{name}.txt", event_template)
    #
    # --- ACE data set
    #
    stat_file = os.path.join(
        pathing_dict["OUT_WEB_DIR"], "Stat_dir", f"{name}_ace_stat"
    )
    event_template = re.sub("#ace_data#", f"{name}_ace.txt", event_template)
    with open(stat_file) as f:
        stat_table = f.read()
    event_template = re.sub("#ace_table#", stat_table, event_template)
    event_template = re.sub("#ace_plot#", f"{name}_ace.png", event_template)
    #
    # --- HRC data set
    #
    stat_file = os.path.join(
        pathing_dict["OUT_WEB_DIR"], "Stat_dir", f"{name}_hrc_stat"
    )
    event_template = re.sub("#hrc_data#", f"{name}_hrc.txt", event_template)
    with open(stat_file) as f:
        stat_table = f.read()
    event_template = re.sub("#hrc_table#", stat_table, event_template)
    event_template = re.sub("#hrc_plot#", f"{name}_hrc.png", event_template)
    #
    # --- ACIS data set
    # --- Format to include plot only if the .gif file from MIT exists
    #
    date_to_fetch = event_data["tstart"].datetime
    date_to_stop = event_data["tstop"].datetime
    acis_plot_links = []
    while (date_to_stop - date_to_fetch).days >= -1:
        url = f"{_ACIS_GIF_SOURCE}{date_to_fetch.strftime('%Y-%j')}.gif"
        r = requests.get(url)
        if r.status_code == 200:
            acis_plot_links.append(url)
        date_to_fetch += timedelta(days=1)
    aline = ""
    for i, url in enumerate(acis_plot_links):
        aline += f"<img src='{url}'style='width:45%; padding-bottom:30px;'>\n"
        if i % 2 == 1:
            aline += "<br/>\n"
    event_template = re.sub("#acis_plot#", aline, event_template)
    #
    # --- GOES data set
    #
    stat_file = os.path.join(
        pathing_dict["OUT_WEB_DIR"], "Stat_dir", f"{name}_goes_stat"
    )
    event_template = re.sub("#goes_data#", f"{name}_goes.txt", event_template)
    with open(stat_file) as f:
        stat_table = f.read()
    event_template = re.sub("#goes_table#", stat_table, event_template)
    event_template = re.sub("#goes_plot#", f"{name}_goes.png", event_template)
    #
    # --- XMM data set
    #
    stat_file = os.path.join(
        pathing_dict["OUT_WEB_DIR"], "Stat_dir", f"{name}_xmm_stat"
    )
    event_template = re.sub("#xmm_data#", f"{name}_xmm.txt", event_template)
    with open(stat_file) as f:
        stat_table = f.read()
    event_template = re.sub("#xmm_table#", stat_table, event_template)
    event_template = re.sub("#xmm_plot#", f"{name}_xmm.png", event_template)
    #
    # --- Write template contents to a html file
    #
    html_file = os.path.join(pathing_dict["OUT_WEB_DIR"], "Html_dir", f"{name}.html")
    html_file2 = os.path.join(pathing_dict["OUT_WEB_DIR2"], "Html_dir", f"{name}.html")
    os.makedirs(os.path.dirname(html_file), exist_ok=True)
    os.makedirs(os.path.dirname(html_file2), exist_ok=True)
    with open(html_file, "w") as f:
        f.write(event_template)
    if html_file != html_file2:
        shutil.copy(html_file, html_file2)


def generate_shutdown_pages(event_data, pathing_dict):
    time, auto, manual, hardness = _create_ordered_list(pathing_dict)
    template_header_file = os.path.join(
        pathing_dict["BIN_DIR"], "template", "main_header_template"
    )
    with open(template_header_file) as f:
        header_template = f.read()
    #
    # --- Create the four pages as strings
    # --- with _<type>_HEADERS to select other pages
    #
    time_page = (
        header_template
        + _DESELECT_HEADER.replace("#TYPE#", "Time Ordered List")
        + _AUTO_HEADER
        + _MANUAL_HEADER
        + _HARDNESS_HEADER
        + "</table>\n<ul>\n"
    )
    auto_page = (
        header_template
        + _TIME_HEADER
        + _DESELECT_HEADER.replace("#TYPE#", "Auto Shutdown List")
        + _MANUAL_HEADER
        + _HARDNESS_HEADER
        + "</table>\n<ul>\n"
    )
    manual_page = (
        header_template
        + _TIME_HEADER
        + _AUTO_HEADER
        + _DESELECT_HEADER.replace("#TYPE#", "Manually Shutdown List")
        + _HARDNESS_HEADER
        + "</table>\n<ul>\n"
    )
    hardness_page = (
        header_template
        + _TIME_HEADER
        + _AUTO_HEADER
        + _MANUAL_HEADER
        + _DESELECT_HEADER.replace("#TYPE", "Hardness Ordered List")
        + "</table>\n<ul>\n"
    )
    #
    # --- Iterate over ordered lists of events to fill our entries for each page
    #



def _create_ordered_list(pathing_dict):
    #
    # --- Read in list of all shutdowns to adjust order.
    #
    with open(f"{pathing_dict['DATA_DIR']}/all_shutdowns.json") as f:
        all_shutdowns = json.load(f)
    time_ordered = list(all_shutdowns.keys())
    time_ordered.sort(reverse=True)
    #
    # --- Iterate over events to splint into other ordered lists
    #
    auto_ordered = []
    manual_ordered = []
    for event in time_ordered:
        if all_shutdowns[event]["mode"] == "auto":
            auto_ordered.append(event)
        if all_shutdowns[event]["mode"] == "manual":
            manual_ordered.append(event)

    hardness = []
    for event in time_ordered:
        stat_file = os.path.join(
            pathing_dict["OUT_WEB_DIR2"], "Stat_dir", f"{event}_ace_stat"
        )
        if not os.path.exists(stat_file):
            raise Exception(f"{stat_file} not generated.")
        with open(stat_file) as f:
            stat_data = [line.strip() for line in f.readlines()]
        for line in stat_data:
            if "p47/p1060" in line or "p47-68/p1060-1900" in line:
                data = re.split("\s+|\t+", line)
                hardness.append(float(data[2]))
    temp = zip(hardness, time_ordered)
    temp = sorted(temp, reverse=True)
    hardness_ordered = []
    for event in temp:
        hardness_ordered.append(event[1])

    return time_ordered, auto_ordered, manual_ordered, hardness_ordered
