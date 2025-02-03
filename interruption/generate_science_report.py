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
from datetime import timedelta, datetime
import requests
import json
from jinja2 import Environment, FileSystemLoader

_ACIS_GIF_SOURCE = "http://acisweb.mit.edu/asc/txgif/gifs/"

_PAGE_NAMES = {'time': 'time_order',
               'auto': 'auto_shut',
               'manual': 'manual_shut',
               'hardness': 'hardness_order'}

_JINJA_ENV = Environment(loader = FileSystemLoader('template', followlinks = True))
_UPDATE_DATE = datetime.now().strftime("%Y-%m-%d")

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
    #
    # --- Read in Stat files for tables
    #
    ace_stat_file = os.path.join(
        pathing_dict["OUT_WEB_DIR"], "Stat_dir", f"{name}_ace_stat"
    )
    with open(ace_stat_file) as f:
        ace_table = f.read()

    hrc_stat_file = os.path.join(
        pathing_dict["OUT_WEB_DIR"], "Stat_dir", f"{name}_hrc_stat"
    )
    with open(hrc_stat_file) as f:
        hrc_table = f.read()
    
    goes_stat_file = os.path.join(
        pathing_dict["OUT_WEB_DIR"], "Stat_dir", f"{name}_goes_stat"
    )
    with open(goes_stat_file) as f:
        goes_table = f.read()
    
    xmm_file = os.path.join(
        pathing_dict["OUT_WEB_DIR"], "Stat_dir", f"{name}_xmm_stat"
    )
    with open(xmm_file) as f:
        xmm_table = f.read()
    #
    # --- Embed Link to existing MIT ACIS plots.
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
    acis_plot = ""
    for i, url in enumerate(acis_plot_links):
        acis_plot += f"<img src='{url}'style='width:45%; padding-bottom:30px;'>\n"
        if i % 2 == 1:
            acis_plot += "<br/>\n"
    #
    # --- Pull and Render Jinja Template
    #
    event_template = _JINJA_ENV.get_template('event_template.jinja')
    render = event_template.render(event_data = event_data,
                                   acis_plot = acis_plot,
                                   ace_table = ace_table,
                                   hrc_table = hrc_table,
                                   goes_table = goes_table,
                                   xmm_table = xmm_table)
    #
    # --- Write template contents to a html file
    #
    html_file = os.path.join(pathing_dict["OUT_WEB_DIR"], "Html_dir", f"{name}.html")
    os.makedirs(os.path.dirname(html_file), exist_ok=True)
    with open(html_file, "w") as f:
        f.write(render)

def generate_shutdown_pages(event_data, pathing_dict):

    with open(f"{pathing_dict['DATA_DIR']}/all_shutdowns.json") as f:
        all_shutdowns = json.load(f)
    ordered_lists = _create_ordered_list(all_shutdowns, pathing_dict)
    main_template = _JINJA_ENV.get_template('main_template.jinja')
    
    for type, sel in ordered_lists.items():
        render = main_template.render(type = type,
                                      sel = sel,
                                      all_shutdowns = all_shutdowns,
                                      pathing_dict = pathing_dict,
                                      _UPDATE_DATE = _UPDATE_DATE)
        html_file = os.path.join(pathing_dict["OUT_WEB_DIR"], f"{_PAGE_NAMES[type]}.html")
        os.makedirs(os.path.dirname(html_file), exist_ok=True)
        with open(html_file, "w") as f:
            f.write(render)

def _create_ordered_list(all_shutdowns, pathing_dict):
    #
    # --- Read in list of all shutdowns to adjust order.
    #
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
            pathing_dict["OUT_WEB_DIR"], "Stat_dir", f"{event}_ace_stat"
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

    return {'time': time_ordered,
     'auto': auto_ordered,
     'manual': manual_ordered,
     'hardness': hardness_ordered}