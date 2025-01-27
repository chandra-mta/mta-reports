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

_ACIS_GIF_SOURCE = "http://acisweb.mit.edu/asc/txgif/gifs/"

def generate_science_report(event_data, pathing_dict):
    """Generate the science report web pages.

    :param event_data: A dictionary which stores interruption data.
    :type event_data: dict(str, cxotime or float or str)
    :param pathing_dict: A dictionary of file paths for storing file input and output.
    :type pathing_dict: dict(str, str)

    """
    generate_event_report(event_data, pathing_dict)
    generate_top_shutdown_pages(event_data, pathing_dict)


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
    date_to_fetch = event_data['tstart'].datetime
    date_to_stop = event_data['tstop'].datetime
    acis_plot_links = []
    while (date_to_stop - date_to_fetch).days >= -1:
        url = f"{_ACIS_GIF_SOURCE}{date_to_fetch.strftime('%Y-%j')}.gif"
        r = requests.get(url)
        if r.status_code == 200:
            acis_plot_links.append(url)
        date_to_fetch += timedelta(days=1)
    aline = ''
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


def generate_top_shutdown_pages(event_data, pathing_dict):
    pass
