#!/proj/sot/ska3/flight/bin/python

#############################################################################
#                                                                           #
#   find_recent_observations.py: create a data analysis link table          #   
#                                                                           #
#           author: t. isobe (tisobe@cfa.harvard.edu)                       #
#                                                                           #
#           Last Update: Mar 15, 2021                                       #
#                                                                           #
#############################################################################

import sys
import os
import re
import astropy.io.fits  as pyfits
import time
import Chandra.Time
import glob
#
#--- Define directory pathing
#
BIN_DIR = "/data/mta/Script/Weekly/Scripts"
AP_DIR = "/data/mta/www/ap_report"
MP_DIR = "/data/mta/www/mp_reports"
AP_EVENTS_WEB = "/mta_days/ap_report/events"
MP_EVENTS_WEB = "/mta_days/mp_report/events"
GRATING_WEB = "/data/mta/www/mta_grat/Grating_Data"
sys.path.append(BIN_DIR)

#---------------------------------------------------------------------------------------
#-- find_recent_observations: create a data analysis link table of recent observations for the weekly
#---------------------------------------------------------------------------------------

def find_recent_observations(etime=0):
    """
    create a data analysis link table of recent observations for the weekly
    input:  etime   --- ending time in seconds from 1998.1.1
    output: line    --- html table of the telemetry data analysis links
    """
    if etime == 0:
        ltime = time.strftime("%Y:%j:%H:%M:%S", time.gmtime())
        etime = int(Chandra.Time.DateTime(ltime).secs)
#
#--- get data about the last 8 days (or between etime and 8days before that)
#
    out = extract_telem_data(etime)
    if out == False:

        line = '<h3>There was No Observations in the Last Eight Days</h3>'
        return line

    else:
        [obs_list, inst_dict, det_dict, grat_dict, targ_dict, type_dict, gratpath] = out
#
#--- start writing table header
#
    line = "<table border=0 width=100%>\n"
    line = line + "<tr>\n"
    line = line + "<th style='text-decoration:underline;text-align:center;'>OBSID</th>\n"
    line = line + "<th style='text-decoration:underline;text-align:center;'>DETECTOR</th>\n"
    line = line + "<th style='text-decoration:underline;text-align:center;'>GRATING</th>\n"
    line = line + "<th style='text-decoration:underline;text-align:left;'>TARGET</th>\n"
    line = line + "<th style='text-decoration:underline;text-align:center;'>ANALYSIS</th>\n"
    line = line + "<th style='text-decoration:underline;text-align:center;'>ACA</th>\n"
    line = line + '</tr>\n'
#
#--- go through each data
#
    for obsid in obs_list:
        line = line + '<tr align=center>\n'
        line = line + '<td><a href="http://acisweb.mit.edu/cgi-bin/get-obsid?id='
        line = line + str(obsid) + '">' + str(obsid) + '</a></td>\n'

        line = line + '<td>' + det_dict[obsid]  + '</td>\n'
        line = line + '<td>' + grat_dict[obsid] + '</td>\n'
        line = line + '<td style="text-align:left;">' + targ_dict[obsid] + '</td>\n'
#
#--- analysis links
#
        mc   = re.search('acis', inst_dict[obsid].lower())
        if mc is not None:
            inst = 'acis'
        else:
            inst = 'hrc'

        afile = f"{AP_DIR}/events/{inst}/{obsid}/event.html"
        mfile = f"{MP_DIR}/events/{inst}/{obsid}/event.html"
        if os.path.isfile(afile):
            line = line + f'<td>\n<a href="{AP_EVENTS_WEB}/{inst}/{obsid}/event.html">{type_dict[obsid]}</a>'

        elif os.path.isfile(mfile):
            line = line + f'<td>\n<a href="{MP_EVENTS_WEB}/{inst}/{obsid}/event.html">{type_dict[obsid]}</a>'

        else:
            line = line + '<td>Missing'
#
#--- grating links
#
        line = line + gratpath[obsid] + '</td>\n'
#
#--- aca links
#
        afile = f"{AP_DIR}/events/aca/{obsid}/aca.html"
        mfile = f"{MP_DIR}/events/aca/{obsid}/aca.html"
        if os.path.isfile(afile):
            line = line + f'<td><a href="{AP_EVENTS_WEB}/aca/{obsid}/aca.html">OK</a></td>\n'

        elif os.path.isfile(mfile):
            line = line +f'<td><a href="{MP_EVENTS_WEB}/aca/{obsid}/aca.html">OK</a></td>\n'

        else:
            line = line + '<td>Missing</td>\n'
            

        line = line + '</tr>\n'
    line = line + '</table>\n'


    return line


#---------------------------------------------------------------------------------------
#-- extract_telem_data: extract the last 8 days of observations and find links to analysis
#---------------------------------------------------------------------------------------

def extract_telem_data(etime):
    """
    extract the last 8 days of observations and find links to analysis
    input:  etime       --- the collection end time in seconds from 1998.1.1
                /data/mta/www/mp_reports/events
    output: obs_list    --- a list of obsid
            inst_dict   --- a dict of insturment
            det_dict    --- a dict of detector
            grat_dict   --- a dict of grating
            targ_dict   --- a dict of target name
            type_dict   --- a dict of type of the analysis
            gratpath    --- a dict of a path to grating analysis result
    """
#
#--- find the data files
#
    path  = f'{MP_DIR}/events/*/*/'
    fname = 'event.html'
    days  = 8
    data  = find_recently_created_file(path, fname, days, etime)
#
#--- checkk whether we find observations, if not tell so and return
#
    if len(data) == 0:
        print("No new observations found posted in the past 8 days.\n")
        return False
#
#--- save data in dict forms with obsid as a key
#
    obs_list  = []
    inst_dict = {}
    det_dict  = {}
    grat_dict = {}
    targ_dict = {}
    type_dict = {}
    gratpath  = {}
#
#--- go through each observation
#
    for ifile in data:
        atemp = re.split('\/', ifile)
        inst  = atemp[6]
        obsid = atemp[7]
#
#--- find whether fits file related to this observation exists
#
        fdata = glob.glob(f"{MP_DIR}/events/{inst}/{obsid}/*.fits")
        if len(fdata) == 0:
            print("Problem getting info on obsid: " + obsid + '\n')
            continue

        obs_list.append(obsid)
        inst_dict[obsid] = inst
#
#--- read the header of the fits file; just use the first of the list
#
        fout = pyfits.open(fdata[0])

        det_dict[obsid]  = fout[0].header['DETNAM']
        targ_dict[obsid] = fout[0].header['OBJECT']
#
#--- analysis type
#
        observer = fout[0].header['OBSERVER']
        dmode    = fout[0].header['DATAMODE']
        if observer == 'CAL':
            type_dict[obsid] = 'CAL'
        else:
            mc = re.search('CC', dmode)
            if mc is not None:
                type_dict[obsid] = 'CC'
            else:
                type_dict[obsid] = 'OK'
#
#--- checking grating observation
#
        grat = fout[0].header['GRATING']
        grat_dict[obsid] = grat

        if grat == 'NONE':
            gratpath[obsid] = '\n'
#
#--- if the grating is used, find the path to the data file
#
        else:
            obs = obsid.zfill(5)
            data = glob.glob(f"{GRATING_WEB}/*/{obs}")
            data.sort(key=os.path.getmtime)
            if len(data) > 0:
                gline = data[0]
                gline = gline + '/obsid_' + obs + '_Sky_summary.html'
                if os.path.isfile(gline):
                    gline = gline.replace('data/mta/www', 'mta_days')
                    line  = '\n/<a href="' + gline + '">Grat</a>\n'
                    gratpath[obsid] = line
                else:
                    gratpath[obsid] = '/NA\n'
            else:
                gratpath[obsid] = '/NA\n'


    return [obs_list, inst_dict, det_dict, grat_dict, targ_dict, type_dict, gratpath]

#---------------------------------------------------------------------------------------
#-- find_recently_created_file: find recently created files                           --
#---------------------------------------------------------------------------------------

def find_recently_created_file(path, fname, days, etime):
    """
    find recently created files
    input:  path    --- path to the file with fname
            fname   --- a file name
            days    --- a time limit in days
    output: save    --- a list of files with full paths
    """

    cut   = etime - days * 86400
    data = glob.glob(f"{path}/{fname}")

    t_save = []
    d_dict = {}
    for ent  in data:
        ltime = time.strftime("%Y:%j:%H:%M:%S", time.gmtime(os.path.getctime(ent)))
        stime = int(Chandra.Time.DateTime(ltime).secs)
        if stime < cut:
            continue
        elif stime > etime:
            continue
        else:
            t_save.append(stime)
            d_dict[stime]  = ent

    t_save = sorted(t_save)
    save   = []
    for stime in t_save:
        save.append(d_dict[stime])

    return save

#---------------------------------------------------------------------------------------

if __name__ == "__main__":

    if len(sys.argv) > 1:
        etime = float(sys.argv[1])
    else:
        etime = 0

    line = find_recent_observations(etime)

    with open('./r_obs_table', 'w') as fo:
        fo.write(line)

