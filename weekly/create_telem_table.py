#!/proj/sot/ska3/flight/bin/python

#################################################################################
#                                                                               #
#       create_telem_table.py: create telemetry table for weekly report         #
#                                                                               #
#           author: t. isobe (tisobe@cfa.harvard.edu)                           #
#                                                                               #
#           last update: Mar 15, 2021                                           #
#                                                                               #
#################################################################################

import sys
import os
import re
import os.path
import time
import Chandra.Time
import astropy.io.fits  as pyfits
import glob
#
#--- Directory Pathing
#
AP_DIR = "/data/mta/www/ap_report"
LIMIT_DIR = "/data/mta4/MTA/data/op_limits"

#-------------------------------------------------------------------------------
#-- get_telem_data: create telemetry table for weekly report                  --
#-------------------------------------------------------------------------------

def get_telem_data(start, stop):
    """
    create telemetry table for weekly report
    input:  start   --- start time in seconds from 1998.1.1
            stop    --- stop time in seconds from 1998.1.1
    output: './telem_table'
    """
#
#--- create dictionaries of limit, description, and unit with msid as key
#
    [l_dict, d_dict, u_dict] = get_limit_values()
#
#--- create date list of format of yyyymmdd from start and stop time in Chandra time format
#
    date_list = make_time_stamp_list(start, stop)

    o_dict = {}
    o_list = []
#
#--- find fits data files of each day and read the data
#
    for date in date_list:
        print("DATE: " + str(date))
        f_list = glob.glob(f"{AP_DIR}/{date}/*/data/*_summ.fits*")
#
#--- read each fits data file
#
        for fits in f_list:
            fh    = pyfits.open(fits)
            data  = fh[1].data
            lmsid = data['name']

            desc  = data['description']
            dmin  = data['min']
            dmax  = data['max']
            ylim  = data['yellow']
            rlim  = data['red']
            fh.close()
#
#--- go through each msid and find violations
#
            for k in range(0, len(lmsid)):
                msid = lmsid[k]
                d_dict[msid] = desc[k]
                if ylim[k] == 0 or rlim[k] == 0:
                    continue
#
#--- check whether this is lower or upper violation
#
                ltable = l_dict[msid]
                if dmin[k] < ltable[0]:
                    pos = -1
                    val = dmin[k]
                    color = 'y'
                    if dmin[k] < ltable[2]:
                        color = 'r'

                elif dmax[k] > ltable[1]:
                    pos = 1
                    val = dmax[k]
                    color = 'y'
                    if dmax[k] > ltable[3]:
                        color = 'r'

                else:
                    continue

                val = '%3.2f' % round(float(val), 2)
#
#--- save date, value and color in a list of a list and then into a dictionary
#
                nlist = [[date, val, color, pos]]
                o_list.append(msid)
                try:
                    alist = o_dict[msid]
                    alist = alist + nlist
                    o_dict[msid] = alist
                except:
                    o_dict[msid] = nlist
#
#--- clean up the msid list so that we have only one of each msid
#
    o_list = clean_up_msid_list(o_list)
#
#--- start creating the html table
#
    line  = '<table border=1>\n'
    line  = line + '<tr><td>MSID</td>\n'
    for date in date_list:
        sdate = convert_date_format(date)
        line = line + '<td>' + sdate + '</td>\n'
    line  = line + "<td><em class='yellow'>yellow limits<br />(lower)<br />upper</em>\n"
    line  = line + "<td><em class='red'>red limits<br />(lower)<br />upper</em>\n"
    line  = line + '<td>Units <td>Description</td></tr>\n'

    for msid in o_list:
        mc = re.search('1pin1at', msid.lower())
        if mc is not None:
            continue
#
#--- first set blank data list for the msid
#
        t_list = []
        for ent in date_list:
            t_list.append([ent,'','', ''])
#
#--- then replace where none empty data exists
#
        data = o_dict[msid]
        for ent in data:
            ctime = ent[0]
            for k in range(0, len(date_list)):
                if ctime == date_list[k]:
                    t_list[k]  = ent
#
#--- create table row entry for the msid
#
        line = line + '<tr><td>' + msid + '</td>\n'
        for ent in t_list:
            if ent[1] == '':
                line = line + '<td>&#160;</td>\n'
            else:
#
#--- yellow warning
#
                if ent[2] == 'y':
#
#--- lower limit violation
#
                    if ent[3] < 0:
                        line = line + '<td><em class="yellow">' 
                        line = line + '(' + str(ent[1]) + ')</em></td>\n'
#
#--- upper limit violation
#
                    else:
                        line = line + '<td><em class="yellow">' 
                        line = line + str(ent[1]) + '</em></td>\n'
#
#--- red violation
#
                else:
                    if ent[3] < 0:
                        line = line + '<td><em class="red">' 
                        line = line + '(' +  str(ent[1]) + ')</em></td>\n'
                    else:
                        line = line + '<td><em class="red">' 
                        line = line + str(ent[1]) + '</em></td>\n'

        line = line + '<td>(' + str(l_dict[msid][0]) + ')<br>' 
        line = line + str(l_dict[msid][1]) + '</td>\n'
        line = line + '<td>(' + str(l_dict[msid][2]) + ')<br>' 
        line = line + str(l_dict[msid][3]) + '</td>\n'
        line = line + '<td>' + u_dict[msid] + '</td><td>' + d_dict[msid] + '</td></tr>\n\n'

    line = line + '</table>\n'

    return line

#-------------------------------------------------------------------------------
#-- get_limit_values: create limits, description, and unit dictionary        ---
#-------------------------------------------------------------------------------

def get_limit_values():
    """
    create limits, description, and unit dictionary
    input: none, but read from /data/mta4/MTA/data/op_limits/op_limits.db'
    output: l_dict  --- a dictionary <msid> <---> [<y low>, <y up>, <r low>, <r up>]
            d_dict  --- a dictionary of <msid> <---> description
            u_dict  --- a dictionary of <msid> <---> unit
    """
    infile = f"{LIMIT_DIR}/op_limits.db"
    with open(infile, 'r') as f:
        data = [line.strip() for line in f.readlines()]
    l_dict = {}
    d_dict = {}
    u_dict = {}
    for ent in data:
        if ent[0] == '#':
            continue
        atemp = re.split('\t+', ent)
        msid  = atemp[0]
        l_ent = [float(atemp[1]), float(atemp[2]), float(atemp[3]), float(atemp[4])]
        l_dict[msid] = l_ent

        atemp = re.split('#', ent)
        try:
            btemp = re.split('\s+', atemp[1])
            try:
                unit  = btemp[-2]
                ctemp = re.split(unit, atemp[1])
                desc  = ctemp[0].strip()
            except:
                unit  = ''
                desc  = atemp[1]
        except:
            unit = ''
            desc = ''

        d_dict[msid] = desc
        u_dict[msid] = unit

    return [l_dict, d_dict, u_dict]

#-------------------------------------------------------------------------------
#-- make_time_stamp_list: make a list of time stamps in the fromat of <yyyy><mm><dd>
#-------------------------------------------------------------------------------

def make_time_stamp_list(start, stop):
    """
    make a list of time stamps in the fromat of <yyyy><mm><dd>
    input:  start   --- start time in seconds from 1998.1.1
            stop    --- stop time in seconds from 1998.1.1
    output: tlist   --- a list of n day time stamps in the format of <yyyy><mm><dd>
    """
#
#--- make sure that the chandra time is well into the date of starting and stopping
#
    start += 3600
    stop  += 7200

    dtime  = start
    tlist  = []
#
#--- repeat until the list is filled
#
    while(dtime < stop):
        out   = Chandra.Time.DateTime(dtime).date
#
#--- drop the decimal part of the second; time.strptime cannot handle it
#
        atemp = re.split('\.', out)
        out   = atemp[0]
        out   = time.strftime('%Y%m%d', time.strptime(out, '%Y:%j:%H:%M:%S'))
        tlist.append(out)
        dtime += 86400.

    return tlist

#-------------------------------------------------------------------------------
#-- convert_date_format: convert date format from <yyyy><mm><dd> to <mm>/<dd>/<yy>
#-------------------------------------------------------------------------------

def convert_date_format(date):
    """
    convert date format from <yyyy><mm><dd> to <mm>/<dd>/<yy>
    input:  date    --- date in <yyyy><mm><dd>
    output: sdate   --- date in <mm>/<dd>/<yy>
    """
    year = date[2] + date[3]
    mon  = date[4] + date[5]
    day  = date[6] + date[7]

    sday = mon + '/' + day + '/' + year

    return sday

#-------------------------------------------------------------------------------
#-- clean_up_msid_list: remove duplicated and unimportant misds                -
#-------------------------------------------------------------------------------

def clean_up_msid_list(o_list):
    """
    remove duplicated and unimportant misds
    input:  o_list  --- a list of msids
    output: o_list  --- cleaned up list
    """

    r_set  = set(o_list)
    o_list = []
    for msid in r_set:
#
#--- drop unimportant msid
#
        mc1 = re.search('TB1T', msid)
        mc2 = re.search('OHRT', msid)
        mc3 = re.search('OOB',  msid)
        mc4 = re.search('4MP',  msid)
        mc5 = re.search('4RT',  msid)
        mc6 = re.search('GRD',  msid)
        if mc1 is not None:
            continue
        if mc2 is not None:
            continue
        if mc3 is not None:
            continue
        if mc4 is not None:
            continue
        if mc5 is not None:
            continue
        if mc6 is not None:
            continue
#
#--- if the unit is C, drop
#
        try:
            unit = u_dict[msid]
            if unit == 'C':
                continue
        except:
            pass

        o_list.append(msid)
        o_list.sort()

    return o_list

#-------------------------------------------------------------------------------

if __name__ == "__main__":

    start = float(sys.argv[1])
    stop  = float(sys.argv[2])

    line  = get_telem_data(start, stop)

    with open('telem_table', 'w') as fo:
        fo.write(line)
