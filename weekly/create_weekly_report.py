#!/proj/sot/ska3/flight/bin/python

#############################################################################
#                                                                           #
#           create_weekly_report.py: create weekly report                   #
#                                                                           #
#           author: t. isobe (tisobe@cfa.harvard.edu)                       #
#                                                                           #
#           Last Update: Oct 12, 2022                                       #
#                                                                           #
#############################################################################

import sys
import os
import re
import time
import datetime
import Chandra.Time
import argparse
import getpass
import traceback
#
#--- Define directory pathing
#
BIN_DIR = "/data/mta/Script/Weekly/Scripts"
TEMPLATE_DIR = f"{BIN_DIR}/Templates"
DATA_DIR = "/data/mta/Script/Weekly/Data"
WEB_DIR = "/data/mta4/www/REPORTS"
CTI_DIR = "/data/mta_www/mta_cti"
SIM_DATA_DIR = "/data/mta/Script/SIM_move/Data"
sys.path.append(BIN_DIR)

import find_focal_temp_peaks    as fftp
import plot_acis_focal_temp     as paft
import create_telem_table       as ctt
import create_bad_pixel_table   as cbpt
import find_recent_observations as frobs
from calendar import month_abbr

#
#--- admin email addresses (list) including those passed through sys args
#
ADMIN  = ['mtadude@cfa.harvard.edu']
#
#--- ephin linst
#
ephtv_list = ['5EIOT', '5EPHINT', 'HKEBOXTEMP', 'HKGHV', 'HKN6I', 'HKN6V', 'HKP27I',\
              'HKP27V', 'HKP5I', 'HKP5V', 'HKP6I', 'HKP6V', 'TEIO', 'TEPHIN']
#
#--- instrument trend list
#
inst_list = ['SIM', 'PCAD', 'Ground Computed Gradients', 'Spacecraft Bus and Subsystem Trends',\
             'OBA Thermal', 'HRMA Thermal', 'Gratings', 'ACIS', 'HRC', 'Ground Computations', 'EPHIN']

#------------------------------------------------------------------------------------------
#-- create_weekly_report: main script to create the weekly report for the week          ---
#------------------------------------------------------------------------------------------

def create_weekly_report(date, year, debug = 0):
    """
    main script to set up the weekly report template for the week
    input:  date    --- date in the format of mmdd (e.g. 0910)
            year    --- year in the format of yyyy (e.g. 2015)
            debug   --- if it is other than 0, print out some output
    output: weekly report /data/mta4/www/REPORT/<yyyy>/<mm><dd>.html
                          /data/mta4/www/REPORT/<yyyy>/<mm><dd>_fptemp.png
            it also creates local copies in <data_dir>
    """
#
#--- one day in seconds
#
    oned  = 86400
#
#--- set various time formats
#
    syear = str(year)                       #--- 4 digit year
    yrd2  = year[2] + year[3]               #--- 2 digit year
    year  = int(float(year))                #--- integer year
    
    date  = str(date)

    smon  = date[0] + date[1]               #--- two digit month
    mon   = int(float(smon))                #--- integer month
    lmon = month_abbr[mon]                  #--- month in letter (e.g.Mar)

    sday  = date[2] + date[3]               #--- two digit mday
    day   = int(float(sday))                #--- integer mday

    ltime = str(year) + ':' + str(mon) + ':' + str(day) + 'T00:00:00'
    ltime = time.strftime('%Y:%j:%H:%M:%S', time.strptime(ltime, '%Y:%m:%dT%H:%M:%S'))
    stop  = int(Chandra.Time.DateTime(ltime).secs) + oned
    atemp = re.split(':', ltime)
    ydate = int(float(atemp[1]))

    day_n = stop - 7 * oned
    
    day01 = stop - 5 * oned
    day0  = stop - 6 * oned
    lday0 = stime_to_ddate(day0)
    sday0 = sdate_to_ldate(lday0)
    start = day0
    lday1 = stime_to_ddate(day01)

    tout  = Chandra.Time.DateTime(day0).date
    atemp = re.split('\.', tout)
    tout  = atemp[0]

    ttemp = re.split(':', tout)
    iru_start  = str(ttemp[0]) + '_' + str(ttemp[1])
#
#---  year of the beginning of the period; could be different from that of the end
#
    byear      = ttemp[0]    

    lday0 = stime_to_ddate(day0)

    day1  = stop - 5 * oned
    lday1 = stime_to_ddate(day1)

    day2  = stop - 4 * oned
    lday2 = stime_to_ddate(day2)
    sday2 = sdate_to_ldate(lday2)

    day3  = stop - 3 * oned
    lday3 = stime_to_ddate(day3)

    day4  = stop - 2 * oned
    lday4 = stime_to_ddate(day4)
    sday4 = sdate_to_ldate(lday4)

    day5  = stop - 1 * oned
    lday5 = stime_to_ddate(day5)

    day6  = stop 
    lday6 = stime_to_ddate(day6)
    sday6 = sdate_to_ldate(lday6)

    tout  = Chandra.Time.DateTime(day6).date
    atemp = re.split('\.', tout)
    tout  = atemp[0]

    ttemp = re.split(':', tout)
    iru_stop    = '_' + str(ttemp[1])

    day7  = stop + 1 * oned
    lday7 = stime_to_ddate(day7)

#
#---- setting file name
#
    atemp = re.split('\/', lday6)
    file_date  = atemp[0] + atemp[1]
    file_date2 = atemp[0] + '/' + atemp[1]
    file_name  = file_date + '.html'
#
#--- title
#
    titledate     = lday0 + ' - ' + lday6

    ldate         = sdate_to_ldate(lday6)

#
#--- focal temp file name
#
    paft.plot_acis_focal_temp(year, ydate)
    fftp.find_focal_temp_peaks(year, mon, day, 0.3)

    fptemp        = file_date + '_fptemp.png'
    fpext_range   = str(start)+' '+  str(stop)
    fpstart       = str(start)
    fplsub        = '"'+ sday0 + '", "' + sday2  + '", "' +  sday4  + '", "' + sday6 + '"'
    fpdsub        = str(day0) + ', ' + str(day2) + ', ' + str(day4) + ', ' + str(day6)
#
#--- IRU span
#
    irudate       = iru_start + iru_stop
    irudate       = str(syear) + '/' +  iru_start + iru_stop 
#
#--- telemetry data table
#
    tel_start = start - oned
    tel_stop  = stop  - oned
    telem_table   = ctt.get_telem_data(tel_start, tel_stop)
#
#--- bad pixcel data table
#
    bad_pix_table = cbpt.create_bad_pixel_table()
#
#--- recent observation table
#
    ro_stop = stop + oned
    r_obs_table   = frobs.find_recent_observations(ro_stop)
#
#--- find trending dates/title
#
    it_date = syear + ':' + smon + ':' + sday
    [title, c_t_date, last_trend_date] = find_inst_trend_name(it_date)
#
#--- index.html input
#
    s1 = sday0[0:3] + ' ' + sday0[3:5]
    s2 = sday6[0:3] + ' ' + sday6[3:5]
    index = '<td> <a href="./' + str(year) + '/' + file_date + '.html">' + s1 + ' - ' + s2 + '</a>'
#
#--- debugging output
#
    if debug != 0:
        print("file_name; "       + file_name )
        print("title date: "      + titledate)
        print("ldate: "           + ldate)
        print("fptemp: "          + fptemp)
        print("fpext_range: "     + fpext_range)
        print("fpstart: "         + fpstart)
        print(" fplsub: "         + fplsub)
        print(" fpdsub: "         + fpdsub)
        print("irudate: "         + irudate)
        print("title: "           + title)
        print("last_trend_date: " + last_trend_date)
        print("index: "           + index)
#
#--- create a work directory
#
    outdir = f"{DATA_DIR}/{ldate}/"
    cmd = 'mkdir -p ' + outdir
    os.system(cmd)
#
#--- read the template for the weekly, and start replacing dates etc
#
    tfile = f"{TEMPLATE_DIR}/this_week"
    with open(tfile, 'r') as f:
        linput = f.read()

    linput = linput.replace('#DDATE#',   file_date)
    linput = linput.replace('#IRUSPAN1#', irudate)
    linput = linput.replace('#IRUSPAN2#', irudate)
    linput = linput.replace('#TITLE#',    title)
    linput = linput.replace('#TITLEDATE#',titledate)

    atemp  = last_trend_date
    atemp  = atemp.replace('/', '')
    linput = linput.replace('#PREVREPORT#', atemp)

    atemp  = re.split('/', last_trend_date)
    pmon   = int(float(atemp[0]))
    lmon = month_abbr[pmon]
    line   = lmon + ' ' + atemp[1]
#
#--- the previous report could be from the last year
#
    ryear = syear
    if mon < pmon:
        ryear = year -1
        ryear = str(ryear)

    linput = linput.replace('#RYEAR#',      ryear)

    linput = linput.replace('#PREVDATE#',   line)

    atitle = str(title)
    atitle = atitle.replace(' ', '_')

    [temp1, temp2, temp3, temp4, temp5, temp6, temp7, temp8] = read_cti_values()
    linput = linput.replace('#ATEMP#',  temp3)
    linput = linput.replace('#ATEMP2#', temp4)
    linput = linput.replace('#DTEMP#',  temp7)
    linput = linput.replace('#DTEMP2#', temp8)

    [val, step, tval] = read_sim()
    linput = linput.replace('#WSTEP#', step)
    linput = linput.replace('#WMOVE#', val)
    linput = linput.replace('#TMOVE#', tval)
#
#--- read the  focal temp peak list
#
    tstop = stop + 86400
    [fcnt, fdata] = read_focal_temp_data(fptemp, outdir)

    linput = linput.replace('#TEMPPEAK#', str(fcnt))
    linput = linput.replace('#TEMPLIST#', fdata)
#
#--- bad pixel
#
    linput = linput.replace('BAD_PIXEL_TABLE', bad_pix_table)
#
#--- photon
#
    linput = linput.replace('PHOTON_TABLE', r_obs_table)
#
#--- telem data
#
    linput = linput.replace('TELEM_TABLE', telem_table)
#
#--- trend data
#
    trend = set_trend_data_input(title)
    linput = linput.replace('#TREND#', trend)
#
#--- write out the weekly report
#
    ofile = outdir + file_name
    with  open(ofile, 'w') as fo:
        fo.write(linput)
#
#--- move files
#
    move_files(date, year, outdir, file_name, fptemp, WEB_DIR)
#
#--- send out email to admin; notify the job complete
#
    if len(ADMIN) > 0:
        send_email_to_admin(date, year)


#----------------------------------------------------------------------------------
#-- stime_to_ddate: change data in second from 1998.1.1 to mm/dd/yy format       --
#----------------------------------------------------------------------------------

def stime_to_ddate(stime):
    """
    change data in second from 1998.1.1 to mm/dd/yy format
    input:  stime   --- time in seconds from 1998.1.1
    output: dtime   --- date in the form of mm/dd/yy (e.g. 08/19/15)
    """
    tlist       = Chandra.Time.DateTime(stime).date
    atemp       = re.split('\.', tlist)
    dtime       = time.strftime('%m/%d/%y', time.strptime(atemp[0], '%Y:%j:%H:%M:%S'))

    return dtime

#----------------------------------------------------------------------------------
#-- sdate_to_ldate: change date in second from 1998.1.1 to MMMdd                 --
#----------------------------------------------------------------------------------

def sdate_to_ldate(sdate):
    """
    change date in second from 1998.1.1 to MMMdd
    input:  stime   --- time in seconds from 1998.1.1
    output: ldate   --- date in form of MMMdd (e.g. Aug19)
    """

    atemp = re.split('\/', sdate)
    mon   = int(float(atemp[0]))
    lmon = month_abbr[mon]

    ldate = lmon + atemp[1]

    return ldate

#----------------------------------------------------------------------------------
#-- read_cti_values: read cti values from the fitting result files               --
#----------------------------------------------------------------------------------

def read_cti_values():
    """
    read cti values from the fitting result file
    input:  none but read from /data/mta_www/mta_cti/Plot_adjust/fitting_result etc
    output: ftemp1  --- Adjucted cti in CTI/year
            ftemp2  --- Adjucted cti in CTI/day
            ftemp3  --- Detrended cti in CTI/year
            ftemp4  --- Detrended cti in CTI/day
    """

    ifile = f'{CTI_DIR}/Plot_adjust/fitting_result'

    [ftemp1, ftemp2, ftemp3, ftemp4] = read_cti(ifile)

    ifile = f'{CTI_DIR}/Det_Plot_adjust/fitting_result'

    [ftemp5, ftemp6, ftemp7, ftemp8] = read_cti(ifile)

    return [ftemp1, ftemp2, ftemp3, ftemp4, ftemp5, ftemp6, ftemp7, ftemp8]


#----------------------------------------------------------------------------------
#-- read_cti:  find a cti value from the file                                    --
#----------------------------------------------------------------------------------

def read_cti(ifile):
    """
    find a cti value from the file
    input:  file    ---- the file name
    output: ftemp1  ---- cti in CTI/year
            ftemp2  ---- cti in CTI/day
            ftemp3  ---- cti in CTI/year
            ftemp4  ---- cti in CTI/day
    """
    with open(ifile) as f:
        data = [line.strip() for line in f.readlines()]

    chk = 0
    for ent in data:
        if chk == 0:
            mc = re.search('mn K alpha', ent)
            if mc is not None:
                chk = 1
                continue
            else:
                continue

        elif chk == 1:
            mc = re.search('ACIS-I Average:', ent)
            if mc is not None:
                chk = 2
                continue
            else:
                continue

        elif chk == 2:
            ent = ent.strip()
            atemp = re.split('\s+', ent)
            try:
                val   = float(atemp[0])
            except:
                continue 

            yval  = val / 365.0
            ftemp1 = '%2.3e' % val
            ftemp2 = '%2.3e' % yval
            chk = 3
            continue

        elif chk == 3:
            ent = ent.strip()
            atemp = re.split('\s+', ent)
            try:
                val   = float(atemp[0])
            except:
                continue

            yval  = val / 365.0
            ftemp3 = '%2.3e' % val
            ftemp4 = '%2.3e' % yval
            chk = 4
            continue

        elif chk == 4:
            break
                
    return [ftemp1, ftemp2, ftemp3, ftemp4]

#----------------------------------------------------------------------------------
#-- read_sim: read sim movement values from weekly averaged page                 --
#----------------------------------------------------------------------------------

def read_sim():
    """
    read sim movement values from weekly averaged page
    input:  none, but read from /data/mta_www/mta_sim/wksum.html
    output: val     --- weekly average time/step
            step    --- weekly counts of TSC moves
            val     --- mission total average time/step
    """
    with open(f'{SIM_DATA_DIR}/weekly_report_stat') as f:
        data = [line.strip() for line in f.readlines()]
    atemp = re.split('\s+', data[0])
    tval  = '%1.5f' % float(atemp[1])
    step  = atemp[2]
    val   = '%1.5f' %float(atemp[3])

    return [val, step, tval]

#----------------------------------------------------------------------------------
#-- read_focal_temp_data: run focal temp script and create a plot, read a table  --
#----------------------------------------------------------------------------------

def read_focal_temp_data(fptemp, outdir):
    """
    read output of find_focal_temp_peaks.py and get focal temp information
    input:  fptemp  --- plot output name
            outdir  --- output directory
            read '/data/mta/Script/Weekly/Data/Focal/focal_temp_list'
    output: fcnt    --- number of peaks observed
            fdata   --- table input
    """
#
#--- read the html table entries
#
    with open(f"{DATA_DIR}/Focal/focal_temp_list") as f:
        data = [line.strip() for line in f.readlines()]

    fcnt  = len(data)
    fdata = ''
    for ent in data:
        fdata = fdata + ent + '\n'
#
#--- move the plot to an appropriate place
#
    os.system(f"cp {DATA_DIR}/Focal/acis_focal_temp.png {outdir}/{fptemp}")

    return [fcnt, fdata]

#----------------------------------------------------------------------------------
#-- read_focal_temp_output: read the focal temperature output and adjust it for better look 
#----------------------------------------------------------------------------------

def read_focal_temp_output():
    """
    read the focal temperature output and adjust it for better look
    input:  none, but read from forcal temp script output
    output: fcnt    --- number of peaks
            out     --- adjucted table
    """
    with open("./out") as f:
        data = [line.strip() for line in f.readlines()]

    rows = []
    chk  = 0
    for ent in data:
        if chk == 0:
            mc = re.search('ALT', ent)
            if mc is not None:
                chk = 1
                continue
        else:
            mc1 = re.search('<tr',  ent)
            mc2 = re.search('</tr', ent)
            if mc1 is not None:
                save = ent
            elif mc2 is not None:
                save = save + ent
                rows.append(save)

    out = ''
    for ent in rows:
        atemp = re.split('<td>', ent)
        line  = '<tr align=center>'
        for i in range(1, 4):
            btemp = re.split('</td>', atemp[i])
            val   = float(btemp[0])
            val   = "%3.2f" % (val)
            val   = str(val)
            line  = line + '<td>' + val + '</td>'
        out = out +  line + '<td align=left>&#160</td></tr>\n'
        
    fcnt = len(rows)

    return [fcnt, out]


#---------------------------------------------------------------------------------
#-- set_trend_data_input: create trend data table input                         --
#---------------------------------------------------------------------------------

def set_trend_data_input(title):
    """
    create trend data table input
    input:  title   --- the group name of the msids
    output: out     --- string  of the html table of the trend
    """
#
#--- read msid lists
#
    title  = title.replace(' ', '_')
    ltitle = title.lower()

    with open(f"{TEMPLATE_DIR}/Headers/Dsave/{ltitle}") as f:
        data = [line.strip() for line in f.readlines()]
#
#--- read header file
#
    with open(f"{TEMPLATE_DIR}/Headers/{title}") as f:
        hdata = [line.strip() for line in f.readlines()]
#
#--- read group display name
#
    with open(f"{TEMPLATE_DIR}/Headers/group_name") as f:
        out = [line.strip() for line in f.readlines()]

    g_dict = {}
    for ent in out:
        atemp = re.split(':', ent)
        g_dict[atemp[0]] = atemp[1]

#
#--- create a dictionary which contains table name (e.g.acistemp.html) as key
#--- and head lines as data
#
    chk = 0
    hdict = {}
    for ent in hdata:
        if chk == 0:
            atemp = re.split('.html', ent)
            fname = atemp[0]
            line = ''
            chk = 1
        else:
            if ent == "<-->":
                hdict[fname] = line + '\n\n'
                chk = 0
                continue
            else:
                line = line + ent + '\n'
#
#--- go around all the data for the week
#
    out = '' 
    for ent in data:
        atemp     = re.split('<>', ent)
        btemp     = re.split('.html', atemp[0])
        group     = btemp[0]
        msid_list = re.split(':', atemp[1])
#
#--- read the current data from the web page
#
        disp = g_dict[group]
        save =  create_html_table(group, disp,  msid_list)
        out  = out + save
        out  = out + '</table>\n</ul >\n<br />\n\n\n'

    return out

#----------------------------------------------------------------------------------
#-- move_files: move the created files to the report directory                   --
#----------------------------------------------------------------------------------

def move_files(date, year, out_dir, file_name, fptemp, hodir):
    """
    move the created files to the report directory
    input:  date        --- input date
            year        --- year of the data
            out_dir     --- output directory
            file_name   --- name of the output file
            fptemp      --- name of forcal temp gif file
            hodir       --- output directory
    output: /data/mta4/www/REPORTS/yyyy/mmdd.html and focal temp plot
    """
    html_dir = f"{hodir}/{year}"
#
#--- when year changes, you need to create a new output directory
#
    if os.path.isdir(html_dir) == False:
        os.system(f"mkdir -p {html_dir}")

    ofile    = out_dir + file_name
    pngfile  = out_dir + fptemp
    cmd      = 'cp ' + ofile + ' ' + pngfile + ' ' +  html_dir
    os.system(cmd)

    cmd      = 'chmod 775 '      + html_dir + '/*'
    os.system(cmd)

    cmd      = 'chgrp mtagroup ' + html_dir + '/*'
    os.system(cmd)

#----------------------------------------------------------------------------------
#-- send_email_to_admin: send out a notification email to admin                  --
#----------------------------------------------------------------------------------

def send_email_to_admin(date, year):
    """
    send out a notification email to admin
    input:  date        --- input date
            year        --- year of the data
    output: email to admin
    """
    line = 'Weekly Report for ' + str(date) + ' (' + str(year) + ') is created. Please check, '
    line = line + 'especially radiation condition of the week. \n\n'
    line = line + 'https://cxc.cfa.harvard.edu/mta/REPORTS/' + str(year) + '/' + str(date) + '.html\n\n'
    line = line + "Don't forget to edit index file: /data/mta4/www/REPORTS/index.html.\n"

    cmd = f'echo "{line}" | mailx -s "Subject: Weekly Report for {str(date)} Created" {" ".join(ADMIN)}'
    os.system(cmd)

#----------------------------------------------------------------------------------
#-- send_error_to_admin: send out a error email to admin                         --
#----------------------------------------------------------------------------------

def send_error_to_admin(e):
    """
    send out a notification email to admin
    input:  e        --- sys.exception() error tracestack
    output: email to admin
    """
    et = time.localtime()
    dt_string = f"{et.tm_year}/{et.tm_mon}/{et.tm_mday} {et.tm_hour}:{et.tm_min}"
    line = f"Failure to generate weekly report at {dt_string}. Please check generating script at {__file__}.\n\n"
    line = line + f"{e}"

    os.system(f'echo "{line}" | mailx -s "Error in Weekly Report Script: {dt_string}" {" ".join(ADMIN)}')

#----------------------------------------------------------------------------------
#-- find_date_and_year_for_report: find nearest Thursday date                    --
#----------------------------------------------------------------------------------

def find_date_and_year_for_report():
    """
    find nearest Thursday date 
    input:  none
    output: date    --- date of the nearest Thu in the format of mmdd (e.g. 0910)
            year    --- year of the nearest Thu
    """
    now = datetime.datetime.now()
    thurs = diff = (3 - now.weekday()) % 7
    if diff > 0:
        diff -= 7
    thurs = now + datetime.timedelta(days = diff)
    return [f"{thurs.month:02}{thurs.day:02}", f"{thurs.year}"]

#-------------------------------------------------------------------------------
#- create_html_table: create table msid entries of the group                 ---
#-------------------------------------------------------------------------------

def create_html_table(group, disp, msid_list):
    """
    create table msid entries of the group
    input:  group       --- group name
            disp        --- dispaly name
            msid_list   --- a list of msid in the group
    output: hline       --- html table elements
    """
#
#--- check whether this is the sun angle group
#
    mc     = re.search('_att', group)
    if mc is not None:
        att = 1
        group = group.replace('_att', '')
    else:
        att = 0

    group  = group.lower()
    cgroup = group.capitalize()
#
#--- set a path and html link to the main page
#
    if att == 0:
        ifile = '/data/mta4/www/MSID_Trends/' + cgroup + '/' + group + '_mid_static_long_main.html'
        hgrp  = 'https://cxc.cfa.harvard.edu/mta/MSID_Trends/' + cgroup + '/' 
        hgrp  = hgrp + group + '_mid_static_long_main.html'
    else:
        ifile = '/data/mta4/www/MSID_Trends/' + cgroup + '/' + group + '_mid_long_sun_angle.html'
        hgrp  = 'https://cxc.cfa.harvard.edu/mta/MSID_Trends/' + cgroup + '/' 
        hgrp  = hgrp + group + '_mid_long_sun_angle.html'

    hline = '<ul><li><h3><a href="' + hgrp + '">' + disp + '</a></h3></li></ul>\n'
    hline = hline + '<table border=1 cellpadding=3 cellspacing=2 style="margin-left:auto;'
    hline = hline + 'margin-right:auto;text-align:center;">\n'

    if att == 0:
        hline = hline + '<th>MSID</th><th></th><th>Mean</th><th>RMS</th><th>Delta/Yr</th>'
        hline = hline + '<th>Delta/Yr/Yr</th><th>Unit</th><th>Description</th>\n'
    else:
        hline = hline + '<tr><th colspan=10>Select msid to open the Sun Angle Page</th></tr>\n'

    with open(ifile) as f:
        data = [line.strip() for line in f.readlines()]

    ccnt = 0
    for msid in msid_list:
        msid = msid.lower()
#
#--- normal msid case
#
        if att == 0:
            for  m in range(0, len(data)):
                ent = data[m]
                mc = re.search(msid, ent)
                if mc is None:
                    continue

                html  = 'https://cxc.cfa.harvard.edu/mta/MSID_Trends/' + cgroup + '/' 
                html  = html + msid.capitalize()  + '/' + msid + '_mid_static_long_plot.html' 
                hline = hline + '<tr><th><a href="javascript:WindowOpener3(\''
#
#--- check multi row entries
#
                mc = re.search('rowspan', ent)
                if mc is not None:
                    ctemp = re.split('>', ent)
                    dtemp = re.split('rowspan=', ctemp[0])
                    rowno = int(float(dtemp[1]))
                    hline = hline +  html + '\')" rowspan=' + str(rowno) + '>' + msid + '</th>'
                    for n in range(0, rowno):
                        line  = data[m+2*n]
#
#--- first line has a bit more info than the following lines
#
                        if n == 0:
                            atemp = re.split('</th>', line)
                            btemp = re.split('<th ',  atemp[1])
                            hline = hline + btemp[0] + '</tr>\n'
#
#--- other lines
#
                        else:
                            atemp = re.split('<th ', line)
                            hline = hline + '<td>&#160;</td>' +  atemp[0] + '</tr>\n'

#
#--- single row entry case
#
                else:
                    hline = hline +  html + '\')">' + msid + '</th>'
                    atemp = re.split('</th>', ent)
                    btemp = re.split('"',  atemp[0])
                    btemp = re.split('<th', atemp[2])
                    hline = hline + '<td>&#160;</td>' +  btemp[0] + '</tr>\n'

                break 
#
#--- sun angle has a special way to display the table
#
        else:
            for ent in data:
                mc = re.search(msid, ent)
                if mc is None:
                    continue
                html  = 'https://cxc.cfa.harvard.edu/mta/MSID_Trends/' + cgroup + '/' 
                html  = html + msid.capitalize()  + '/' + msid + '_mid_long_sun_angle.html' 

                hline = hline + '<th><a href="javascript:WindowOpener3(\''
                hline = hline +  html + '\')">' + msid + '</th>\n'
            ccnt += 1
            if ccnt >= 10:
                hline = hline + '</tr>\n<tr>\n'
                ccnt = 0
    if att == 1:
        hline = hline + '</tr>\n'

    return hline


#--------------------------------------------------------------------------------
#-- find_inst_trend_name: find which msid group is  this week's trending section 
#--------------------------------------------------------------------------------

def find_inst_trend_name(cdate=''):
    """
    find which msid group is  this week's trending section
    input:  cdate    --- the date of report. if '', use the closest past Thu date
    output: inst    --- group name
            dtime   --- the date of the report in <mm>/<yy>
            ptim    --- the date of the last time this group was reported in <mm>/<yy>
    """

    base_time = 695537994               #--- 2020:016:05:00:00 starting from "SIM"
    day7      = 86400 * 7
    
#
#--- for the case the date is given, find the thu before that date
#
    if cdate != '':
        today_dt   = datetime.datetime.strptime(cdate, '%Y:%m:%d')
        ltime      = time.strftime('%Y:%j:05:00:00', time.strptime(cdate, '%Y:%m:%d'))
#
#--- otherwise, find the last Thu
#
    else:
        today_dt   = datetime.date.today()
        ltime      = str(today_dt).strip()
        ltime      = time.strftime('%Y:%j:05:00:00', time.strptime(ltime, '%Y-%m-%d'))
#
#--- given date in seconds from 1998.1.1
#
    today_s    = int(Chandra.Time.DateTime(ltime).secs)
#
#--- find the last Thu (a Thu week before)
#
    idx = (today_dt.weekday() + 1) % 7
    last_thu   = today_dt - datetime.timedelta(7+idx -4)
    last_thu   = str(last_thu)
#
#--- sometime the date come with 00:00:00. get rid of that part
#
    mc         = re.search(':', last_thu)
    if mc is not None:
        atemp    = re.split('\s+', last_thu)
        last_thu = atemp[0]
#
#--- convert into seconds from 1998.1.1
#
    ltime      = time.strftime("%Y:%j:05:00:00", time.strptime(last_thu, '%Y-%m-%d'))
    last_thu_s = int(Chandra.Time.DateTime(ltime).secs)
#
#--- we want nearest Thu, not the Thu of the last week if there is another thu between
#
    diff       = today_s - last_thu_s
    if diff >= day7:
        last_thu_s += day7
#
#--- find which instrument is due on that date
#
    dlen  = len(inst_list)
    dspan = dlen * day7
    diff  = last_thu_s - base_time

    while diff > dspan:
        diff -= dspan 
        if diff < dspan:
            break

    tdiff = int(diff / day7)
    pos = int(tdiff - dlen*int(tdiff /dlen))
    inst = inst_list[pos]
#
#--- date of that Thu
#
    dtime = convert_stime_to_trend_date(last_thu_s)
#
#--- the date of the last time this instrument is selected
#
    ptime =  convert_stime_to_trend_date(last_thu_s - dlen * day7 + 1)

    return [inst, dtime, ptime]


#--------------------------------------------------------------------------------
#-- convert_stime_to_trend_date: convert stime into mm/dd format               --
#--------------------------------------------------------------------------------

def convert_stime_to_trend_date(stime):
    """
    convert stime into mm/dd format
    input:  stime   --- time in seconds from 1998.1.1
    output: dtime   --- time in mm/dd
    """

    ltime = Chandra.Time.DateTime(stime).date
    atemp = re.split('\.', ltime)
    ltime = atemp[0]
    dtime = time.strftime('%m/%d', time.strptime(ltime, '%Y:%j:%H:%M:%S'))

    return dtime

#------------------------------------------------------------------------------------------

if __name__ == "__main__":
#
#--- Send notification email if script fails to run
#
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mode", choices = ['flight','test'], required = True, help = "Determine running mode.")
    parser.add_argument("-p", "--path", required = False, help = "Directory path to determine output location of report.")
    parser.add_argument("-d", "--date", required = False, help = "Date of thursday (format yyyy/mm/dd) of weekly report.")
    parser.add_argument("-e", '--email', nargs = '*', required = False, help = "list of emails to recieve notifications")
    args = parser.parse_args()

#
#--- Determine Date Information
#

    if args.date:
        date_info = args.date.split("/")
        if len(date_info) != 3:
            parser.error(f"Provided date: {args.date} must be in yyyy/mm/dd format")
        year = date_info[0]
        date = date_info[1] + date_info[2]
    else:
#
#--- If date is not provided, find the nearest thursday
#
        try:
            [date, year] = find_date_and_year_for_report()
            print(f"Weekly Report Date: {year}/{date}")
        except Exception as exc:
            e = ''.join(traceback.format_exception(exc))
            send_error_to_admin(e)
            traceback.print_exc()

    if args.mode == "test":
#
#--- Check that the machine running this test can view all network directories before running
#
        import platform
        machine = platform.node()
        if machine not in ['r2d2-v.cfa.harvard.edu', 'c3po-v.cfa.harvard.edu']:
            parser.error(f"Need r2d2-v or c3po-v to view /data/mta/www. Current machine: {machine}")
#
#--- Redefine Admin for sending notification email in test mode
#       
        if args.email != None:
            ADMIN = args.email
        else:
            ADMIN = [os.popen(f"getent aliases | grep {getpass.getuser()} ").read().split(":")[1].strip()]

#
#--- Path output to same location as unit pytest
#
        BIN_DIR = f"{os.getcwd()}"
        TEMPLATE_DIR = f"{BIN_DIR}/Templates"
        OUT_DIR = f"{BIN_DIR}/test/outTest"
        if args.path:
            OUT_DIR = args.path
        DATA_DIR = f"{OUT_DIR}/Data"
        WEB_DIR = f"{OUT_DIR}/REPORTS"
        os.makedirs(DATA_DIR, exist_ok = True)
        os.makedirs(f"{DATA_DIR}/Focal", exist_ok = True)
        os.makedirs(WEB_DIR, exist_ok = True)
#
#--- Cycle thorugh imported modules, changing their global pathing
#
        mod_group = [fftp, paft, ctt, cbpt, frobs]
        for mod in mod_group:
            if hasattr(mod, 'BIN_DIR'):
                mod.BIN_DIR = BIN_DIR
            if hasattr(mod, 'DATA_DIR'):
                mod.DATA_DIR = DATA_DIR
            if hasattr(mod, 'WEB_DIR'):
                mod.WEB_DIR = WEB_DIR
        create_weekly_report(date, year)

    else:
#
#--- Create a lock file and exit strategy in case of race conditions
#
        try:
            name = os.path.basename(__file__).split(".")[0]
            user = getpass.getuser()
            if os.path.isfile(f"/tmp/{user}/{name}.lock"):
                sys.exit(f"Lock file exists as /tmp/{user}/{name}.lock. Process already running/errored out. Check calling scripts/cronjob/cronlog.")
            else:
                os.system(f"mkdir -p /tmp/{user}; touch /tmp/{user}/{name}.lock")
            
            create_weekly_report(date, year)
#
#--- Remove lock file once process is completed
#
            os.system(f"rm /tmp/{user}/{name}.lock")
        except Exception as exc:
            e = ''.join(traceback.format_exception(exc))
            send_error_to_admin(e)
            traceback.print_exc()
