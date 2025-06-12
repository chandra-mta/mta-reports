#!/proj/sot/ska3/flight/bin/python

#############################################################################################
#                                                                                           #
#   find_focal_temp_peaks.py: find acis focal temperature peack postion, temp, and width    #
#                                                                                           #
#               author: t. isobe (tisobe@cfa.harvard.edu)                                   #
#                                                                                           #
#               last update: Oct 08, 2021                                                   #
#                                                                                           #
#############################################################################################

import os
import sys
import re
import time
import numpy
from datetime import datetime
import Chandra.Time
import unittest
from calendar import isleap

#
#--- from ska
#
from Ska.Shell import getenv, bash
ascdsenv = getenv('source /home/ascds/.ascrc -r release; source /home/mta/bin/reset_param', shell='tcsh')

#
#--- Define directory pathing
#
BIN_DIR = "/data/mta/Script/Weekly/Scripts"
DATA_DIR = "/data/mta/Script/Weekly/Data"
FOCAL_DIR = "/data/mta/Script/ACIS/Focal/Data"

sys.path.append(BIN_DIR)

BTFMT    = '%m/%d/%y,%H:%M:%S'
basetime = datetime.strptime('01/01/98,00:00:00', BTFMT)

#-----------------------------------------------------------------------------------------------
#-- find_focal_temp_peaks: estimate focal temperature peak position, temperature, and the peak width
#-----------------------------------------------------------------------------------------------

def find_focal_temp_peaks(year='', month='', mday='', tdiff=''):
    """
    estimate focal temperature peak position, temperature, and the peak width.
    the data are collected Thu - Thu span close to the date given.
    input:  year    --- year of the period. default: '' (means the current year)
            month   --- month of the period, default: '' (mean the current month)
            mday    --- date of the month   defalut: '' (menas today's date)
    output: focal_temp_list:    a file contain, peak positioin, temperature, and the peak width
    """
#
#--- find time spans that the data will be collected (Thu - Thu period)
#
    [tstart, tstop, dstart, dstop] = find_time_span(year, month, mday)
#
#--- extract data for the period. the temperature values are smoothed
#
    out   = Chandra.Time.DateTime(tstop).date
    atemp = re.split(':', out)
    tyear = int(atemp[0])
    yday  = int(atemp[1])
    [time_set, temp_set] = read_focal_temp(tyear, yday, tstart, tstop)
#
#--- find peak position, temperature and width of the peak
#
    peak_list = select_peak(time_set, temp_set, tdiff)
    peak_list = clean_up_peak_list(peak_list)
    peak_list = convert_to_readable(peak_list)
#
#--- if the top of the peak from the last period is in the plotting range, keep it; otherwise drop
#
    stime  = Chandra.Time.DateTime(tstop).secs
    yb_cut = stime - 8 * 86400
    line = ''
    yb_cut = yday - 8
    for ent in peak_list:
        vtime = float(ent[0])
        if vtime < yb_cut:
            continue
        if vtime >= stime:
            continue

        ltime  = adjust_digit_format(vtime)
        comp   = float(ltime)
        ltemp  = adjust_digit_format(float(ent[1]))
        lwidth = adjust_digit_format(float(ent[2]), top =1)
        if dstart < dstop:
            if comp < dstart or comp > dstop:
                continue
        else:
            if comp < dstart and comp > dstop:
                continue

        line = line + '<tr align=center><td>' + str(ltime) + '</td>'
        line = line + '<td>'           + str(ltemp) + '</td>'
        line = line + '<td>'           + str(lwidth) + '</td></tr>\n'
#
#--- print out the data
#
    outfile = f"{DATA_DIR}/Focal/focal_temp_list"
    with  open(outfile, 'w') as fo:
        fo.write(line)
    
#-----------------------------------------------------------------------------------------------
#-- adjust_digit_format: adjust print out digit format                                        --
#-----------------------------------------------------------------------------------------------

def adjust_digit_format(val, top=3):
    """
    adjust print out digit format
    input:  val --- value
            top --- how many digit we need; default:3
    output: sval    --- format adjusted sting value
    """
    if val > 0:
        sign = ''
    else:
        sign = '-'

    val   = abs(val)
    sval  = str(val)
    atemp = re.split('\.', sval)
    p1    = atemp[0]
    p2    = atemp[1]

    p1_len = len(p1)
    if p1_len < top:
        if sign == '':
            for k in range(p1_len, top):
                p1 = '0' + p1
        else:
            sp = ''
            for k in range(p1_len, top):
                sp = ' ' + sp
                p1 = sp + '-' + p1
    if len(p2) < 2:
        p2 = p2 + '0'

    sval = p1 + '.' + p2
    if sign == '-':
        if float(sval) < 0.0:
            sval = ' ' + sval
        else:
            sval = '-' + sval

    return sval

#-----------------------------------------------------------------------------------------------
#-- find_time_span: find time span for Thu to Thu nearest to a given date                     --
#-----------------------------------------------------------------------------------------------

def find_time_span(year = '', month = '', mday = ''):
    """
    find time span for begining of Fri to the end of hu nearest to a given date
    input:  year    --- year.           default: current year
            month   --- month.          default: current month
            mday    --- month date.     default: today's date
    """
#
#--- if date is given, find week day and year date
#
    if year != '':    
        input_time = str(year) +':' + str(month) + ':' + str(mday)
        tlist = time.strptime(input_time, "%Y:%m:%d") 
        wday  = tlist.tm_wday
        yday  = tlist.tm_yday
        chk   = 0
#
#--- find today's date information (in local time)
#
    else:
        tlist = time.localtime()

        year  = tlist[0]
        mon   = tlist[1]
        day   = tlist[2]
        wday  = tlist[6]
        yday  = tlist[7]
        chk   = 1
#
#--- find the difference to Thursday. wday starts on Monday (0)
#--- and set data collect date span
#
        diff  = 4 - wday

        if diff != 0:
            yday += diff
            if yday < 1:
                year -= 1
                base = 365 + isleap(int(year))
                yday = base - yday

    syear  = year
    dstart = yday - 8
    syday  = yday - 7
    base = 365 + isleap(int(year))

    if syday < 0:
        syear -= 1
        syday  += base

    if dstart < 0:
        syear -= 1
        dstart += base
#
#--- convert time into seconds from 1998.1.1
#--- put extra 24 hrs before and 24 hrs after to cover the border line peak 
#
    if chk == 0:
        start = convertto1998sec(syear, syday) - 86400.0
    else:
        start = convertto1998sec(syear, syday) - 2* 86400.0

    stop  =  start + 86400.0 * 8
    dstop = dstart + 7
    if dstop > base:
        dstop -= base

    return [start, stop, dstart, dstop] 

#-----------------------------------------------------------------------------------------------
#-- convertto1998sec: convert time format from mm/dd/yy,hh:mm:ss to seconds from 1998.1.1    ---
#-----------------------------------------------------------------------------------------------

def convertto1998sec(year, yday):
    """
    convert time format from mm/dd/yy,hh:mm:ss to seconds from 1998.1.1
    input:  ftime      --- time in mm/dd/yy,hh:mm:ss or yyyy-mm-dd,hh:mm:ss
    output  stime      --- time in seconds from 1998.1.1
    """
    ftime = f"{year}:{yday:03}:00:00:00"
    sec1998 = Chandra.Time.DateTime(ftime).secs

    return sec1998


#-----------------------------------------------------------------------------------------------
#-- read_focal_temp: read focal plane temperature data                                        --
#-----------------------------------------------------------------------------------------------

def read_focal_temp(tyear, yday, tstart, tstop):
    """
    read focal plane temperature data
    input:  tyear   --- this year
            yday    --- today's y date
            tstart  --- starting time in seconds from 1998.1.1
            tstop   --- stopping time in seconds from 1998.1.1
    output: ftime   --- a list of time 
            focal   --- a list of focal temp
    """
#
#--- if y day is less than 8, read the data from the last year
#
    if yday < 8:
        ifile = f"{FOCAL_DIR}/focal_plane_data_5min_avg_{tyear-1}"
        data   = read_data_file_col(ifile, sep='\s+', c_len=2)
        if data[0] != 0:
            ftime  = data[0]
            focal  = data[1]
        else:
            ftime  = []
            focal  = []
    else:
        ftime  = []
        focal  = []
#
#--- otherwise, just read this year
#
    try:
        ifile = f"{FOCAL_DIR}/focal_plane_data_5min_avg_{tyear}"
        data   = read_data_file_col(ifile, sep='\s+', c_len=2)
        if data[0] != 0:
            ftime  = ftime + data[0]
            focal  = focal + data[1]
    except:
        pass
#
#--- select out the data for the last 7 days
#
    [ftime, focal] = select_data_by_date(ftime, focal, tstart, tstop)
    
    return [ftime, focal]

#-----------------------------------------------------------------------------------------------
#-- select_data_by_date: selet out the potion of the data by time                             --
#-----------------------------------------------------------------------------------------------

def select_data_by_date(x, y, tstart, tstop):
    """
    selet out the potion of the data by time
    input:  x       --- a list of time data
            y       --- a list of data
            tstart  --- a starting time in seconds from 1998.1.1
            tstop   --- a stopping time in seconds from 1998.1.1
    output: x       --- a list of time data selected
            y       --- a list of data selected
    """
    
    x   = numpy.array(x)
    y   = numpy.array(y)
    ind = x >= tstart
    x   = x[ind]
    y   = y[ind]

    ind = x <= tstop
    x   = list(x[ind])
    y   = list(y[ind])
    
    return [x, y]

#-----------------------------------------------------------------------------------------------
#-- smooth_data: take averages of temperatures and return lists of time and temperature lists --
#-----------------------------------------------------------------------------------------------

def smooth_data(time_set, temp_set):
    """
    take averages of temperatures and return lists of time and temperature lists
    input:  time_set    --- a list of time in seconds from 1998.1.1
            temp_set    --- a list of temperature 
    output: stime_set   --- a list of time in seconds from 1998.1.1
            stemp_list  --- a list of smoothed temperature 
    """
    
    tspan = 600                 #---- average is currently taken for 10 min span
    prev  = time_set[0]
    fsum  = temp_set[0]
    cnt   = 1
    stime_set = []
    stemp_set = []
    for i in range(1, len(time_set)):
        if time_set[i] < prev + tspan:
#
#--- drop extreme temperature cases; probably they are glitchs
#
            if temp_set[i] < -130:
                continue
            if temp_set[i] > -70:
                continue
            fsum += temp_set[i]
            cnt  += 1
        else:
            avg = fsum / cnt
            stime_set.append(prev+ 300)
            stemp_set.append(avg)
            prev = time_set[i]
            fsum = temp_set[i]
            cnt  = 1

    avg = fsum / cnt
    avg = '%.2f' % round(avg, 2)
    avg = float(avg)
    diff = 0.5 * (time_set[-1] + prev)
    stime_set.append(diff)
    stemp_set.append(avg)

    return[stime_set, stemp_set]

#-----------------------------------------------------------------------------------------------
#-- mving_avg_data: take a moving average                                                      -
#-----------------------------------------------------------------------------------------------

def mving_avg_data(time_set, temp_set):
    """
    take a moving average
    input:  time_set    ---- a list of time
            temp_set    ---- a set of temperature 
    output: stime_set   ---- a list of time
            stemp_set   ---- a list of smoothed temp list
    """
    tstep = 10                  #--- moving average is currently taken for 10 data points

    hstep = int(0.5 * tstep)
    stime_set = []
    stemp_set = []
    tlen      = len(time_set)

    for i in range(hstep+1 , tlen-hstep-1):
        sum = 0
        cnt = 0
        for m in range(0, tstep):
            step = i + m - hstep
            sum += temp_set[step]
            cnt += 1
        avg = sum / cnt
        avg = '%.2f' % round(avg, 2)
        avg = float(avg)
        stime_set.append(int(float(time_set[i])))
        stemp_set.append(avg)


    return [stime_set, stemp_set]

#-----------------------------------------------------------------------------------------------
#-- find_turning_point: find peaks and valleys of the focal temperature data                 ---
#-----------------------------------------------------------------------------------------------

def find_turning_point(time_set, temp_set):
    """
    find peaks and valleys of the focal temperature data
    input:  time_set    --- a list of time in seconds from 1998.1.1
            temp_set    --- a list of forcal temperature
    """

    up_list   = []
    down_list = []
#
#--- find whether the temperature is going up or down at the begining
#
    diff = temp_set[1] - temp_set[0]
    if diff >= 0:
        slope = 1
    else:
        slope = 0

    for i in range(1, len(time_set)):
        diff = temp_set[i] - temp_set[i-1]
        if diff >= 0:
            cslope = 1
        else:
            cslope = 0
#
#--- if the slope direction changes, we assume that we reached a peak (or a valley)
#
        if slope != cslope:
#
#---  the valley must be lower then -115
#
            if cslope == 1:
                if temp_set[i] < -115:
                    up_list.append(i)
#
#--- the peak must be higher than -118.5
#
            else:
                if temp_set[i] > -118.5:
                    down_list.append(i)
            slope = cslope

    up_list.append(len(time_set)-1)
    return [up_list, down_list]

#-----------------------------------------------------------------------------------------------
#-- select_peak: find a peak and valleys srrounding the peak                                 ---
#-----------------------------------------------------------------------------------------------

def select_peak(time_set,temp_set,tdiff=0.3):
    """
    find a peak and valleys surrounding the peak
    input:  time_set    --- a list of time in seconds from 1998.1.1
            temp_set    --- a list of temperatures
            tdiff       --- a criteria difference between peak and valley
    output: peak_list   --- a list  of lists of:
                [<peak position> <peak temp> <valley position1> <valley temp> <valley position2> <valley temp>]
    """

    [up_list, down_list] = find_turning_point(time_set, temp_set)

    line = ''
    for i in range(0, len(up_list)):
        k = up_list[i]
        try:
            k1 = up_list[i+1]
        except:
            k1 = up_list[len(up_list)-1]
        line = line +  '\t\t' + str(Chandra.Time.DateTime(time_set[k]).date) + "<--->" 
        line = line +  str(sec1998tofracday(time_set[k])) +"<--->" + str(temp_set[k]) + '\n'
        for m in down_list:
            if time_set[m] >= time_set[k] and time_set[m] <= time_set[k1]:
                line = line + '\nPEAK:' + str(Chandra.Time.DateTime(time_set[m]).date) + "<--->" 
                line = line + str(sec1998tofracday(time_set[m])) +"<--->" + str(temp_set[m]) + '\n\n'

    outfile = f"{DATA_DIR}/Focal/focal_peak_ref"
    with open(outfile, 'w') as fo:
        fo.write(line)

    peak_list = []
#
#--- find a peak and it temperature
#
    lstart = up_list[0]
    for k in down_list:
        htemp = temp_set[k]
        for m in range(0, len(up_list)):
            p1 = time_set[up_list[m]]
            try:
                p2 = time_set[up_list[m+1]]
                lchk = 0 
            except:
                p2 = time_set[-1]
                lchk = 1
            spoint = 0
#
#--- find two valleys around the peak
#
            if time_set[k] >= p1 and time_set[k] <= p2:
#
#--- if the temperature difference between the peak and the valley is less than 1 degree
#--- find the next closest valley point
#
                for l in range(m, 0, -1):
                    diff = htemp - temp_set[up_list[l]]
                    if diff > tdiff:
                        spoint = up_list[l]
                        break
                epoint = len(up_list) - 1
                for l in range(m+1, len(up_list)):
                    diff = htemp - temp_set[up_list[l]]
                    if diff > tdiff:
                        epoint = up_list[l]
                        break

                p1 = time_set[spoint]
                p2 = time_set[epoint]

                peak_list.append([time_set[k], temp_set[k], p1, temp_set[spoint], p2, temp_set[epoint]])
                break

    return peak_list

#-----------------------------------------------------------------------------------------------
#-- clean_up_peak_list: find overlapped data and combine them                                 --
#-----------------------------------------------------------------------------------------------

def clean_up_peak_list(peak_list):
    """
    find overlapped data and combine them
    input:  peak_list   --- a list of lists of:
            [<peak position> <peak temp> <valley position1> <valley temp> <valley position2> <valley temp>]
    output: mpeak_list  --- a cleaned up list of lists
    """
                
    p_len        = len(peak_list)
    if p_len == 0:
        return []
    [m_time, m_temp, s_time, s_temp, e_time, e_temp] = peak_list[0]
    cleaned_list = []
    for i in range(1, p_len):
        [m_time2, m_temp2, s_time2, s_temp2, e_time2, e_temp2] = peak_list[i]

        if s_time > s_time2:
            continue
#
#--- if two set of peaks have the same start and stop valley positions, combine them
#
#        elif s_time == s_time2 and e_time == e_time2:
        elif s_time == s_time2:
            if e_time == e_time2:
                if m_temp < m_temp2:
                    m_time = m_time2
                    m_temp = m_temp2
            else:
                alist = [m_time, m_temp, s_time, s_temp, e_time, e_temp]
                cleaned_list.append(alist)
                m_time = m_time2
                m_temp = m_temp2
                s_time = s_time2
                s_temp = s_temp2
                e_time = e_time2
                e_temp = e_temp2
#
#--- if the start and stop time are in order (in two set), save the data
#
        elif s_time < s_time2:
            if e_time > s_time2:

                if m_temp2 > m_temp:
                    m_time = m_time2
                    m_temp = m_temp2
                    s_time = s_time2
                    s_temp = s_temp2
                    e_time = e_time2
                    e_temp = e_temp2

            alist = [m_time, m_temp, s_time, s_temp, e_time, e_temp]
            cleaned_list.append(alist)
            m_time = m_time2
            m_temp = m_temp2
            s_time = s_time2
            s_temp = s_temp2
            e_time = e_time2
            e_temp = e_temp2

        if i == p_len -1:
            alist = [m_time, m_temp, s_time, s_temp, e_time, e_temp]
            cleaned_list.append(alist)
            break

#
#--- remove duplicate
#
    prev  = cleaned_list[0]
    mpeak_list = [prev]
    for i in range(1, len(cleaned_list)):
        if prev == cleaned_list[i]:
            continue
        else:
            prev = cleaned_list[i]
            mpeak_list.append(prev)
#
#---- remove really close peak
#
    rpeak_list = []
    m_len      = len(mpeak_list)
    for i in range(1, m_len):
        diff1 = mpeak_list[i][2] - mpeak_list[i-1][2]
        diff2 = mpeak_list[i][4] - mpeak_list[i-1][4]
        if diff1 > 14000 and diff2 > 14000:
            rpeak_list.append(mpeak_list[i-1])
        else:
            if mpeak_list[i][1] < mpeak_list[i-1][1]:
                mpeak_list[i] = mpeak_list[i-1]
        if i == m_len-1:
            rpeak_list.append(mpeak_list[i])
            break

    return rpeak_list


#-----------------------------------------------------------------------------------------------
#-- convert_to_readable: convert the data to reable form                                     ---
#-----------------------------------------------------------------------------------------------

def convert_to_readable(peak_list):
    """
    convert the data to reable form
    input:  peak_list   ---  a list of lists of:
           [<peak position> <peak temp> <valley position1> <valley temp> <valley position2> <valley temp>]
    output: lday        --- ydate
            focal       --- focal temperature (in C)
            width       --- width of the peak in day)
    """
    slist = []
    for ent in peak_list:
        lday  = sec1998tofracday(ent[0])
        focal = float(ent[1])
        focal = "%.2f" % round(focal, 2)
        start = ent[2]
        stop  = ent[4]
        width = (stop -start)/86400.
        width = "%.2f" % round(width, 2)

        alist = [lday, focal, width]
        slist.append(alist)

    return slist

#-----------------------------------------------------------------------------------------------
#-- sec1998tofracday: convert time from seconds from 1998.1.1 to fractional yday              --
#-----------------------------------------------------------------------------------------------

def sec1998tofracday(stime):
    """
    convert time from seconds from 1998.1.1 to fractional yday 
    input:  stime   --- time in seconds from 1998.1.1
    output: lday    --- fractional year date. igore year
    """
    ptime = Chandra.Time.DateTime(stime).date
    atemp = re.split(':', ptime)
    day   = float(atemp[1])
    hh    = float(atemp[2])
    mm    = float(atemp[3])
    ss    = float(atemp[4])
    dtime = day + hh/24.0 + mm/ 1440.0 + ss / 86400.0
    lday  = "%.2f" % round(dtime, 2)

    return lday

#-----------------------------------------------------------------------------------------------
#-- read_data_file_col: read ascii data file                                                  --
#-----------------------------------------------------------------------------------------------

def read_data_file_col(ifile, sep='', remove=0, c_len=0):
    """
    read ascii data file
    input:  ifile   --- file name
            sep     --- split indicator: default: '' --- not splitting
            remove  --- indicator whether to remove the file after reading: default: 0 --- no
            c_len   --- numbers of columns to be read. col=0 to col= c_len. default: 0 --- read all
    output: data    --- a list of lines or a list of lists
    """
    with open(ifile) as f:
        data = [line.strip() for line in f.readlines()]
    
    if remove > 0:
        os.remove(ifile)
    
    if len(data) == 0:
        return [0, 0]

    if sep != '':
        atemp = re.split(sep, data[0])
        if c_len == 0:
            c_len = len(atemp)
        save = []
        for k in range(0, c_len):
            save.append([])
    
        for ent in data:
            atemp = re.split(sep, ent)
            for k in range(0, c_len):
                try:
                    save[k].append(float(atemp[k]))
                except:
                    save[k].append(atemp[k])
    
        return save
    
    else:
        return data


#-----------------------------------------------------------------------------------------
#-- TEST TEST TEST TEST TEST TEST TEST TEST TEST TEST TEST TEST TEST TEST TEST TEST    ---
#-----------------------------------------------------------------------------------------

class TestFunctions(unittest.TestCase):
    """
    testing functions
    """

#-----------------------------------------------------------------------------------------

    def test_find_time_span(self):

        [start, stop, dstart, dstop] = find_time_span()

        print('TIME START/STOP: ' + str(start) + '<--->' + str(stop))

#-----------------------------------------------------------------------------------------

    def test_find_focal_temp_list(self):

        comp = [['22.52', -115.19, '0.85'], ['23.77', -114.81, '1.21'], ['25.15', -112.8, '1.19'], \
                ['26.81', -114.69, '1.10'], ['27.82', -111.39, '0.73']]
        year = 2016
        month = 1
        mday  = 29
        [start, stop, dstart, dstop]        = find_time_span(year, month, mday)
        #[time_set, temp_set] = find_focal_temp_list(start, stop)
        [time_set, temp_set] = find_focal_temp_peaks(start, stop)

        peak_list = select_peak(time_set, temp_set)
        peak_list = clean_up_peak_list(peak_list)
        peak_list = convert_to_readable(peak_list)

        self.assertEquals(peak_list, comp)

        for ent in peak_list:
            print(str(ent))

#-----------------------------------------------------------------------------------------------
 
if __name__ == "__main__":
#
#--- if you like to specify the date, give year, month, and date
#
    test  = 0
    tdiff = 0.3
    if len(sys.argv) == 2:
        if sys.argv[1] == 'test':
            test = 1
            del sys.argv[1:]
        else:
            year  = ''
            month = ''
            mday  = ''
            tdiff = float(sys.argv[1])

    elif len(sys.argv) == 4:
        year  = int(float(sys.argv[1]))
        month = int(float(sys.argv[2]))
        mday  = int(float(sys.argv[3]))
    elif len(sys.argv) == 5:
        year  = int(float(sys.argv[1]))
        month = int(float(sys.argv[2]))
        mday  = int(float(sys.argv[3]))
        tdiff = float(sys.argv[4])
    else:
        year  = ''
        month = ''
        mday  = ''

    if test == 0:
        find_focal_temp_peaks(year, month, mday, tdiff)
    else:
        unittest.main()
