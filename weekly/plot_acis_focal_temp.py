#!/proj/sot/ska3/flight/bin/python

#############################################################################################
#                                                                                           #
#           plot_acis_focal_temp.py: plot acis focal temperature trend                      #
#                                                                                           #
#               author: t. isobe (tisobe@cfa.harvard.edu)                                   #
#                                                                                           #
#               last update: Mar 15, 2021                                                   #
#                                                                                           #
#############################################################################################

import os
import sys
import re
import time
import numpy
import astropy.io.fits  as pyfits
import Chandra.Time
import unittest
from calendar import isleap
#
#--- from ska
#
from Ska.Shell import getenv, bash
ascdsenv = getenv('source /home/ascds/.ascrc -r release; punlearn dataseeker', shell='tcsh')
#
#--- plotting routine
#
import matplotlib as mpl

if __name__ == '__main__':

    mpl.use('Agg')

from pylab import *
import matplotlib.pyplot as plt
import matplotlib.font_manager as font_manager
import matplotlib.lines as lines


#
#--- Define directory pathing
#
BIN_DIR = "/data/mta/Script/Weekly/Scripts"
DATA_DIR = "/data/mta/Script/Weekly/Data"
FOCAL_DIR = "/data/mta/Script/ACIS/Focal/Data"

sys.path.append(BIN_DIR)
#
#--- set column names and header
#
orb_col_list  = ['time', 'x', 'y', 'z']
ang_col_list  = ['time','point_suncentang']
lfile = f"{BIN_DIR}/house_keeping/loginfile"

#-----------------------------------------------------------------------------------------------
#-- plot_acis_focal_temp: plot acis focal temperature                                        ---
#-----------------------------------------------------------------------------------------------

def plot_acis_focal_temp(tyear='', yday=''):
    """
    plot acis focal temperature; the plotting range is the last 7 days
    input:  none, but read from several database
    output: ./acis_focal_temp.png
    """
    if tyear == '':
        tyear  = int(float(time.strftime('%Y', time.gmtime())))
        yday   = int(float(time.strftime('%j', time.gmtime())))
        today  = time.strftime('%Y:%j:00:00:00', time.gmtime())
    else:
        today = f"{tyear}:{yday:03}:00:00:00"

    cdate  = Chandra.Time.DateTime(today).secs
    cstart = cdate - 86400.0 * 7.0
#
#--- extract focal temp data
#
    [ftime, focal]     = read_focal_temp(tyear, yday, cstart, cdate)
#
#--- convert time format to yday
#
    [ftime, byear]     = convert_time_format(ftime)
#
#--- extract altitude data and sun angle data
#
    [atime, alt, sang] = read_orbit_data(cstart, cdate)
    [atime, byear]     = convert_time_format(atime)
#
#--- convert alttude to normalized to sun angle (range between 0 and 180)
#
    alt                = compute_norm_alt(alt)
#
#--- plot data
#
    xlabel             = 'Day of Year (' + str(byear) + ')'
    [ltime, byear]     = convert_time_format([cstart, cdate])

    plot_data(ftime, focal, atime, alt, sang, ltime[0], ltime[1], xlabel)

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
#--- if y daay is less than 8, read the data from the last year
#
    if yday < 8:
        ifile = f"{FOCAL_DIR}/focal_plane_data_5min_avg_{tyear-1}"
        data   = read_data_file(ifile, sep='\s+', c_len=2)
        ftime  = data[0]
        focal  = data[1]
    else:
        ftime  = []
        focal  = []
#
#--- otherwise, just read this year
#
    ifile = f"{FOCAL_DIR}/focal_plane_data_5min_avg_{tyear}"
    data   = read_data_file(ifile, sep='\s+', c_len=2)
    ftime  = ftime + data[0]
    focal  = focal + data[1]
#
#--- select out the data for the last 7 days
#
    [ftime, focal] = select_data_by_date(ftime, focal, tstart, tstop)

    return [ftime, focal]

#-----------------------------------------------------------------------------------------------
#-- read_orbit_data: read altitude and sun angle data                                        ---
#-----------------------------------------------------------------------------------------------

def read_orbit_data(tstart, tstop):
    """
    read altitude and sun angle data
    input:  tstart  --- starting time in seconds from 1998.1.1
            tstop   --- stopping time in seconds from 1998.1.1
    output: data    --- a list of lists of [time alt, sun_angle]
    """
#
#--- set up the input for dataseeker and extract the data
#
    fits = 'dataseek_avg.fits'
    cmd  = 'touch infile'
    os.system(cmd)

    cmd1 = '/usr/bin/env PERL5LIB=  '
    cmd2 = " dataseeker.pl infile=infile outfile=" + fits + " "
    cmd2 = cmd2 + "search_crit='columns=pt_suncent_ang,sc_altitude timestart=" + str(tstart)
    cmd2 = cmd2 + " timestop=" + str(tstop) + "' loginFile=" + lfile

    cmd  = cmd1 + cmd2
    bash(cmd, env=ascdsenv)
#
#--- read fits file and extract the data
#
    cols = ['time', 'sc_altitude', 'pt_suncent_ang']
    data = read_fits_data(fits, cols)
#
#--- clean up
#
    os.remove(fits)
    os.remove('infile')

    return data

#-----------------------------------------------------------------------------------------------
#-- select_data_by_date: selet out the potion of the data by time                             --
#-----------------------------------------------------------------------------------------------

def select_data_by_date(x, y, tstart, tstop):
    """
    select out the potion of the data by time
    input:  x       --- a list of time data
            y       --- a list of data
            tstart  --- a starting time in seconds from 1998.1.1
            tstop   --- a stopping time in seconds from 1998.1.1
    output: x       --- a list of time data selected
            y       --- a list of data selected
    """
    x   = numpy.array(x)
    y   = numpy.array(y)
    ind = (x > tstart) & (x < tstop)
    x   = list(x[ind])
    y   = list(y[ind])

    return [x, y]

#-----------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------

def compute_norm_alt(v, nval=180.):
    """
    normalize the data to a given max size
    input:  v       --- a list of the data
            nval    --- the max value; default = 180
    output: v       --- a list of the data normlaized
    """
    vmin = min(v)
    vmax = max(v)
    v    = v - vmin
    v    = v / (vmax - vmin)
    v    = v * nval

    return list(v)

#-----------------------------------------------------------------------------------------------
#-- convert_time_format: convert a list of the time data into ydate                           --
#-----------------------------------------------------------------------------------------------

def convert_time_format(otime):
    """
    convert a list of the time data into ydate
    input:  otime   --- a list of time in seconds from 1998.1.1
    output: save    --- a list of time in y date
            prev    --- the year of the data
    """

    save = []
    prev = 0
    for ent in otime:
        out = Chandra.Time.DateTime(ent).date
        atemp = re.split(':', out)

        year  = int(atemp[0])
        yday  = float(atemp[1])
        hh    = float(atemp[2])
        mm    = float(atemp[3])
        ss    = float(atemp[4])

        yday +=  hh /24.0 + mm / 1440.0 + ss / 86400.0

        if prev == 0:
            prev = year
            save.append(yday)
            base = 365 + isleap(year)
        else:
            if year != prev:
                save.append(yday + base)
            else:
                save.append(yday)

    return [save, prev]

#-----------------------------------------------------------------------------------------------
#-- read_data_file: read ascii data file                                                      --
#-----------------------------------------------------------------------------------------------

def read_data_file(ifile, sep='', remove=0, c_len=0):
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

#-----------------------------------------------------------------------------------------------
#-- plot_data: plot data                                                                      --
#-----------------------------------------------------------------------------------------------

def plot_data(ftime, ftemp, stime, alt, sang, xmin, xmax, xlabel):
    """
    plot data
    input:  ftime   --- a list of time for focal temp 
            ftemp   --- a list of focal temp data
            stime   --- a list of time for altitude and sun angle
            alt     --- a list of altitude data
            sang    --- a list of sun agnle
            xmin    --- min of x plotting range
            xmax    --- max of x plotting range
            xlabel  --- the label for x axis
    output: acis_focal_temp.png
    """
#    
#--- set sizes
#
    fsize  = 16
    color  = 'blue'
    color2 = 'red'
    color3 = 'green'
    marker = '.'
    psize  = 8
    lw     = 3
    alpha  = 0.3
    width  = 10.0
    height = 5.0
    resolution = 200
#
#-- close everything opened before
#
    plt.close('all')
#
#--- set font size
#
    mpl.rcParams['font.size'] = fsize
    props = font_manager.FontProperties(size=fsize)
    plt.subplots_adjust(hspace=0.08)
#
#--- set plotting range  focal temp
#
    [ymin, ymax] = set_focal_temp_range(ftemp)

    fig, ax1 = plt.subplots()

    ax1.set_autoscale_on(False)
    ax1.set_xbound(xmin,xmax)
    ax1.set_xlim(xmin=xmin, xmax=xmax, auto=False)
    ax1.set_ylim(ymin=ymin, ymax=ymax, auto=False)

    temp, = ax1.plot(ftime, ftemp, color=color, label="Focal Temp", lw=lw)

    ax1.set_xlabel(xlabel)
    ax1.set_ylabel('Focal Plane Temp (degC)')
    ax1.tick_params(axis='y', labelcolor=color)
#
#--- set plotting range sun angle
#
    ax2 = ax1.twinx()                   #--- setting the second axis 

    ax2.set_autoscale_on(False)
    ax2.set_xbound(xmin,xmax)
    ax2.set_xlim(xmin=xmin,  xmax=xmax, auto=False)
    ax2.set_ylim(ymin=0, ymax=180, auto=False)

    sun, = ax2.plot(stime, sang, color=color2, label="Sun Angle", alpha=0.8)

    ax2.set_ylabel('Sun Angle (degree)')
    ax2.tick_params(axis='y', labelcolor=color2)
#
#--- plot altitude
#
    alt, = ax2.plot(stime, alt, color=color3, label="Altitude", alpha=0.8)
#
#--- adding legend
#
    fontP = font_manager.FontProperties()
    fontP.set_size(8)
    plt.legend(loc='upper right', bbox_to_anchor=(1.0, -0.06), handles=[temp, sun, alt],\
               fancybox=False, ncol=1, prop=fontP)
#
#--- save the plot
#
    outfile = f"{DATA_DIR}/Focal/acis_focal_temp.png"
    fig     = plt.gcf()
    fig.set_size_inches(width, height)
    plt.tight_layout()
    plt.savefig(outfile, format='png', dpi=resolution)

    plt.close('all')

#-----------------------------------------------------------------------------------------------
#-- set_focal_temp_range: setting the focal temp plotting range                               --
#-----------------------------------------------------------------------------------------------

def set_focal_temp_range(v):
    """
    setting the focal temp plotting range
    input:  v       --- focal temp
    output: vmin    --- min of the plotting range
            vmax    --- max of the plotting range
    """

    vmin = min(v)
    vmax = max(v)
    diff = vmax - vmin

    if vmin  > 122:
        vmin = 122
    else:
        vmin = int(vmin) -1
    
    vmax = int(vmax + 0.02 * diff)

    return [vmin, vmax]


#-------------------------------------------------------------------------------------------------
#-- read_fits_data: read fits data                                                              --
#-------------------------------------------------------------------------------------------------

def read_fits_data(fits, cols):
    """
    read fits data
    input:  fits    --- fits file name
            cols    --- a list of col names to be extracted
    output: save    --- a list of lists of data extracted
    """

    hout = pyfits.open(fits)
    data = hout[1].data
    hout.close()

    save = []
    for col in cols:
        out = data[col]
        save.append(out)

    return save

    

#-----------------------------------------------------------------------------------------
#-- TEST TEST TEST TEST TEST TEST TEST TEST TEST TEST TEST TEST TEST TEST TEST TEST    ---
#-----------------------------------------------------------------------------------------

class TestFunctions(unittest.TestCase):
    """
    testing functions
    """
#------------------------------------------------------------

    def test_read_focal_temp(self):

        year   = 2018
        yday   = 5
        cdate  = Chandra.Time.DateTime('2018:005:00:00:00').secs
        cstart = cdate - 86400.0 * 7.0

        [x, y] = read_focal_temp(year, yday, cstart, cdate)

        print('Focal: ' + str(len(x)) + '<-->' + str(x[:10]) + '<-->' +  str(y[:10]))


#------------------------------------------------------------

    def test_read_orbit_data(self):

        year   = 2018
        yday   = 5
        cdate  = Chandra.Time.DateTime('2018:005:00:00:00').secs
        cstart = cdate - 86400.0 * 7.0

        [x, y, y2] = read_orbit_data(cstart, cdate)

        print('Alt: ' + str(len(x)) + '<-->' + str(x[:10]) + '<-->' +  str(y[:10]))


#------------------------------------------------------------
#
#    def test_read_sunangle(self):
#
#        year   = 2018
#        yday   = 5
#        cdate  = Chandra.Time.DateTime('2018:005:00:00:00').secs
#        cstart = cdate - 86400.0 * 7.0
#
#        [x, y] = read_sunangle(cstart, cdate)
#
#        print('Sun Angle: ' + str(len(x)) + '<-->' + str(x[:10]) + '<-->' +  str(y[:10]))





#-----------------------------------------------------------------------------------------------

if __name__ == '__main__':

    #unittest.main()
    #exit(1)
    if len(sys.argv) == 3:
        year = int(float(sys.argv[1]))
        yday = int(float(sys.argv[2]))
    else:
        year = ''
        yday = ''

    plot_acis_focal_temp(year, yday)








