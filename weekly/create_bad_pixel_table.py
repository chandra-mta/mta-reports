#!/proj/sot/ska3/flight/bin/python

#############################################################################
#                                                                           #
#   create_bad_pixel_table.py: create bad pixel table for the weekly report #
#                                                                           #
#           author: t. isobe (tisobe@cfa.harvard.edu)                       #
#                                                                           #
#           Last Update: Mar 15, 2021                                       #
#                                                                           #
#############################################################################

import sys
import re
import time
import random
#
#--- Define directory pathing
#
BIN_DIR = "/data/mta/Script/Weekly/Scripts"
BAD_PIX_DIR = "/data/mta/Script/ACIS/Bad_pixels/Data"
sys.path.append(BIN_DIR)

#---------------------------------------------------------------------------------------
#-- create_bad_pixel_table: create bad pixel table for the weekly report              --
#---------------------------------------------------------------------------------------

def create_bad_pixel_table():
    """
    create bad pixel table for the weekly report
    input:  none but read from /data/mta/Script/ACIS/Bad_pixels/Data/*
    output: line    --- a html table to display the current bad pixel list
    """

    line  = '<table border=1 cellpadding=3>'
    line  = line + '<tr align=center><th>&#160</th><th>CCD0</th><th>CCD1</th><th>CCD2</th>'
    line  = line + '<th>CCD3</th><th>CCD4</th><th>CCD5</th><th>CCD6</th><th>CCD7</th>'
    line  = line + '<th>CCD8</th><th>CCD9</th></tr>'
#
#--- previously unknown bad pixels
#
    line  = line + create_table_section('ccd',  'new',   'Previously Unknown Bad Pixels')
#
#--- current warm pixels
#
    line  = line + create_table_section('ccd',  'warm',  'Current Warm  Pixels')
#
#--- flickering warm pixel
#
    line  = line + create_table_section('ccd',  'flick', 'Flickering Warm Pixels')
#
#--- current hot pixel
#
    line  = line + create_table_section('hccd', 'warm',  'Current Hot Pixels')
#
#--- flickering hot pixel
#
    line  = line + create_table_section('hccd', 'flick', 'Flickering Hot Pixels')
#
#--- warm column
#
    line  = line + create_table_section('col',  'warm',  'Warm column candidates')
#
#--- flickering warm column
#
    line  = line + create_table_section('col',  'flick', 'Flickering Warm column candidates')

    return line

#---------------------------------------------------------------------------------------
#-- create_table_section: create bad pixel table entry                               ---
#---------------------------------------------------------------------------------------

def create_table_section(ctype, btype, title):
    """
    create bad pixel table entry 
    input:  ctype   --- ccd, hccd, or col
            btye    --- new, warm of flick
            title   --- the title of the section
    output: line    --- the content of the section
    """
    line = '<tr style="text-align:center"><td>' + title + '</td>\n'
    for ccd in range(0, 10):
        ifile = f"{BAD_PIX_DIR}/{ctype}{ccd}_information"
        with open(ifile) as f:
            data = [line.strip() for line in f.readlines()]
        save  = []
        for out in data:
            mc    = re.search(btype, out)
            if mc is not None:
                atemp = re.split('\s+', out)
                for  ent in atemp[1:]:
                    mc2 = re.search(':', ent)
                    if mc2 is not None:
                        continue
                    save.append(ent)

        line = line + '<!-- ccd' + str(ccd) + ' -->\n'
        line = line + '<td>\n'
        if len(save) > 0:
            for ent in save:
                line = line + ent + '\n'
        else:
            line = line + '&#160;\n'
        line = line + '</td>\n'
    line = line + '</tr>\n\n'

    return line


#---------------------------------------------------------------------------------------

if __name__ == '__main__':

    line = create_bad_pixel_table()

    with open('./bad_pix_list', 'w') as fo:
        fo.write(line)






