MTA Reports
===========

The **mta-reports** repository contains python script sets for for generating MTA reports.

**interruption** contains the **run_interruption.py** script and supporting submodule for generating
reports which cover radiation information in the event of a science run shutdown due to radiation safing measures.

Interruption
============

In the event of an scs107 run / shutdown due to radiation safing procedures, run the **run_interruption.py** script with the following steps.
This should be done around two days after the Chandra science run is confirmed to be back online.

1. SSH -XY as the mta user into one of the mta machines which can view the /data/mta_www directory. The typical choice is boba-v
    a: ssh -XY mta@boba-v
2. Go to /data/mta/Script/Interrupt/Scripts.
3. Using email alerts and scheduling information, identify…
* The starting time of the shutdown (typically when scs107 is confirmed active)
* Whether the shutdown was performed automatically by OBC safing methods or manually from a ground command.
* The stopping time of the shutdown, which is when the first science observation occurs in the return-to-science loads outlined in the MP schedule.
- https://cxc.cfa.harvard.edu/target_lists/
- https://cxc.cfa.harvard.edu/target_lists/stscheds/index.html
- https://icxc.harvard.edu/mp/html/schedules.html
4. Run the run_interruption.py python script with the following command line arguments. This automatically names the event as the <yyyy><mm><dd>
date of the starting time of the shutdown.
* -m: flight or test mode. For changing the live html files, run in flight mode.
* --start: start time of the radiation shutdown in "%Y:%j:%H:%M:%S", or "%Y:%m:%d:%H:%M:%S" string format.
* --stop: stop time of the radiation shutdown in "%Y:%j:%H:%M:%S", or "%Y:%m:%d:%H:%M:%S" string format.
* -r: SCS-107 run version, either automatic or manual. Consult email alerts.
5. This generates the report in the original mta_days web directory. Navigate to the /data/mta_www/mta_interrupt/Note_dir directory and
create a <event_name>.txt file to list the email alerts, SOT shift reports, and radiation discussions that pertain to the shutdown.
6. We also store a copy of the radiation interruptions in the RADIATION_new web directory.
* Navigate to the /data/mta4/www/RADIATION_new directory.
* move mta_interrupt mta_interrupt~
* cp -r /data/mta/www/mta_interrupt .
* cd to mta_interrupt
* sed -i "s/mta_days/mta\/RADIATION_new/g"  *.html
