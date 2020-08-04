"""VMAX Host Logical Unit to LUN mapping script.

This script is used to generate the mapping of LUN using WWN
"""
# To create directory structure
import os
# Date and time from the system
from datetime import datetime
# To get hostname
import socket
# Python builtin logging function
import logging
# Import the duallog package to set up logging both to screen and console.
# https://github.com/acschaefer/duallog
import duallog
# pass multiple arguments with help function
import argparse
# Customized subprocess module
from sub import sub
# iterate with multiple arrays
import itertools
# pretty print
# from pprint import pprint
# Pandas
import pandas as pd
# Paramiko to send the file to master node
import paramiko
# SSH Key for authentication
mykey = '/home/rda/.ssh/id_rsa.pub'
# create a dataframe which is used to export output to csv
df = pd.DataFrame()
# Set date
date_ = datetime.now().strftime('%d_%m_%Y__%H:%M')
# archive filepath
arcdir = "/rundeck/storage/vmax_luns_data/archives/"
if not os.path.exists(arcdir):
    os.makedirs(arcdir)

# Git filepath
gitpath = "/git/cps_storage_scripts/"

# report filepath
reppath = "/rundeck/storage/vmax_hlu_luns_data/"
if not os.path.exists(reppath):
    os.makedirs(reppath)

# Set up dual logging and tell duallog where to store the logfiles.
duallog.setup('{}vmax_hlu2lunmap_{}'.format(reppath, date_))

# Get DC value from the Rundeck option parameter


def check_arg(args=None):
    """Argparse to take DC as parameter."""
    parser = argparse.ArgumentParser(
        description='Script to generate and send VMAX Capacity Report ')

    parser.add_argument('-dc', '--datacenter',
                        help='Datacenter parameter required',
                        required='True')
    results = parser.parse_args(args)

    return (results.datacenter)


dc = check_arg()

# Get hostname
host = socket.gethostname()
# print(host)
# Get IP address
checkip = os.popen('/usr/bin/sudo ifconfig eth0 | /bin/grep "inet\
 addr" | /bin/cut -d: -f2 | /bin/cut -d" " -f1')
ip = checkip.read()
ip = ip.rstrip("\r\n")
# print(ip)
logging.info("VMAX HLU2LUNMAP script started on -{}-{}".format(host, ip))

# Open VMAX arrays info @ GitHub
try:
    vmax_file = open(gitpath + 'vec_vmax_arrays.csv', 'r')
except Exception as file_error:
    logging.error("Unable to open VMAX arrays info file\
 -{}. Error - {}".format(vmax_file, file_error))

# initialize list variables
arrays = []
sids = []

# List of commands used to the HLU information

# Get local vmax arrays
lst_ary_cmd = "/usr/bin/sudo /opt/emc/SYMCLI/bin/symcfg list |/bin/grep Local"
lst_ary = sub.run_cmd(lst_ary_cmd)
if lst_ary[-1] == 0:
    # split by new line and push the lines as list elements
    arrays = [s.strip() for s in lst_ary[0].split('\n') if s]
    logging.info(arrays)
else:
    logging.error("Unable to get local VMAX arrays info\
 {}. \nError - {}".format(lst_ary_cmd, lst_ary[1]))

"""Using string split and append method to extract Serial Number of the
VMAX array and append it to 'sids' list"""
for line in arrays:
    temp = line.split()
    sids.append(temp[0])
    logging.debug(
        'For Loop to get locally attached arrays serial \
number{} {}'.format(line, temp[0]))
# Command to list the SRP (storage resource pools) information of VMAX array/s
for sid in sids:
    sid.strip()
    logging.info(sid)
    # print(sid)
    # Get array name
    for sid, line in itertools.product(sids, vmax_file):
        # define the lists to hold the  temporary value
        # print(sid, line)
        devs = []
        devtype = []
        devname = []
        devwwn = []
        wwn = []
        devcap = []
        totcap = []
        aloper = []
        freecap = []
        useper = []
        cmpcap = []
        cmpper = []
        alocap = []
        mview = []
        mviews = []
        mv_dev = []
        mv_devs = []

        check = line.split(',')
        check[3].rstrip()

        if sid == check[3] and ip == check[5]:
            check[2] = check[2].rstrip("\r\n")  # remove newlines
            ary_name = check[2]
            logging.debug('Array name - {}'.format(ary_name))
            # print(ary_name)
            # Command to get Device Names
            dev_name_cmd = "/usr/bin/sudo /opt/emc/SYMCLI/bin/symdev\
 -sid {} list -identifier device_name -all | /bin/grep TDEV".format(sid)
            dev_name = sub.run_cmd(dev_name_cmd)
            if dev_name[-1] == 0:
                dev_names = [s.strip() for s in dev_name[0].split('\n') if s]
                logging.info("Device Names\n{}".format(dev_names))
            else:
                logging.error("Unable to run device name command\
. {} \nError - {}".format(dev_name_cmd, dev_name[1]))

            # Get the first and last device to use it for next command

            for line in dev_names:
                words = line.split()
                devs.append(words[0])
                devtype.append(words[1])
                devname.append(words[2])
            first = devs[0]
            last = devs[-1]
            # pprint(first)
            # pprint(last)
            # command to get the WWN of TDEV devices or LUN IDs
            wwn_cmd = "/usr/bin/sudo /opt/emc/SYMCLI/bin/symdev \
-sid {} list -range {}:{} -wwn |/bin/grep TDEV".format(sid, first, last)
            wwn_out = sub.run_cmd(wwn_cmd)
            if wwn_out[-1] == 0:
                wwns = [s.strip() for s in wwn_out[0].split('\n') if s]
                logging.info("Device Names\n{}".format(wwns))
            else:
                logging.error("Unable to run command to get WWNs \
 - {}.\nError - {}".format(wwn_cmd, wwn_out[1]))

            for line in wwns:
                words = line.split()

                for dev in devs:
                    if words[0] == dev:
                        # print(wwns.index(line), words)
                        devwwn.append(words[0])
                        wwn.append(words[-1])
                        # print(devwwn, wwn, '\n')
                        # logging.error('{}, {}'.format(devwwn, wwn))

            """Get the capacity information of the
            devices {in the range first to last }"""
            cap_cmd = "/usr/bin/sudo /opt/emc/SYMCLI/bin/symcfg\
 -sid {} list -range {}:{} -tdev -gb| /bin/grep .0".format(sid, first, last)
            cap = sub.run_cmd(cap_cmd)
            if cap[-1] == 0:
                caps = [s.strip() for s in cap[0].split('\n') if s]
                logging.info("Devices Capacity\n{}".format(caps))
            else:
                logging.error("Unable to run devices capacity command\
, {}.\nError - {}".format(cap_cmd, cap[1]))

            for line in caps:
                words = line.split()
                if words[0] in devs:
                    devcap.append(words[0])
                    totcap.append(words[3])
                    alocap.append(words[4])
                    freecap.append(float(words[3]) - float(words[4]))
                    aloper.append(words[5])
                    cmpper.append(words[6])

            # Get masking views of the array
            mv_cmd = "/usr/bin/sudo /opt/emc/SYMCLI/bin/symaccess\
 -sid {} list view | egrep MV".format(sid)
            mview = sub.run_cmd(mv_cmd)
            mviews = [s.strip() for s in mview[0].split('\n') if s]
            # pprint(mviews)

            # Get LUNs of each masking view's stroage group
            for view in mviews:
                # print(view)
                viewlines = view.split('\n')
                for viewline in viewlines:
                    viewname = viewline.split()
                    mv_dev_cmd = "/usr/bin/sudo /opt/emc/SYMCLI/bin/symaccess\
 -sid {} list view -name {} -detail | /bin/grep -v '^[[:space:]]' |\
 /bin/grep -i 'not visible' | sort -u \
".format(sid, viewname[0])
                    mv_dev = sub.run_cmd(mv_dev_cmd)
                    if mv_dev[-1] == 0:
                        mv_devs = [s.strip() for s in
                                   mv_dev[0].split('\n') if s]
                        logging.info("Masking View \n{}".format(mv_devs[0]))
                    else:
                        logging.error("Unable to run masking view command\
            , {}.\nError - {}".format(mv_dev[1]))
                    # pprint(mv_devs)
                    for mv_dev in mv_devs:
                        m_d = mv_dev.split()
                        j = mv_devs.index(mv_dev)
                        for dev in devs:
                            # print(dev)
                            i = devs.index(dev)
                            if m_d[0] == dev:
                                """print(f"{ary_name},{devs[i]},{devname[i]},\
    {devtype[i]},{wwn[i]},{viewname[0]},{viewname[1]},{viewname[2]},\
    {viewname[3]},{devcap[i]},{totcap[i]},{alocap[i]},{aloper[i]},\
    {cmpper[i]}")"""
                                # Create a list to append it to dataframe
                                list1 = [[ary_name, sid, devs[i], devname[i],
                                         devtype[i], m_d[4], wwn[i],
                                         viewname[0], viewname[1], viewname[2],
                                         viewname[3], totcap[i], alocap[i],
                                         freecap[i], aloper[i], cmpper[i]]]
                                logging.info(list1)
                                df = df.append(list1, ignore_index=True)
logging.info(df.info())

# Export to CSV
try:
    df.to_csv(reppath+host+'.csv', encoding='utf-8', index=False, header=False)
except Exception as dfError:
    logging.error("Unable to export to CSV.\nError - {}".format(dfError))

# Send the CSV file to Rundeck Master Node
# Create a SSH Client
ssh_client = paramiko.SSHClient()
# Load system host keys
ssh_client.load_system_host_keys(filename='/home/rda/.ssh/id_rsa.pub')
# Trust all host signatures or keys
ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh_client.connect(hostname='192.168.61.115', username='rda',
                   key_filename=mykey)
sftp = ssh_client.open_sftp()
file_name = host+'.csv'
remote_path = reppath
try:
    sftp.chdir(remote_path)  # Test if remote_path exists
except IOError:
    sftp.mkdir(remote_path)  # Create remote_path
    sftp.chdir(remote_path)
    # At this point, you are in remote_path in either case
# Put CSV file to rundeck master node
try:
    sftp.put(reppath+file_name, './'+file_name)
except Exception as sftpError:
    logging.error("Unable to transfer csv file. Error - {}".format(sftpError))
sftp.close()
