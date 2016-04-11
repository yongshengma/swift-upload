#!/usr/bin/env python

# upload objects to swift cluster
# 
import logging
import logging.handlers
import argparse
import os, sys, json, time, calendar
import MySQLdb
from datetime import datetime
from subprocess import Popen, PIPE

dirbase = "/jxq.getanddownload.shell/upload/"
tenants = ['cmbc','cmcc','cebb','shell']

# Deafults
LOG_LEVEL = logging.INFO  # Could be e.g. "DEBUG" or "WARNING"

# Define and parse command line arguments
parser = argparse.ArgumentParser(description="Upload service")
parser.add_argument("ip", help="which camera's data to upload")

args = parser.parse_args()
if not args.ip:
    sys.exit(-99);

# Load camera properties, e.g. log file path and data file path
with open(dirbase + 'camera_props.json', 'r') as f:
    p = json.load(f)

# screen tenant list to find where this camera ip located 
for tenant in tenants:
    if args.ip in p[tenant]['camera'].keys():
        found_tenant = True
        logfile = p[tenant]['camera'][args.ip]['logfile']
        datapath = p[tenant]['camera'][args.ip]['datapath']
        lscmd = r'ls ' + os.path.join(datapath, '*MP4') + ' | head -n 10'
        break

if not found_tenant:
    sys.exit(-97);

# Configure logging to log to a file, making a new file at midnight and keeping the last 3 day's data
# Give the logger a unique name (good practice)
logger = logging.getLogger(__name__)
# Set the log level to LOG_LEVEL
logger.setLevel(LOG_LEVEL)
# Make a handler that writes to a file, making a new file at midnight and keeping 3 backups
handler = logging.handlers.TimedRotatingFileHandler(logfile, when="midnight", backupCount=3)
# Format each log message like this
formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')
# Attach the formatter to the handler
handler.setFormatter(formatter)
# Attach the handler to the logger
logger.addHandler(handler)

# load credentails for accessing keystone, swift proxy and mysql db
with open(dirbase + 'core_creds.json', 'r') as f:
    c = json.load(f)

keystone    = c['keystone']['uri']
key_ver     = c['keystone']['ver']
credentials = json.dumps(c['keystone'][tenant], ensure_ascii=True)

proxy   = c['proxy']['uri']
prx_ver = c['proxy']['ver']
account = c['proxy']['account'][tenant]

mysql_host = c['mysql_2']['host']
mysql_port = c['mysql_2']['port']
mysql_user = c['mysql_2']['user']
mysql_pass = c['mysql_2']['pass']
mysql_db = c['mysql_2']['db']
mysql_table = c['mysql_2']['table'][tenant]

sqlstmt = "insert into " + mysql_table
sqlstmt += "( created_at, "
sqlstmt += "length_tag, "
sqlstmt += "channel, "
sqlstmt += "ymd, "
sqlstmt += "hms, "
sqlstmt += "timestamp, "
sqlstmt += "plan_tag, "
sqlstmt += "device_name, "
sqlstmt += "name, "
sqlstmt += "size_kb, "
sqlstmt += "timestamp_end, "
sqlstmt += "device_ip ) "
sqlstmt += "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

class SubProcError(Exception): pass
class InternalError(Exception): pass
class UnauthError(Exception): pass
class NoneValsWarning(Exception): pass
class NoneFileWarning(Exception): pass
class DBFailError(Exception): pass

# Get a token from keystone server
#
def getToken():
    # Bogged in forever until a valid token acquired
    # otherwise we can't do next thing anyway
    # therefore keep on trying if connection fails or keystone has trouble
    while True:
        logger.info("Fetch a token: ")
        try:
            # credentials is dummied for security when cmd is logged
            lcmd = 'curl -d \'%s\' -H "Content-type:application/json" %s/%s/tokens' \
                     %("<credentials>",keystone,key_ver)
            logger.info(lcmd)

            # real command
            cmd = 'curl -d \'%s\' -H "Content-type:application/json" %s/%s/tokens' \
                    %(credentials,keystone,key_ver)

            p = Popen(cmd , shell=True, stdout=PIPE, stderr=PIPE)
            out, err = p.communicate()

            logger.info("return code: %s" % p.returncode)
            if p.returncode != 0: 
                raise SubProcError()

            tags = json.loads(out)
            break
        except SubProcError:
            logger.error("ERROR: getToken() return code %s" % p.returncode)
            logger.error(err.rstrip())
            time.sleep(5)   # wait and try again

    return tags['access']['token']['id']


# Get a group of files for upcoming process
#
def listMany(lscmd):
    try:
        p = Popen(lscmd, shell=True, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        logger.info("%s; return code %s" % (lscmd, p.returncode))
        if p.returncode != 0: 
            raise SubProcError()
        return out.splitlines()
    except SubProcError:
        logger.error("ERROR: listMany() return code %s" % p.returncode)
        return None

def parseOne(filename):
    meta = os.stat(filename)            # filename is full path
    fname = os.path.split(filename)[1]  # fname is path-stripped

    # an example of file name:
    #   K_CH14_J_20150813_054000_20_195.178.10.126_DS-9116HF-ST16.MP4

    attr = fname.split('_')
    v = {}
    v['created_at']  = convertUnixTS(meta.st_mtime)
    v['length_tag']  = attr[0]
    v['channel']     = attr[1]
    v['ymd']         = attr[3]
    v['hms']         = attr[4]
    v['timestamp']   = convertDataTime(attr[3]+' '+attr[4], "%Y%m%d %H%M%S")
    v['plan_tag']    = attr[2]
    v['device_name'] = attr[7].split('.')[0]
    v['name']        = fname  # file name
    v['size_kb']     = meta.st_size
    v['timestamp_end'] = v['timestamp'] + int(attr[5]) * 60
    v['device_ip']     = attr[6]
    v['container']     = attr[3][:6]
    return v

def putOne(filename, v):  # filename is full pathed
    global token
    processed = False    # flag indicating if this file has been uploaded
    try_times = 0        # restrict try times 
    while True:          # use a loop as we want to try again due to occasional error
        try_times += 1
        if try_times > 3: 
            logger.error("PUT: tried 3 times but failed. Let's give up and move on to next.")
            break
        try:
            container = v['container']
            expiration = v['timestamp'] + (31+30+31)*24*60*60   # expire after 3 months

            # token is dummied for concise as it's too long when cmd is logged 
            lcmd = 'curl -i -X PUT -T %s -H "X-Delete-At:%d" -H "X-Auth-Token:%s" %s/%s/%s/%s/' \
                    %(filename,expiration,"<token>",proxy,prx_ver,account,container)
            logger.info(lcmd)

            # real command
            cmd = 'curl -i -X PUT -T %s -H "X-Delete-At:%d" -H "X-Auth-Token:%s" %s/%s/%s/%s/' \
                    %(filename,expiration,token,proxy,prx_ver,account,container)

            p = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
            out, err = p.communicate()
            logger.info(out.splitlines())
            logger.info("return code: %s" % p.returncode)

            if p.returncode != 0: 
                raise SubProcError()
            if "401 Unauthorized" in out:
                raise UnauthError()
            if "500 Internal Error" in out:
                raise InternalError()
            if "503 Internal Server Error" in out:
                raise InternalError()

            if "201 Created" in out:
                processed = True
                break

        except SubProcError:
            logger.error("curl return code %s" % p.returncode)
        except InternalError:
            logger.error("500 Internal Error")
        except UnauthError:
            logger.error("401 Unauthorized. Maybe invalid token.")
            logger.info("Try to get a new token:")
            token = getToken()
            logger.info("Got a new token.")

    # END of WHILE loop
    return processed


def putMany(filegroup):
    vals = []
    fgroup = filegroup[:]
    for filename in fgroup:   # filename is full pathed
        v = parseOne(filename)
        s = putOne(filename, v)
        if s:
            # a row that will be inserted into db
            vals.append(( v['created_at'], \
                          v['length_tag'], \
                          v['channel'], \
                          v['ymd'], \
                          v['hms'], \
                          v['timestamp'], \
                          v['plan_tag'], \
                          v['device_name'], \
                          v['name'], \
                          v['size_kb'], \
                          v['timestamp_end'], \
                          v['device_ip'] ))
        else: 
            filegroup.remove(filename) 
            # modified filegroup will be used in later removing files which have been uploaded

    return vals


def insertMany(vals):
    processed = False
    # exit only when insert is done
    # keep on looping if database connection fails
    while True:
        try:
            conn = MySQLdb.connect( host=mysql_host, \
                                    port=mysql_port, \
                                    user=mysql_user, \
                                    passwd=mysql_pass, \
                                    db=mysql_db ) 
            cur = conn.cursor()
            cur.executemany(sqlstmt, vals)
            logger.info("insert into database %s at %s ; done." % (mysql_db, mysql_host))
        except MySQLdb.Error, msg:
            logger.error("MySQL Error %d: %s" %(msg.args[0], msg.args[1]))
            conn.rollback()
            conn.close()
            time.sleep(5)   # wait and try again
            #raise
        else:
            cur.close()
            conn.commit()
            conn.close()
            processed = True
            break

    return processed

# Put objects (data files) into swift storage url
# 
def upload():
    try:
        filegroup = listMany(lscmd)
        if not filegroup:
            raise NoneFileWarning()

        vals = putMany(filegroup)   # filegroup could change thereafter
        if not vals:
            raise NoneValsWarning()

        insrt = insertMany(vals)
        if not insrt:
            raise DBFailError()
        else:
            # filegroup might have shrinked due to failed curl PUT.
            # Only uploaded files are removed.
            for f in filegroup: 
                os.remove(f)

    except NoneFileWarning:
        logger.warning("No file for uploading, so sleep 5 minute ...")
        time.sleep(300) # wait and try again
    except NoneValsWarning:
        logger.warning("No value for database insert: values required.")
    except DBFailError:
        logger.error("Database operations failed. ")


# Convert Unix Epoch timestamp to human readable format
# 
def convertUnixTS(ts):
    return datetime.fromtimestamp(
            ts).strftime('%Y-%m-%d %H:%M:%S')

# Convert date string to Unix Epoch timestamp
# 
def convertDataTime(ds, re):   # (date string, its format)
    dt = datetime.strptime(ds, re)
    return calendar.timegm(dt.utctimetuple())


if __name__ == '__main__':

    token = getToken()
    if not token:
        sys.exit(-98)

    while True:
        upload()
