#!/usr/bin/env python3
##!/opt/anaconda/miniconda3/bin/python
##!/usr/bin/env python

import os
import sys

with open( 'runLocust.log', 'w') as outFile:
    print( 'running locust', file=outFile )

sys.path.append( os.path.expanduser('~/locust'))

from locust.main import main
import locust.events
import locust.runners
import locust.stats

g_instanceId = None
g_ipAddr = None

def readInstanceIdFile( inFilePath ):
    global g_instanceId
    try:
        with open( inFilePath, "r" ) as inFile:
            line = inFile.readline().strip()
            g_instanceId = line
    except Exception:
        print( 'could not read from %s' % (inFilePath), file=sys.stderr )

def readIpAddrFile( inFilePath ):
    global g_ipAddr
    try:
        with open( inFilePath, "r" ) as inFile:
            line = inFile.readline().strip()
            g_ipAddr = line
    except Exception:
        print( 'could not read from %s' % (inFilePath), file=sys.stderr )

def onReportToMaster(client_id, data):
    if g_instanceId:
        data[ 'instanceId' ] = g_instanceId
    if g_ipAddr:
        data[ 'ipAddr' ] = g_ipAddr
    sys.stderr.flush()
    sys.stdout.flush()

#locust.runners.SLAVE_REPORT_INTERVAL = 10.0
#print( 'SLAVE_REPORT_INTERVAL is', locust.runners.SLAVE_REPORT_INTERVAL )

if False:  # '--master' in sys.argv:
    print( 'opening statsOutFile' )
    dataDirPath = 'data'
    os.makedirs( dataDirPath, exist_ok=True )
    dataFilePath = dataDirPath+'/locustStats.csv'
    if os.path.exists( dataFilePath ):
        os.remove( dataFilePath )
    #locust.stats.openStatsOutFile( dataFilePath )

if True:
    readInstanceIdFile( os.path.expanduser( '~/instanceId.txt' ) )
    #readIpAddrFile( os.path.expanduser( '~/ipAddr.txt' ) )
    locust.events.report_to_master += onReportToMaster

retCode =  main()
print( 'runLocust exiting' )
sys.stderr.flush()
sys.stdout.flush()
sys.exit( retCode )
