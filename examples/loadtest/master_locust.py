import datetime
import json
import os
import six
import sys


import locust
from locust import events
from locust import HttpLocust, TaskSet
from locust.log import console_logger
from locust import web

def indexxx(l):
    l.client.get("/")

class MinimalBehavior(TaskSet):
    tasks = {indexxx: 1}

class MinimalLocust(HttpLocust):
    task_set = MinimalBehavior

import analyzeLtStats

@web.app.route("/ncstats")
def genStatsPage():

    html = analyzeLtStats.compileStats( 'data' )

    return html

g_statsOutFile = None
g_jlogFile = None
g_stopping = False

def openJLogFile( dataFilePath ):
    global g_jlogFile
    g_jlogFile = open( dataFilePath, 'w' )
    g_jlogFile.flush()

def openStatsOutFile( dataFilePath ):
    global g_statsOutFile
    g_statsOutFile = open( dataFilePath, 'w' )
    g_statsOutFile.write( 'dateTime,endDateTimeStr,worker,nr,rps,rpsMax,rpsMin,mspr,msprMax,msprMed,msprMin,nFails,nUsers\n' )
    g_statsOutFile.flush()

def median_from_dict(total, count):
    """
    total is the number of requests made
    count is a dict {response_time: count}
    """
    pos = (total - 1) / 2
    for k in sorted(six.iterkeys(count)):
        if pos < count[k]:
            return k
        pos -= count[k]

reportedZombies = set()

def on_slave_report(client_id, data):
    if 'instanceId' in data:
        instanceId = data[ 'instanceId' ].strip()
        #console_logger.info( 'instanceId %s' % (instanceId) )
        if client_id not in locust.runners.locust_runner.clients:
            if client_id not in reportedZombies:
                reportedZombies.add( client_id )
                console_logger.info( 'zombie instance %s' % (instanceId) )
    else:
        instanceId = None

    ipAddr = None
    if 'ipAddr' in data:
        ipAddr = data[ 'ipAddr' ].strip()

    if client_id not in locust.runners.locust_runner.clients:
        return  # NO FURTHER PROCESSING of reports from zombies

    if g_stopping:
        if len( data['stats']):
            console_logger.info( 'on_slave_report DROPPING because g_stopping len(stats): %d', len( data['stats']) )
        return
    if len( data['stats']):
        #if len(data['stats']) != 1:
        #    console_logger.info( '%d stats objects' % len(data['stats']) )
        nUsers = data['user_count']
        stats = data['stats_total']
        rpss = data['stats_total']['num_reqs_per_sec']
        #rpss = data['stats'][0]['num_reqs_per_sec']
        if len( rpss ) > 0:
            rps = sorted(rpss.values())[ int(len(rpss)/2) ]  # approximate median value
            maxRps = max( rpss.values() )
            minRps = min( rpss.values() )
        else:
            rps = maxRps = minRps = 0
        numReqs = data['stats_total']['num_requests']
        rtimes = data['stats_total']['response_times']  # these are quantized when large
        if numReqs:
            meanRTime = data['stats_total']['total_response_time'] / numReqs
        else:
            meanRTime = 0
        #meanRTime = sum(rtimes.keys()) / len(rtimes.keys())  # old way, imprecise
        maxRTime = data['stats_total']['max_response_time']  # was max( rtimes.keys() )
        minRTime = data['stats_total']['min_response_time']
        if minRTime == None:
            minRTime = 0
        #minRTime = min( rtimes.keys() )  # old way, imprecise

        medRTime = median_from_dict( numReqs, rtimes )
        if medRTime == None:
            medRTime = 0

        # get begin and end time stamps (they are request times, not response-received times)
        startTimeStamp = data['stats_total']['start_time']
        # endTimeStamp = data['stats_total']['last_request_timestamp']  # not useful, it is truncated
        startDateTime = datetime.datetime.fromtimestamp( startTimeStamp )  # could be bad if closks out of sync (or wrong timezone)
        masterDateTime = datetime.datetime.now()
        timeDiscrep = (masterDateTime-startDateTime).total_seconds() - 3
        if abs( timeDiscrep ) > 6.0:
            console_logger.info( 'timeDiscrep: %.3f for instance %s', timeDiscrep, instanceId )
            if abs( timeDiscrep ) > 15:
                console_logger.info( 'DROPPING due to time discrepancy %s', instanceId )
                return  # DROPPING data


        endDateTime = datetime.datetime.now()

        if instanceId and ipAddr:
            workerName = '%s_%s' % (ipAddr, instanceId)
        elif instanceId: 
            workerName = '_%s' % (instanceId)
        else:
            workerName = client_id.rsplit('_',1)[0]
            if workerName == 'localhost':
                if instanceId:
                    workerName = instanceId
                else:
                    workerName = client_id

        if g_jlogFile:
            json.dump( [workerName, data], g_jlogFile )
            print( "", file=g_jlogFile )
            g_jlogFile.flush()

        numFails = stats['num_failures']

        #console_logger.info( '%s worker: %s; reqs: %d RPS: %.1f (%.1f-%.1f); response time: %.1f (%.1f-%.1f)' % \
        #    (datetime.datetime.now().isoformat(), workerName, numReqs, rps, minRps, maxRps, meanRTime, minRTime, maxRTime) )
        #console_logger.info( 'rTimes: %s' % (data['stats_total']['response_times'].keys()) )
        #console_logger.info( 'stats_total: %s' % (data['stats_total']) )
        if data['stats_total']['max_response_time'] != maxRTime:
            console_logger.info( 'stats_total: %s' % (data['stats_total']) )
        if numFails > 0:
            console_logger.info( 'num_failures: %d' % (numFails) )

        if False:
            console_logger.info( 'disabled' )
        else:
            try:
                outString = '%s,%s,%s,%d,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f,%d,%d' % \
                    (startDateTime.isoformat(), endDateTime.isoformat(),
                        workerName, numReqs, rps, maxRps, minRps, 
                        meanRTime, maxRTime, medRTime, minRTime, numFails, nUsers
                        )
                if g_statsOutFile:
                    print( outString, file=g_statsOutFile )
                    g_statsOutFile.flush()
                else:
                    console_logger.info( 'NO g_statsOutFile (master)' )
            except:
                print( 'outString error', rps, maxRps, minRps, meanRTime, maxRTime, medRTime, minRTime, file=sys.stderr )

def on_hatch_complete( user_count=0 ):
    '''event hook function to be called by Locust '''
    console_logger.info( 'on_hatch_complete called with count %d', user_count )

def on_master_start_hatching():
    '''event hook function to be called by Locust '''
    global g_stopping
    console_logger.info( 'on_master_start_hatching called' )
    g_stopping = False

def on_master_stop_hatching():
    '''event hook function to be called by Locust '''
    global g_stopping
    console_logger.info( 'on_master_stop_hatching called' )
    g_stopping = True

# main section (executes when this module is imported)
#if '--master' in sys.argv:
if True:
    print( 'opening statsOutFile' )
    dataDirPath = 'data'
    os.makedirs( dataDirPath, exist_ok=True )
    dataFilePath = dataDirPath+'/locustStats.csv'
    if os.path.exists( dataFilePath ):
        os.remove( dataFilePath )
    openStatsOutFile( dataFilePath )
    openJLogFile( dataDirPath+'/locustStats.jlog' )


events.slave_report += on_slave_report
events.hatch_complete += on_hatch_complete
events.master_start_hatching += on_master_start_hatching
events.master_stop_hatching += on_master_stop_hatching
