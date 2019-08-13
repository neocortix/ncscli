#!/usr/bin/env python3
"""
analyze Nutch job history for ovelapping and idle times
"""
# standard imports
import argparse
#import datetime
import logging

# third-party imports
#import dateutil
import matplotlib as mpl
import matplotlib.patches as patches
if __name__ == "__main__":
    import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# neocortix imports
None


logger = logging.getLogger(__name__)



def pdTimeToSeconds( pdTime ):
    '''convert pandas (or numpy?) time stamp to seconds since epoch'''
    if isinstance( pdTime, pd.Timestamp ):
        return pdTime.to_pydatetime().timestamp()
    return 0

def plotRenderTimes( fetcherTable ):
    '''plots fetcher (and some core) job timings based on file dates'''
    fetcherCounts = fetcherTable.hostSpec.value_counts()
    fetcherNames = sorted( list( fetcherCounts.index ), reverse = True )
    #fetcherNames.append( 'core' )
    nClusters = len( fetcherNames )
    fig = plt.figure()
    ax = plt.gca()
    clusterHeight = 1
    yMargin = .25
    yMax = nClusters*clusterHeight + yMargin
    ax.set_ylim( 0, yMax )
    yTickLocs = np.arange( clusterHeight/2, nClusters*clusterHeight, clusterHeight)
    plt.yticks( yTickLocs, fetcherNames )
    tickLocator10 = mpl.ticker.MultipleLocator(10)
    ax.xaxis.set_minor_locator( tickLocator10 )
    ax.xaxis.set_major_locator( mpl.ticker.MultipleLocator(60) )
    ax.xaxis.set_ticks_position( 'both' )
    alpha = .75
    
    # get the xMin and xMax from the union of all cluster time ranges
    allStartTimes = pd.Series()
    allFinishTimes = pd.Series()
    for cluster in fetcherNames:
        #print( cluster )
        jobs = fetcherTable[fetcherTable.hostSpec==cluster]
        startTimes = jobs.dateTime
        finishTimes = jobs.dateTime + jobs.durTd
        allStartTimes = allStartTimes.append( startTimes )
        allFinishTimes = allFinishTimes.append( finishTimes )
    xMin = pdTimeToSeconds( allStartTimes.min() )
    xMax = pdTimeToSeconds( allFinishTimes.max() ) + 10
    xMax = max( xMax, pdTimeToSeconds( allStartTimes.max() ) ) # + 40
    #print( xMin, xMax )
    ax.set_xlim( xMin, xMax )
    ax.set_xlim( 0, xMax-xMin )
    
    #jobColors = { 'collect': 'tab:blue', 'rsync': 'mediumpurple', 'render':  'lightseagreen' }
    #jobColors = { 'collect': 'lightseagreen', 'rsync': 'mediumpurple', 'render':  'tab:blue' }
    jobColors = { 'collect': 'lightseagreen', 'rsync': 'tab:purple', 'render':  'tab:blue' }
    jiggers = { 'collect': .2, 'rsync': 0, 'render':  .1 }
  
    jobColor0 = mpl.colors.to_rgb( 'gray' )
   
    #jobBottom = clusterHeight * .1 + yMargin
    jobBottom = yMargin
    for cluster in fetcherNames:
        #print( cluster )
        jobs = fetcherTable[fetcherTable.hostSpec==cluster]
        # plot some things for each segment tied to this fetcher
        for row in jobs.iterrows():
            job = row[1]
            startSeconds = pdTimeToSeconds( job.dateTime ) - xMin
            durSeconds = job.duration
            if durSeconds == 0:
                durSeconds = 1
            color = jobColors.get( job.eventType, jobColor0 )
            jigger = jiggers.get( job.eventType, 0 )
            boxHeight = clusterHeight*.7
            #if job.eventType == 'rsync':
            #    boxHeight -= clusterHeight * .2
            ax.add_patch(
                patches.Rectangle(
                    (startSeconds, jobBottom-jigger),   # (x,y)
                    durSeconds,          # width
                    boxHeight,          # height
                    facecolor=color, edgecolor='k', linewidth=0.5,
                    alpha=alpha
                    )
                )
            if job.eventType == 'rsync' and (job.sequenceNum != 0) :
                label = str(job.sequenceNum)
                y = jobBottom+.1
                ax.annotate( label, xy=(startSeconds+.4, y) )
        jobBottom += clusterHeight
        
    
    
    plt.gca().grid( True, axis='x')
    plt.tight_layout();
    fig.show()

if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s',
        datefmt='%Y/%m/%d %H:%M:%S')
    logger.setLevel(logging.DEBUG)
    logger.debug( 'the logger is configured' )

    ap = argparse.ArgumentParser( description=__doc__)
    #ap.add_argument( '--dbName', default=defaultDbName, help='the crawl db name' )
    args = ap.parse_args()
    logger.debug( 'args: %s', args )
    
    inFilePath = 'data/perfLog.csv'  # perfLog_1080p-2 perfLog_short perfLog_t3-small_540p
    timingTable = pd.read_csv( inFilePath )
    timingTable['dateTime'] = pd.to_datetime( timingTable.dateTimeStr )
    timingTable['durTd'] = pd.to_timedelta(timingTable.duration, unit='s')
    
    plotRenderTimes( timingTable )
    
    
