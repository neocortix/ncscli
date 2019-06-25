#!/usr/bin/env python3
"""
analyze statistics from load test
"""


# standard library modules
import datetime
import json
import logging
#import logging.handlers
import sys
# third-party modules
import dateutil
import dateutil.parser
#import dateutil.tz
import geoip2.webservice  # maxmind geolocation service
import jinja2
import pandas as pd


logger = logging.getLogger(__name__)


g_geoip2Client = None
g_stats = pd.DataFrame()  # HACK, should not need to be global


def initLogging():
    # configure logger formatting
    logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
    logDateFmt = '%Y/%m/%d %H:%M:%S'
    logging.basicConfig(format=logFmt, datefmt=logDateFmt)
    logger.setLevel( logging.INFO )

    # done
    logger.debug('the logger is configured')


def getGeoip2Info( ipAddr, client ):
    ''' return geolocation info from MaxMind geoip2 service'''
    #logger.info( 'getting %s', ipAddr )
    result = {}
    try:
        response = client.city( ipAddr )
        result['city'] = response.city.name
        result['countryCode'] = response.country.iso_code
        result['countryName'] = response.country.name
        result['domain'] = response.traits.domain
        result['isp'] = response.traits.isp
        result['latitude'] = response.location.latitude
        result['longitude'] = response.location.longitude
        result['postal'] = response.postal.code
        result['stateCode'] = response.subdivisions.most_specific.iso_code
        result['stateName'] = response.subdivisions.most_specific.name
        result['addr'] = ipAddr
        #logger.info( 'city: %s', city )
    except Exception as exc:
        logger.error( 'got exception (%s) %s', type(exc), exc)
    
    return result

def getHostLocationsMaxmind( ipAddrs, geoip2Client ):
    locations = ipAddrs.apply( getGeoip2Info, client=geoip2Client )
    locationTable = pd.DataFrame( list(locations) )
    return locationTable.set_index( 'addr' )

def getHostLocationsNcs():
    # read launched.json to get ncs location data
    launchedJsonFilePath = 'launched.json'
    with open( launchedJsonFilePath, 'r' ) as inFile:
        instancesAllocated = json.load( inFile )
    #logger.debug( 'instancesAllocated %s', instancesAllocated ) 
    locations = []
    for inst in instancesAllocated:
        if 'device-location' in inst:
            #ansibleName = 'phone_'+inst['instanceId']
            #logger.info( 'inst %s, loc %s', inst['instanceId'], inst['device-location'] )
            rec = { 'addr': inst['instanceId'] }
            locInfo = inst['device-location']
            if not locInfo['locality']:
                logger.warning( 'no locality for %s %s', inst['instanceId'], locInfo )
            rec['city'] = locInfo['locality']
            rec['stateCode'] = locInfo['area']
            rec['countryCode'] = locInfo['country-code']
            locations.append( rec )
            #logger.info( 'locRec %s', rec )
    locationTable = pd.DataFrame( list(locations) )
    #print( 'locationTable', locationTable )
    return locationTable.set_index( 'addr' )

def getLocKey( addr, locs ):
    row = locs.ix[ addr ]
    return '%s.%s.%s' % ( row.countryCode, row.stateCode, row.city )
    #return str( (row.latitude, row.longitude) )  #str of tuple

def extractWorkerId( workerName ):
    #if '_phone_' in workerName:
    if '_' in workerName:
        return workerName.split('_')[-1]  # instance id
        #return workerName.split('_')[0]  # IP addr
    else:
        return workerName

def loadStats( inFilePath, workerLocs ):
    # load stats from .csv file
    stats = pd.read_csv( inFilePath )
    
    # map hostnames in the dataframe to IPs
    #stats['workerIP'] = stats.worker.map( hostNameIPs )

    stats['workerIP'] = stats.worker.apply( extractWorkerId )
    '''
    if 'locKey' not in workerLocs:
        workerIPs = pd.Series( stats.workerIP.unique() )
        # geoip the workers, and fill in blank entries
        workerLocs = getHostLocations( workerIPs, g_geoip2Client )
        workerLocs.fillna( {'city': 'cc', 'stateCode': 'ss'}, inplace=True )
        
        # create composite key for locations
        workerLocs['locKey'] = workerLocs.countryCode + '.' + workerLocs.stateCode + '.' + workerLocs.city
    '''
    if 'locKey' in workerLocs:
        stats['locKey'] = stats.workerIP.map( workerLocs.locKey )


    #stats['locKey'] = stats.workerIP.apply( getLocKey, locs=workerLocs )
    return stats

def reportCompiledStats( stats ):
    resultsSummary = {}

    startDateTime = dateutil.parser.parse(stats.dateTime.min())
    startDateTimeUtc = startDateTime.replace( tzinfo=datetime.timezone.utc )
    resultsSummary['startDateTimeStr'] = startDateTimeUtc.isoformat()

    endDateTime = dateutil.parser.parse(stats.endDateTimeStr.max())
    durSeconds = (endDateTime-startDateTime).total_seconds()
    resultsSummary['durSeconds'] = durSeconds
    durMinutes = durSeconds / 60

    nDevices = len( stats['workerIP'].unique() )
    resultsSummary['nDevices'] = nDevices
    
    nFails = stats['nFails'].sum() if 'nFails' in stats else 0
    resultsSummary['nFails'] = int( nFails )

    print( 'Load Test started:', startDateTime.strftime('%Y/%m/%d %H:%M:%S%z') )
    print( 'Duration %.1f minutes'% durMinutes )

    print( '\n# of worker devices:', nDevices )
    #print( '# of geo regions:', len( stats['locKey'].unique() ) )


    #print( '\nGlobal Summary' )
    nReqs = int(stats['nr'].sum())
    nReqsSatisfied = nReqs - nFails
    #nReqsSatisfied = int(stats['nr'].sum())  # probably includes failures now that locust bug is fixed
    resultsSummary['nReqsSatisfied'] = int(nReqsSatisfied)
    print( '# of requests satisfied:', nReqsSatisfied )
    rps = stats['nr'].sum() / durSeconds
    print( 'requests per second: %.1f' % (rps) )
    print( 'RPS per device: %.2f' % (rps / nDevices) )

    if (nFails + nReqsSatisfied):
        failRate = nFails / (nFails + nReqsSatisfied)
    else:
        failRate = 0
    print( '# of requests failed:', nFails )
    print( 'failure rate: %.1f%%' % (failRate * 100) )

    meanResponseTime = stats['mspr'].mean()
    print( 'mean response time: %.1f ms' % meanResponseTime )
    resultsSummary['meanResponseTimeMs'] = meanResponseTime
    print( 'response time range: %.1f-%.1f ms' % (stats['msprMin'].min(), stats['msprMax'].max() ) )
 

    return resultsSummary

    #print( '\nRPS (per worker)' )
    #print( stats.rps.describe( [.05, .95] ) )
    
    #print( '\nresponse times' )
    #print( stats.mspr.describe( [.05, .95] ) )
 
    #print( '\nRPS (per worker) by location' )
    #print( stats.groupby('locKey').rps.describe( percentiles=[.05, .95] )  )

    #print( '\nresponse times by location' )
    #print( stats.groupby('locKey').mspr.describe( percentiles=[.05, .95] )  )

def deriveStats( stats ):
    result = pd.Series()
    startDateTime = dateutil.parser.parse(stats.dateTime.min())
    endDateTime = dateutil.parser.parse(stats.endDateTimeStr.max())
    durSeconds = (endDateTime-startDateTime).total_seconds()
    durMinutes = durSeconds / 60
    
    nDevices = len( stats['worker'].unique() )
    
    #print( 'Load Test started:', startDateTime.strftime('%Y/%m/%d %H:%M:%S%z') )
    #print( 'Duration %.1f minutes'% durMinutes )

    #print( '\n# of worker devices:', nDevices )
    #print( '# of geo regions:', len( stats['locKey'].unique() ) )


    #print( '\nGlobal Summary' )
    #print( '# of requests:', stats['nr'].sum() )
    rps = stats['nr'].sum() / durSeconds
    #print( 'requests per second: %.1f' % (rps) )
    #print( 'RPS per device: %.1f' % (rps / nDevices) )
    #print( 'mean response time: %.1f ms' % (stats['mspr'].mean()) )
    #rtRange = '%.1f-%.1f ms' % (stats['msprMin'].min(), stats['msprMax'].max() )
    #print( 'response time range: %.1f-%.1f ms' % (stats['msprMin'].min(), stats['msprMax'].max() ) )
    
    numReqs = stats['nr'].sum()
    numFails = stats['nFails'].sum() if 'nFails' in stats else 0

    s = pd.Series( { 'started': startDateTime.strftime('%Y/%m/%d %H:%M:%S%z') } )
    result = result.append( s )
    result = result.append( pd.Series( { 'durMinutes': durMinutes } ) )
    result = result.append( pd.Series( { 'devices': nDevices } ) )
    result = result.append( pd.Series( { 'requests': numReqs } ) )
    result = result.append( pd.Series( { 'failures': numFails } ) )
    result = result.append( pd.Series( { 'failPct': numFails * 100 / (numReqs+numFails) } ) )
    result = result.append( pd.Series( { 'rps': rps } ) )
    result = result.append( pd.Series( { 'rpsPerDev': (rps / nDevices) } ) )
    result = result.append( pd.Series( { 'mean rt': stats['mspr'].mean() } ) )
    result = result.append( pd.Series( { 'median rt': stats['msprMed'].median() } ) )
    result = result.append( pd.Series( { '90Pct rt': stats['msprMax'].quantile(.90) } ) )
    #result = result.append( pd.Series( { 'rt range': rtRange } ) )
    result = result.append( pd.Series( { 'max rt': stats['msprMax'].max() } ) )

    return result    

def genHtmlTable( df ):
    return df.to_html( index=False, classes=['sortable'], justify='left',
                      float_format=lambda x: '%.1f' % x
                      )

def compileStats( dataDirPath ):
    statsFileName = 'locustStats.csv'  # locustStats.csv locustStats_17devs.csv
    global g_geoip2Client
    g_geoip2Client = geoip2.webservice.Client( '134556', 'mzV4c9IXWRyn' )

    #geoInfo = getGeoip2Info( '34.216.219.139', g_geoip2Client )
    #print( geoInfo )
    
    #dataDirPath = 'data'  # '../../loadtest/data
    
    if False:
        # get hostnames indexed by ip addresses
        try:
            ipHostNames=json.load( open(dataDirPath+'/ip-hostnames.json'))
            if len( set(ipHostNames.values()) ) != len( ipHostNames.values() ):
                logger.warning( 'hostnames are not unique' )
                print( sorted(ipHostNames.values()) )
        except Exception:
            ipHostNames = {}
            
        # invert to get  ip addresses indexed by hostnames (hopefully unique)
        #hostNameIPs = {v: k for k, v in ipHostNames.items()}
            
    usingMaxmind = False
    if True:  # if len( ipHostNames ):
        # geoip the workers, and fill in blank entries
        if usingMaxmind:
            workerIPs = pd.Series( list(ipHostNames.keys()) )
            workerLocs = getHostLocationsMaxmind( workerIPs, g_geoip2Client )
        else:
            workerLocs = getHostLocationsNcs()
        workerLocs.fillna( {'city': 'cc', 'stateCode': 'ss'}, inplace=True )
        
        # create composite key for locations
        workerLocs['locKey'] = workerLocs.countryCode + '.' + workerLocs.stateCode
        #workerLocs['locKey'] = workerLocs.countryCode + '.' + workerLocs.stateCode + '.' + workerLocs.city
    else:
        workerLocs = pd.DataFrame()
    
    # load stats from .csv file
    global g_stats
    g_stats = loadStats( dataDirPath+'/'+statsFileName, workerLocs )

    


    # fill in any unknown values
    g_stats.fillna( {'locKey': '.unknown', 'workerIP': '0.0.0.0'}, inplace=True )
    
    # to disable geo-grouping (thus enabling IP-grouping)
    #g_stats['locKey'] = g_stats.workerIP

    outDf = pd.DataFrame()
    if 'locKey' in g_stats:
        # do per-region summary
        outDf = pd.DataFrame()
        locKeys = g_stats['locKey'].unique()
        #print( '\nGlobal Summary' )
        #print( '# of geo regions:', len( locKeys ) )
        #print( locKeys )
        these = deriveStats( g_stats )
        these = pd.Series( {'locKey': '.global'} ).append( these )
        outDf = outDf.append( [these] )
        
        for locKey in locKeys:
            #print( '\nRegion: ', locKey )
            lStats = g_stats[ g_stats.locKey == locKey ]
            if len( lStats ):
                these = deriveStats( lStats )
                these = pd.Series( {'locKey': locKey} ).append( these )
                outDf = outDf.append( [these] )
    perRegion = outDf.reset_index( drop=True )
    
    outDf = pd.DataFrame()
    #workerKeys = g_stats['workerIP'].unique()
    workerKeys = g_stats['worker'].unique()
    
    for key in workerKeys:
        #lStats = g_stats[ g_stats['workerIP'] == key ]
        lStats = g_stats[ g_stats['worker'] == key ]
        if len( lStats ):
            these = deriveStats( lStats )
            if 'locKey' in lStats:
                locKey = lStats.locKey.iloc[0]
            else:
                locKey = '.unknown'
            these = pd.Series( {'worker': key, 'locKey': locKey} ).append( these )
            outDf = outDf.append( [these] )

    perWorker = outDf.reset_index( drop=True )
    perWorker = perWorker.drop( ['devices', 'rps'], 1 )

    print( 'per-country worker counts', file=sys.stderr )
    countryCodes = perWorker['locKey'].str.slice(0, 2)
    print( countryCodes.value_counts(), file=sys.stderr )

    worstCases = g_stats[ (g_stats.msprMax > 15000) | (g_stats.mspr > 3000) | (g_stats.msprMed > 3000)  ]

    regionTable = perRegion.to_html( index=False, classes=['sortable'], justify='left', float_format=lambda x: '%.1f' % x )
    workerTable = perWorker.to_html( index=False, classes=['sortable'], justify='left', float_format=lambda x: '%.1f' % x )
    worstCasesTable = genHtmlTable( worstCases )

    if True:
        envir = jinja2.Environment( 
                loader = jinja2.FileSystemLoader(sys.path),
                autoescape=jinja2.select_autoescape(['html', 'xml'])
                )
        template = envir.get_template('ltStats.html.j2')
        html = template.render( ltRegionTable=regionTable,
            ltWorkerTable=workerTable,ltWorstCasesTable=None )
    else:
        html = '<html> <body>\n%s\n</body></html>\n' % regionTable

    return html    

def reportStats( dataDirPath = 'data' ):
    global g_stats
    g_stats = pd.DataFrame()
    

    html = compileStats( dataDirPath )
        
    with open( dataDirPath+'/ltStats.html', 'w', encoding='utf8') as htmlOutFile:
        htmlOutFile.write( html )

    resultsSummary = reportCompiledStats( g_stats )
    logger.info( 'resultsSummary %s', resultsSummary )
    return resultsSummary


if __name__ == "__main__":
    initLogging()
    
    reportStats()
    '''
    g_dataDirPath = 'data'
    g_stats = pd.DataFrame()
    

    html = compileStats( g_dataDirPath )
        
    with open( g_dataDirPath+'/ltStats.html', 'w', encoding='utf8') as htmlOutFile:
        htmlOutFile.write( html )

    resultsSummary = reportCompiledStats( g_stats )
    logger.info( 'resultsSummary %s', resultsSummary )
    '''
