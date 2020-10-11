#!/usr/bin/env python3
"""
plots loadtest results produced by runBatchJMeter
"""
# standard library modules
import argparse
import csv
import glob
import json
import logging
import math
import os
import re
import sys
import warnings
# third-party modules
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def demuxResults( inFilePath ):
    instanceList = []
    with open( inFilePath, 'rb' ) as inFile:
        for line in inFile:
            decoded = json.loads( line )
            # print( 'decoded', decoded ) # just for debugging, would be verbose
            # iid = decoded.get( 'instanceId', '<unknown>')
            if 'args' in decoded:
                # print( decoded['args'] )
                if 'state' in decoded['args']:
                    if decoded['args']['state'] == 'retrieved':
                        # print("%s  %s" % (decoded['args']['frameNum'],decoded['instanceId']))
                        instanceList.append([decoded['args']['frameNum'],decoded['instanceId']])
    return instanceList

def ingestGatlingLog( inFilePath ):
    '''read the tab-delimited file; return contents as a list of dicts'''
    fieldNames = ['scope', 'class', 'which', 'startTime', 'endTime', 'status' ]
    rows = []
    with open( inFilePath, newline='') as csvfile:
        reader = csv.DictReader(csvfile, dialect='excel-tab', fieldnames=fieldNames)
        for row in reader:
            rows.append( row )
    return rows

def getColumn(inputList,column):
    return [inputList[i][column] for i in range(0,len(inputList))]

def flattenList(inputList):
    return [num for elem in inputList for num in elem]

def makeTimelyXTicks():
    # x-axis tick marks at multiples of 60 and 10
    ax = plt.gca()
    ax.xaxis.set_major_locator( mpl.ticker.MultipleLocator(60) )
    ax.xaxis.set_minor_locator( mpl.ticker.MultipleLocator(10) )


if __name__ == "__main__":
    # configure logger formatting
    logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
    logDateFmt = '%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
    logging.basicConfig(format=logFmt, datefmt=logDateFmt)

    # treat numpy deprecations as errors
    warnings.filterwarnings('error', category=np.VisibleDeprecationWarning)

    ap = argparse.ArgumentParser( description=__doc__, fromfile_prefix_chars='@', formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    ap.add_argument( '--dataDirPath', required=True, help='the path to to directory for input and output data' )
    args = ap.parse_args()

    logger.info( 'plotting data in directory %s', os.path.realpath(args.dataDirPath)  )

    
    outputDir = args.dataDirPath
    launchedJsonFilePath = outputDir + "/recruitLaunched.json"
    print("launchedJsonFilePath = %s" % launchedJsonFilePath)
    jlogFilePath = outputDir + "/batchRunner_results.jlog"
    print("jlogFilePath = %s\n" % jlogFilePath)

    if not os.path.isfile( launchedJsonFilePath ):
        logger.error( 'file not found: %s', launchedJsonFilePath )
        sys.exit( 1 )

    launchedInstances = []
    with open( launchedJsonFilePath, 'r') as jsonInFile:
        try:
            launchedInstances = json.load(jsonInFile)  # an array
        except Exception as exc:
            sys.exit( 'could not load json (%s) %s' % (type(exc), exc) )
    if False:
        print(len(launchedInstances))
        print(launchedInstances[0])
        print(launchedInstances[0]["instanceId"])
        # print(launchedInstances[0]["device-location"])
        print(launchedInstances[0]["device-location"]["latitude"])
        print(launchedInstances[0]["device-location"]["longitude"])
        print(launchedInstances[0]["device-location"]["display-name"])
        print(launchedInstances[0]["device-location"]["country"])

    completedJobs = demuxResults(jlogFilePath)

    mappedFrameNumLocation = []
    mappedFrameNumLocationUnitedStates = []
    mappedFrameNumLocationRussia = []
    mappedFrameNumLocationOther = []
    
    for i in range(0,len(completedJobs)):
        for j in range(0,len(launchedInstances)):
            if launchedInstances[j]["instanceId"] == completedJobs[i][1]:
                mappedFrameNumLocation.append([completedJobs[i][0],
                                           launchedInstances[j]["device-location"]["latitude"],
                                           launchedInstances[j]["device-location"]["longitude"],
                                           launchedInstances[j]["device-location"]["display-name"],
                                           launchedInstances[j]["device-location"]["country"]
                                           ])
                if launchedInstances[j]["device-location"]["country"] == "United States":
                    mappedFrameNumLocationUnitedStates.append([completedJobs[i][0],
                                               launchedInstances[j]["device-location"]["latitude"],
                                               launchedInstances[j]["device-location"]["longitude"],
                                               launchedInstances[j]["device-location"]["display-name"],
                                               launchedInstances[j]["device-location"]["country"]
                                               ])
                elif launchedInstances[j]["device-location"]["country"] == "Russia":
                    mappedFrameNumLocationRussia.append([completedJobs[i][0],
                                               launchedInstances[j]["device-location"]["latitude"],
                                               launchedInstances[j]["device-location"]["longitude"],
                                               launchedInstances[j]["device-location"]["display-name"],
                                               launchedInstances[j]["device-location"]["country"]
                                               ])
                else:
                    mappedFrameNumLocationOther.append([completedJobs[i][0],
                                               launchedInstances[j]["device-location"]["latitude"],
                                               launchedInstances[j]["device-location"]["longitude"],
                                               launchedInstances[j]["device-location"]["display-name"],
                                               launchedInstances[j]["device-location"]["country"]
                                               ])
                

    print("\nLocations:")
    for i in range(0,len(mappedFrameNumLocation)):
        print("%s" % mappedFrameNumLocation[i][3])
        
        

    print("\nReading Response Time data")
    resultFilePaths = []

    workerDirs = glob.glob( os.path.join( outputDir, 'gatlingResults_*' ) )
    print( 'workerDirs', workerDirs )
    for workerDir in workerDirs:
        dirContents = os.listdir( workerDir )
        if dirContents:
            innerPath = os.path.join( workerDir, dirContents[0] )
            if os.path.isdir( innerPath ):
                #print( 'found dir', innerPath )
                filePath = os.path.join( innerPath, 'simulation.log' )
                if os.path.isfile( filePath ):
                    #print( 'found file', filePath )
                    resultFilePaths.append( filePath )

    numResultFiles = len(resultFilePaths)    

    # read the simulation.log files (which contain timings for every request)
    pat = r'gatlingResults_([^/]*)'
    responseData = []
    for i in range(0,numResultFiles):
        inFilePath = resultFilePaths[i]
        logger.debug( 'reading %s', inFilePath )
        rows = ingestGatlingLog( inFilePath )
        if not rows:
            logger.info( 'no rows in %s', inFilePath )
            continue
        match = re.search( pat, inFilePath ).group(1)
        print( 'found frameNum', match, 'in', inFilePath )
        frameNum = int( match )
        startTimes = []
        elapsedTimes = []
        for row in rows:
            #print( 'considering row', row )
            if row['scope'] == 'REQUEST':  #  and row['which'] == 'request_1'
                startTime = float(row['startTime'])/1000
                endTime = float(row['endTime'])/1000
                startTimes.append( startTime )
                elapsedTimes.append( endTime-startTime )
        if startTimes:
            minStartTimeForDevice = min(startTimes)
            jIndex = -1
            for j in range (0,len(mappedFrameNumLocation)):
                if frameNum == mappedFrameNumLocation[j][0]:
                    jIndex = j
            responseData.append([frameNum,minStartTimeForDevice,startTimes,elapsedTimes,mappedFrameNumLocation[jIndex]])
    if not responseData:
        sys.exit( 'no plottable data was found' )

    # first, time-shift all startTimes by subtracting the minStartTime for each device
    # and compute the maxStartTime (i.e. test duration) for each device
    relativeResponseData = []
    for i in range(0,len(responseData)):
        relativeStartTimes = []
        for ii in range(0,len(responseData[i][2])):
            # difference = responseData[i][2][ii]-globalMinStartTime
            # if i==2 and ii<3700 and difference > 500:
            #     print("i = %d   ii = %d   difference = %f    data = %f" % (i,ii,difference,responseData[i][2][ii] ))
            # relativeStartTimes.append(responseData[i][2][ii]-globalMinStartTime)
            relativeStartTimes.append(responseData[i][2][ii]-responseData[i][1])
        maxStartTime = max(relativeStartTimes)
        relativeResponseData.append([responseData[i][0],relativeStartTimes,responseData[i][3],responseData[i][4],maxStartTime])

    # compute median maxStartTime
    medianMaxStartTime = np.median(getColumn(relativeResponseData,4))
    print("medianMaxStartTime = %f" % medianMaxStartTime)

    # remove device records which ran too long
    # print(relativeResponseData[0])
    culledRelativeResponseData = []
    cullResponseData = True
    excessDurationThreshold = 30  # in seconds
    for i in range(0,len(relativeResponseData)):
        if cullResponseData:
            # print("i = %d   min, max = %f  %f" % (i,min(relativeResponseData[i][1]),max(relativeResponseData[i][1])))
            if relativeResponseData[i][4]<(medianMaxStartTime+excessDurationThreshold):
                # print("min, max = %f  %f" % (min(relativeResponseData2[i][1]),max(relativeResponseData2[i][1])))
                culledRelativeResponseData.append(relativeResponseData[i])
        else:
            culledRelativeResponseData.append(relativeResponseData[i])

    print("Number of devices = %d" % len(relativeResponseData))
    print("Culled Number of devices = %d" %len(culledRelativeResponseData))
    culledLocations = getColumn(getColumn(culledRelativeResponseData,3),3)

    print("\nCulled Locations:")
    for i in range(0,len(culledLocations)):
        print("%s" % culledLocations[i])
        
    print("\nAnalyzing Location data")
    startRelTimesAndMSPRsUnitedStatesMuxed = []
    startRelTimesAndMSPRsRussiaMuxed = []
    startRelTimesAndMSPRsOtherMuxed = []
    clipTimeInSeconds = 3.0  # normally 3.00

    for i in range(0,len(culledRelativeResponseData)):
        # print(culledRelativeResponseData[i][3][4])
        if culledRelativeResponseData[i][3][4]=="United States" :
            startRelTimesAndMSPRsUnitedStatesMuxed.append([culledRelativeResponseData[i][1],culledRelativeResponseData[i][2] ])
        elif culledRelativeResponseData[i][3][4]=="Russia" :     
            startRelTimesAndMSPRsRussiaMuxed.append([culledRelativeResponseData[i][1],culledRelativeResponseData[i][2] ])
        else:
            startRelTimesAndMSPRsOtherMuxed.append([culledRelativeResponseData[i][1],culledRelativeResponseData[i][2] ])

    startRelTimesAndMSPRsUnitedStates = [flattenList(getColumn(startRelTimesAndMSPRsUnitedStatesMuxed,0)),flattenList(getColumn(startRelTimesAndMSPRsUnitedStatesMuxed,1))]
    startRelTimesAndMSPRsRussia = [flattenList(getColumn(startRelTimesAndMSPRsRussiaMuxed,0)),flattenList(getColumn(startRelTimesAndMSPRsRussiaMuxed,1))]
    startRelTimesAndMSPRsOther = [flattenList(getColumn(startRelTimesAndMSPRsOtherMuxed,0)),flattenList(getColumn(startRelTimesAndMSPRsOtherMuxed,1))]

    # print(len(startRelTimesAndMSPRsUnitedStates[0]))
    # print(len(startRelTimesAndMSPRsRussia[0]))
    # print(len(startRelTimesAndMSPRsOther[0]))

    print("Determining Delivered Load")
    timeBinSeconds = 5
    culledRequestTimes = []
    for i in range(0,len(culledRelativeResponseData)):
        # print("min, max = %f  %f" % (min(culledRelativeResponseData[i][1]),max(culledRelativeResponseData[i][1])))
        culledRequestTimes.append(culledRelativeResponseData[i][1])

    flattenedCulledRequestTimes = flattenList(culledRequestTimes)
    maxCulledRequestTimes = max(flattenedCulledRequestTimes)
    print("Number of Responses = %d" %len(flattenedCulledRequestTimes))
    print("Max Culled Request Time = %.2f" % maxCulledRequestTimes)
    numBins = int(np.floor(maxCulledRequestTimes / timeBinSeconds + 3))
    # print(numBins)
    deliveredLoad = np.zeros(numBins)
    deliveredLoadTimes = np.zeros(numBins)
    for i in range(0,len(flattenedCulledRequestTimes)):
        bin = int(np.floor(flattenedCulledRequestTimes[i]/timeBinSeconds))+1
        deliveredLoad[bin] += 1/timeBinSeconds

    for i in range(0,len(deliveredLoadTimes)):
        deliveredLoadTimes[i] = i*timeBinSeconds
    # print(deliveredLoad)
    # print(deliveredLoadTimes)



    print("\nReading World Map data")
    mapFileName = "./WorldCountryBoundaries.csv"
    mapFile = open(mapFileName, "r")
    mapLines = mapFile.readlines()
    mapFile.close()
    mapNumLines = len(mapLines)    

    CountryData = []
    CountrySphericalData = []

    # for i in range(1,8) :
    for i in range(1,mapNumLines) :
        firstSplitString = mapLines[i].split("\"")
        nonCoordinateString = firstSplitString[2]    
        noncoordinates = nonCoordinateString.split(",")
        countryString = noncoordinates[6]

        if firstSplitString[1].startswith('<Polygon><outerBoundaryIs><LinearRing><coordinates>') and firstSplitString[1].endswith('</coordinates></LinearRing></outerBoundaryIs></Polygon>'):
            coordinateString = firstSplitString[1].replace('<Polygon><outerBoundaryIs><LinearRing><coordinates>','').replace('</coordinates></LinearRing></outerBoundaryIs></Polygon>','').replace(',0 ',',0,')
            # print("coordinateString = %s" % coordinateString)
            # print("nonCoordinateString = %s" % nonCoordinateString)
            coordinates = [float(j) for j in coordinateString.split(",")]  
            coordinateList = np.zeros([int(len(coordinates)/3),2])
            for j in range(0,len(coordinateList)) :
                coordinateList[j,:] = coordinates[j*3:j*3+2]
            coordinateSphericalList = np.zeros([int(len(coordinates)/3),3])
            for j in range(0,len(coordinateSphericalList)) :
                r = 1
                phi = 2*math.pi*coordinates[j*3]/360
                theta = 2*math.pi*(90-coordinates[j*3+1])/360
                coordinateSphericalList[j,0] = r * np.sin(theta) * np.cos(phi)
                coordinateSphericalList[j,1] = r * np.sin(theta) * np.sin(phi)
                coordinateSphericalList[j,2] = r * np.cos(theta)

            # print("noncoordinates = %s" % str(noncoordinates))
            # print("countryString = %s" % countryString)
            # print("coordinateList = %s" % str(coordinateList))
            CountryData.append([countryString,coordinateList])
            CountrySphericalData.append([countryString,coordinateSphericalList])
        else :
            # print("Exception Line %i  %s" % (i,countryString))
            # if firstSplitString[1].startswith("<MultiGeometry>") :
            #     print("MultiGeometry  Line %i  %s" % (i,countryString))
            # else :
            #     print("Inner Boundary Line %i  %s" % (i,countryString))
            reducedCoordinateString = firstSplitString[1].replace('<MultiGeometry>','').replace('</MultiGeometry>','').replace('<Polygon>','').replace('</Polygon>','').replace('<outerBoundaryIs>','').replace('</outerBoundaryIs>','').replace('<innerBoundaryIs>','').replace('</innerBoundaryIs>','').replace('<LinearRing>','').replace('</LinearRing>','').replace('</coordinates>','').replace(',0 ',',0,')
            # print("reducedCoordinateString = %s" % reducedCoordinateString)
            coordinateStringSets = reducedCoordinateString.split("<coordinates>")
            # print("coordinateStringSets = %s" % str(coordinateStringSets))
            coordinateSets= []
            for j in range(1,len(coordinateStringSets)) :
                coordinateSets.append([float(k) for k in coordinateStringSets[j].split(",")])
            # print("coordinateSets = %s" % str(coordinateSets))
            coordinateList = []
            coordinateSphericalList = []
            for j in range(0,len(coordinateSets)) :
                # print("\ncoordinateSets[%i] = %s" % (j,str(coordinateSets[j])))
                coordinateList.append(np.zeros([int(len(coordinateSets[j])/3),2]))
                for k in range(0,len(coordinateList[j])) :
                    coordinateList[j][k,:] = coordinateSets[j][k*3:k*3+2]
                # print("\ncoordinateList[%i] = %s" % (j,str(coordinateList[j])))
                coordinateSphericalList.append(np.zeros([int(len(coordinateSets[j])/3),3]))
                for k in range(0,len(coordinateSphericalList[j])) :
                    r = 1
                    phi = 2*math.pi*coordinateSets[j][k*3]/360
                    theta = 2*math.pi*(90-coordinateSets[j][k*3+1])/360
                    coordinateSphericalList[j][k,0] = r * np.sin(theta) * np.cos(phi)
                    coordinateSphericalList[j][k,1] = r * np.sin(theta) * np.sin(phi)
                    coordinateSphericalList[j][k,2] = r * np.cos(theta)

            CountryData.append([countryString,coordinateList])
            CountrySphericalData.append([countryString,coordinateSphericalList])

    figSize1 = (19.2, 10.8)
    fontFactor = 0.75
    mpl.rcParams.update({'font.size': 22})
    mpl.rcParams['axes.linewidth'] = 2 #set the value globally
    markerSizeValue = 10

    # plot world map
    fig = plt.figure(3, figsize=figSize1)
    ax = fig.gca()
    # Turn off tick labels
    ax.set_yticklabels([])
    ax.set_xticklabels([])
    # ax.set_aspect('equal')
    # for i in range(0,20) :
    colorValue = 0.85
    edgeColor = None  # (colorValue*.9, colorValue*.9, colorValue*.9)

    for i in range(0,len(CountryData)) :
        if isinstance( CountryData[i][1], np.ndarray ):
            ax.add_artist(plt.Polygon(CountryData[i][1],edgecolor=edgeColor,
                facecolor=(colorValue,colorValue,colorValue),aa=True))
        else :
            for j in range(0,len(CountryData[i][1])) :
                ax.add_artist(plt.Polygon(CountryData[i][1][j],edgecolor=edgeColor,
                    facecolor=(colorValue,colorValue,colorValue),aa=True))

    plt.plot(getColumn(mappedFrameNumLocationUnitedStates,2),getColumn(mappedFrameNumLocationUnitedStates,1),linestyle='', color=(0.0, 0.5, 1.0),marker='o',markersize=markerSizeValue)
    plt.plot(getColumn(mappedFrameNumLocationRussia,2),getColumn(mappedFrameNumLocationRussia,1),linestyle='', color=(1.0, 0.0, 0.0),marker='o',markersize=markerSizeValue)
    plt.plot(getColumn(mappedFrameNumLocationOther,2),getColumn(mappedFrameNumLocationOther,1),linestyle='', color=(0.0, 0.9, 0.0),marker='o',markersize=markerSizeValue)
    plt.xlim([-180,180])
    plt.ylim([-60,90])
    #plt.show()
    plt.savefig( outputDir+'/worldMap.png', bbox_inches='tight')

    plotMarkerSize = 3
    plt.figure(10, figsize=figSize1)
    plt.plot(startRelTimesAndMSPRsUnitedStates[0],startRelTimesAndMSPRsUnitedStates[1], linestyle='', color=(0.0, 0.6, 1.0),marker='o',markersize=plotMarkerSize)
    plt.plot(startRelTimesAndMSPRsRussia[0],startRelTimesAndMSPRsRussia[1], linestyle='', color=(1.0, 0.0, 0.0),marker='o',markersize=plotMarkerSize)
    plt.plot(startRelTimesAndMSPRsOther[0],startRelTimesAndMSPRsOther[1], linestyle='', color=(0.0, 1.0, 0.0),marker='o',markersize=plotMarkerSize)
    plt.ylim([0,clipTimeInSeconds])
    #makeTimelyXTicks()
    plt.title("Response Times (s)\n", fontsize=42*fontFactor)
    plt.xlabel("Time during Test (s)", fontsize=32*fontFactor)  
    plt.ylabel("Response Times (s)", fontsize=32*fontFactor)  
    plt.savefig( outputDir+'/responseTimesByRegion.png', bbox_inches='tight' )
    #plt.show()    
    # plt.clf()
    # plt.close()  

    plt.figure(2, figsize=figSize1)
    plt.plot( deliveredLoadTimes, deliveredLoad, linewidth=5, color=(0.0, 0.6, 1.0) )
    #makeTimelyXTicks()
    # plt.xlim([0,270])
    plt.title("Delivered Load During Test\n", fontsize=42*fontFactor)
    plt.xlabel("Time during Test (s)", fontsize=32*fontFactor)  
    plt.ylabel("Requests per second", fontsize=32*fontFactor)  
    plt.savefig( outputDir+'/deliveredLoad.png', bbox_inches='tight' )
    #plt.show()
