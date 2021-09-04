#!/usr/bin/env python3
"""
plots loadtest results produced by runBatchJMeter
"""
# standard library modules
import argparse
import json
import logging
import math
import os
import sys
import warnings
# third-party modules
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np

from shutil import copyfile
from datetime import datetime

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def boolArg( v ):
    '''use with ArgumentParser add_argument for (case-insensitive) boolean arg'''
    if v.lower() == 'true':
        return True
    elif v.lower() == 'false':
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

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

def getColumn(inputList,column):
    return [inputList[i][column] for i in range(0,len(inputList))]

def flattenList(inputList):
    return [num for elem in inputList for num in elem]

def makeTimelyXTicks():
    # x-axis tick marks at multiples of 60 and 10
    ax = plt.gca()
    ax.xaxis.set_major_locator( mpl.ticker.MultipleLocator(60) )
    ax.xaxis.set_minor_locator( mpl.ticker.MultipleLocator(10) )
    
def getFieldsFromFileNameCSV3(fileName,firstRecord=0) :
    file = open(fileName, "r", encoding='utf-8')
    rawLines = file.readlines()

    # remove newlines from quoted strings
    lines = []
    assembledLine = ""
    for i in range(0,len(rawLines)):
    # for i in range(0,20):
        numQuotesInLine = len(rawLines[i].split('"'))-1 
        if assembledLine == "":
            if (numQuotesInLine % 2) == 0:
                lines.append(rawLines[i])
            else:
                assembledLine = assembledLine + rawLines[i].replace("\n"," ")
        else:
            if (numQuotesInLine % 2) == 0:
                assembledLine = assembledLine + rawLines[i].replace("\n"," ")
            else:
                assembledLine = assembledLine + rawLines[i]
                lines.append(assembledLine)
                # print(assembledLine)
                assembledLine = ""
                
    # need to handle quoted substrings 
    for i in range(0,len(lines)):
        if '"' in lines[i]:
            # print ("\nline = %s" % lines[i])
            lineSplitByQuotes = lines[i].split('"')
            quotedStrings = []
            for j in range(0,len(lineSplitByQuotes)):
                if j%2==1:
                    quotedStrings.append(lineSplitByQuotes[j].replace(',',''))
                    lines[i] = lines[i].replace(lineSplitByQuotes[j],lineSplitByQuotes[j].replace(',',''))
                    lines[i] = lines[i].replace('"','')
            # print ("lineSplitByQuotes = %s" % lineSplitByQuotes)
            # print ("\nquotedStrings = %s\n" % quotedStrings)
            # print ("Corrected line = %s" % lines[i])
    fields = [lines[i].split(',') for i in range(firstRecord,len(lines))]
    file.close()   
    rows = []
    for row in fields:
        if len( row ) < 4:
            logger.warning( 'row had fewer than 4 fields in %s; %s', fileName, row )
        else:
            rows.append( row )
    return rows

def genXmlReport( wasGood ):
    '''preliminary version generates "fake" junit-style xml'''
    templateProlog = '''<?xml version="1.0" ?>
<testsuites>
    <testsuite tests="1" errors="0" failures="%d" name="loadtests" >
        <testcase classname="com.neocortix.loadtest" name="loadtest" time="1.0">
    '''
    templateFail = '''
        <failure message="response time too high">Assertion failed</failure>
    '''
    templateEpilog = '''
        </testcase>
    </testsuite>
</testsuites>
    '''
    if wasGood:
        return (templateProlog % 0) + templateEpilog
    else:
        return (templateProlog % 1) + templateFail + templateEpilog
def genXmlReport( wasGood ):
    '''preliminary version generates "fake" junit-style xml'''
    templateProlog = '''<?xml version="1.0" ?>
<testsuites>
    <testsuite tests="1" errors="0" failures="%d" name="loadtests" >
        <testcase classname="com.neocortix.loadtest" name="loadtest" time="1.0">
    '''
    templateFail = '''
        <failure message="response time too high">Assertion failed</failure>
    '''
    templateEpilog = '''
        </testcase>
    </testsuite>
</testsuites>
    '''
    if wasGood:
        return (templateProlog % 0) + templateEpilog
    else:
        return (templateProlog % 1) + templateFail + templateEpilog

 

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
    ap.add_argument( '--logY', type=boolArg, help='whether to use log scale on Y axis', default=False)
    ap.add_argument( '--rampStepDuration', type=float, default=60, help='duration, in seconds, of ramp step' )
    ap.add_argument( '--SLODuration', type=float, default=240, help='SLO duration, in seconds' )
    ap.add_argument( '--SLOResponseTimeMax', type=float, default=2.0, help='SLO RT threshold, in seconds' )

    args = ap.parse_args()

    logger.info( 'plotting data in directory %s', os.path.realpath(args.dataDirPath)  )

    # new arguments for SLOcomparison plot
    rampStepDurationSeconds = args.rampStepDuration
    SLODurationSeconds = args.SLODuration
    SLOResponseTimeMaxSeconds = args.SLOResponseTimeMax

    #mpl.rcParams.update({'font.size': 28})
    #mpl.rcParams['axes.linewidth'] = 2 #set the value globally
    logYWanted = args.logY
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
        print(launchedInstances[0]["device-location"])
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
                

    '''
    print("\nLocations:")
    for i in range(0,len(mappedFrameNumLocation)):
        print("%s" % mappedFrameNumLocation[i][3])
    '''
        
    print("\nReading Response Time data")    
    #determine number of files and their filenames  TestPlan_results_001.csv
    fileNames = os.listdir(outputDir)    
    # print(fileNames) 

    resultFileNames = []
    for i in range(0,len(fileNames)):
        if "TestPlan_results_" in fileNames[i] and ".csv" in fileNames[i]:
            resultFileNames.append(fileNames[i])
        else:
            subDir = os.path.join( outputDir, fileNames[i] )
            inFilePath = os.path.join( subDir, 'TestPlan_results.csv' )
            if os.path.isdir( subDir ) and os.path.isfile( inFilePath ):
                partialPath = fileNames[i] + '/TestPlan_results.csv'
                resultFileNames.append( partialPath )
    numResultFiles = len(resultFileNames)    
    # print(resultFileNames)
    # print(numResultFiles)

    # read the result .csv file to find out what labels are present
    labels = []
    for i in range(0,numResultFiles):
        inFilePath = outputDir + "/" + resultFileNames[i]
        fields = getFieldsFromFileNameCSV3(inFilePath,firstRecord=1) 
        if not fields:
            logger.info( 'no fields in %s', inFilePath )
            continue
        for j in range(0,len(fields)):
            labels.append(fields[j][2])
    reducedLabels = list(np.unique(labels))
    print("\nreducedLabels = %s \n" % reducedLabels)
    numberedReducedLabels = []
    for i in range(0,len(reducedLabels)):
        # look for two numbers followed by "_"
        conditionFound = False
        for j in range(0,len(reducedLabels[i])-2):
            if reducedLabels[i][j:j+2].isnumeric() and reducedLabels[i][j+2]=="_":
                conditionFound = True
        # if reducedLabels[i][2]=="_" and reducedLabels[i][0:2].isnumeric():
        if conditionFound:
            numberedReducedLabels.append(reducedLabels[i])
    print("numberedReducedLabels = %s \n" % numberedReducedLabels)

    # read the result .csv files
    # first try the JPetStore way, looking for fields[j][6] != "text"
    responseData = []
    for i in range(0,numResultFiles):
        inFilePath = outputDir + "/" + resultFileNames[i]
        fields = getFieldsFromFileNameCSV3(inFilePath) 
        if not fields:
            logger.info( 'no fields in %s', inFilePath )
            continue
        if 'TestPlan_results_' in resultFileNames[i] and '_merged_' not in resultFileNames[i]:
            frameNum = int(resultFileNames[i].lstrip("TestPlan_results_").rstrip(".csv"))
        elif resultFileNames[i].startswith('jmeterOut_'):
            numPart = resultFileNames[i].split('/')[0].split('_')[1]
            frameNum = int( numPart )
        else:
            # should not happen, but may help debugging
            print( 'file name not recognized', resultFileNames[i] )
            continue
        startTimes = []
        elapsedTimes = []
        labels = []
        startTimesNumberedReduced = []
        elapsedTimesNumberedReduced = []
        labelsNumberedReduced = []
        startTimesAllCodes = []
        labelsAllCodes = []
        codes = []
        threads = []
        receivedBytes = []
        sentBytes = []
        receivedBytesNumberedReduced = []
        sentBytesNumberedReduced = []
        for j in range(0,len(fields)):
            if len(fields[j]) <= 3:
                logger.info( 'fields[j]: %s from %s', fields[j], resultFileNames[i] )
            if (len(fields[j]) > 3) and (fields[j][2] in reducedLabels) and fields[j][3] == "200" and fields[j][6] != "text":
            # if (fields[j][2] == "HTTP Request" or fields[j][2] == "GetWorkload" or fields[j][2] == "GetStarttime" or fields[j][2] == "GetDistribution")  and fields[j][3] == "200":
                startTimes.append(int(fields[j][0])/1000.0)
                elapsedTimes.append(int(fields[j][1])/1000.0)         
                labels.append(fields[j][2])         
                threads.append(int(fields[j][12]))
                receivedBytes.append(int(fields[j][9]))
                sentBytes.append(int(fields[j][10]))
                if fields[j][2] in numberedReducedLabels:
                    startTimesNumberedReduced.append(int(fields[j][0])/1000.0)
                    elapsedTimesNumberedReduced.append(int(fields[j][1])/1000.0)
                    labelsNumberedReduced.append(fields[j][2])         
                    receivedBytesNumberedReduced.append(int(fields[j][9]))
                    sentBytesNumberedReduced.append(int(fields[j][10]))
            if (len(fields[j]) > 3) and (fields[j][2] in reducedLabels):
                startTimesAllCodes.append(int(fields[j][0])/1000.0)
                truncatedResponseCode = fields[j][3]
                if not truncatedResponseCode.isdigit():
                    truncatedResponseCode = 599
                codes.append(int(truncatedResponseCode))
                labelsAllCodes.append(fields[j][2])
        if startTimes:
            minStartTimeForDevice = min(startTimes)
            jIndex = -1
            for j in range (0,len(mappedFrameNumLocation)):
                if frameNum == mappedFrameNumLocation[j][0]:
                    jIndex = j
            responseData.append([frameNum,minStartTimeForDevice,startTimes,elapsedTimes,mappedFrameNumLocation[jIndex],labels,startTimesAllCodes,codes,startTimesNumberedReduced,elapsedTimesNumberedReduced,labelsNumberedReduced,labelsAllCodes,threads,receivedBytes,sentBytes,receivedBytesNumberedReduced,sentBytesNumberedReduced])

    if not responseData:

        # now try the simple way (original runBatchJMeter demos), 
        #    looking for fields[j][6] == "text"
        responseData = []
        for i in range(0,numResultFiles):
            inFilePath = outputDir + "/" + resultFileNames[i]
            fields = getFieldsFromFileNameCSV3(inFilePath) 
            if not fields:
                logger.info( 'no fields in %s', inFilePath )
                continue
            # frameNum = int(resultFileNames[i].lstrip("TestPlan_results_").rstrip(".csv"))
            if 'TestPlan_results_' in resultFileNames[i]:
                frameNum = int(resultFileNames[i].lstrip("TestPlan_results_").rstrip(".csv"))
            elif resultFileNames[i].startswith('jmeterOut_'):
                numPart = resultFileNames[i].split('/')[0].split('_')[1]
                frameNum = int( numPart )
            else:
                # should not happen, but may help debugging
                print( 'file name not recognized', resultFileNames[i] )
                continue

            startTimes = []
            elapsedTimes = []
            labels = []
            startTimesNumberedReduced = []
            elapsedTimesNumberedReduced = []
            labelsNumberedReduced = []
            startTimesAllCodes = []
            labelsAllCodes = []
            codes = []
            threads = []
            receivedBytes = []
            sentBytes = []
            receivedBytesNumberedReduced = []
            sentBytesNumberedReduced = []
            for j in range(0,len(fields)):
                if len(fields[j]) <= 3:
                    logger.info( 'fields[j]: %s from %s', fields[j], resultFileNames[i] )
                if (len(fields[j]) > 3) and (fields[j][2] in reducedLabels) and fields[j][3] == "200" and fields[j][6] == "text":
                # if (fields[j][2] == "HTTP Request" or fields[j][2] == "GetWorkload" or fields[j][2] == "GetStarttime" or fields[j][2] == "GetDistribution")  and fields[j][3] == "200":
                    startTimes.append(int(fields[j][0])/1000.0)
                    elapsedTimes.append(int(fields[j][1])/1000.0)         
                    labels.append(fields[j][2])         
                    threads.append(int(fields[j][12]))
                    receivedBytes.append(int(fields[j][9]))
                    sentBytes.append(int(fields[j][10]))
                    if fields[j][2] in numberedReducedLabels:
                        startTimesNumberedReduced.append(int(fields[j][0])/1000.0)
                        elapsedTimesNumberedReduced.append(int(fields[j][1])/1000.0)
                        labelsNumberedReduced.append(fields[j][2])         
                        receivedBytesNumberedReduced.append(int(fields[j][9]))
                        sentBytesNumberedReduced.append(int(fields[j][10]))
                if (len(fields[j]) > 3) and (fields[j][2] in reducedLabels):
                    startTimesAllCodes.append(int(fields[j][0])/1000.0)
                    truncatedResponseCode = fields[j][3]
                    if not truncatedResponseCode.isdigit():
                        truncatedResponseCode = 599
                    codes.append(int(truncatedResponseCode))
                    labelsAllCodes.append(fields[j][2])
            if startTimes:
                minStartTimeForDevice = min(startTimes)
                jIndex = -1
                for j in range (0,len(mappedFrameNumLocation)):
                    if frameNum == mappedFrameNumLocation[j][0]:
                        jIndex = j
                responseData.append([frameNum,minStartTimeForDevice,startTimes,elapsedTimes,mappedFrameNumLocation[jIndex],labels,startTimesAllCodes,codes,startTimesNumberedReduced,elapsedTimesNumberedReduced,labelsNumberedReduced,labelsAllCodes,threads,receivedBytes,sentBytes,receivedBytesNumberedReduced,sentBytesNumberedReduced])


    if not responseData:
        sys.exit( 'no plottable data was found' )

    # first, time-shift all startTimes by subtracting the minStartTime for each device
    # and compute the maxStartTime (i.e. test duration) for each device
    relativeResponseData = []
    for i in range(0,len(responseData)):
        relativeStartTimes = []
        relativeStartTimesAllCodes = []
        relativeStartTimesNumberedReduced = []
        for ii in range(0,len(responseData[i][2])):
            # difference = responseData[i][2][ii]-globalMinStartTime
            # if i==2 and ii<3700 and difference > 500:
            #     print("i = %d   ii = %d   difference = %f    data = %f" % (i,ii,difference,responseData[i][2][ii] ))
            # relativeStartTimes.append(responseData[i][2][ii]-globalMinStartTime)
            relativeStartTimes.append(responseData[i][2][ii]-responseData[i][1])
        for ii in range(0,len(responseData[i][6])):
            relativeStartTimesAllCodes.append(responseData[i][6][ii]-responseData[i][1])
        for ii in range(0,len(responseData[i][8])):
            relativeStartTimesNumberedReduced.append(responseData[i][8][ii]-responseData[i][1])
        maxStartTime = max(relativeStartTimes)
        relativeResponseData.append([responseData[i][0],relativeStartTimes,responseData[i][3],responseData[i][4],maxStartTime,responseData[i][5],relativeStartTimesAllCodes,responseData[i][7],relativeStartTimesNumberedReduced,responseData[i][9],responseData[i][10],responseData[i][11],responseData[i][12],responseData[i][13],responseData[i][14],responseData[i][15],responseData[i][16]])

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

    # compute maximum number of threads
    maxThreads = 0
    for i in range(0,len(culledRelativeResponseData)):
        maxThreads += max(culledRelativeResponseData[i][12])
    print("maxThreads = %d" % maxThreads)

    # compute differential record of threadCounts for each instance
    # then interleave the threadCount records to make the plot
    differentialThreads = [[] for i in range(0,len(culledRelativeResponseData))]
    for i in range(0,len(culledRelativeResponseData)):
        lastThreadCount = 0
        for j in range(0,len(culledRelativeResponseData[i][12])):
            if culledRelativeResponseData[i][12][j] != lastThreadCount:
                diff = culledRelativeResponseData[i][12][j] - lastThreadCount
                differentialThreads[i].append([culledRelativeResponseData[i][1][j], diff])
                lastThreadCount = culledRelativeResponseData[i][12][j] 

    differentialThreadsFlattened = flattenList(differentialThreads)
    differentialThreadsSorted = sorted(differentialThreadsFlattened,key=lambda tup: tup[0])
    differentialThreadsIntegrated = []
    lastVal = 0
    for i in range(0,len(differentialThreadsSorted)):
        newVal = lastVal + differentialThreadsSorted[i][1]
        differentialThreadsIntegrated.append([differentialThreadsSorted[i][0],newVal])
        lastVal = newVal
    differentialThreadsForPlotting = [[0,0]]
    lastVal = 0
    for i in range(0,len(differentialThreadsIntegrated)):
        differentialThreadsForPlotting.append([differentialThreadsIntegrated[i][0],lastVal])
        differentialThreadsForPlotting.append([differentialThreadsIntegrated[i][0],differentialThreadsIntegrated[i][1]])
        lastVal = differentialThreadsIntegrated[i][1]
        lastTimeVal = differentialThreadsIntegrated[i][0]
    differentialThreadsForPlotting.append([lastTimeVal,lastVal])
    differentialThreadsForPlotting.append([lastTimeVal,0])

    # print("differentialThreadsSorted = %s" % differentialThreadsSorted)
    # print("differentialThreadsIntegrated = %s" % differentialThreadsIntegrated)
    # print("differentialThreadsForPlotting = %s" % differentialThreadsForPlotting)

    # compute total receivedBytes and total sentBytes
    totalReceivedBytes = 0
    totalSentBytes = 0
    for i in range(0,len(culledRelativeResponseData)):
        totalReceivedBytes += sum(culledRelativeResponseData[i][13])
        totalSentBytes += sum(culledRelativeResponseData[i][14])
    print("totalReceivedBytes = %d" % totalReceivedBytes)
    print("totalSentBytes = %d" % totalSentBytes)

    # split out receivedBytes and sentBytes by Label
    numberBadCodes = 0
    receivedBytesByLabel = [[0,numberedReducedLabels[i]] for i in range(0,len(numberedReducedLabels))]
    sentBytesByLabel = [[0,numberedReducedLabels[i]] for i in range(0,len(numberedReducedLabels))]
    for i in range(0,len(culledRelativeResponseData)):
        for j in range(0,len(culledRelativeResponseData[i][15])):
            label = culledRelativeResponseData[i][10][j]
            if label in numberedReducedLabels:
                index = numberedReducedLabels.index(label)
                receivedBytesByLabel[index][0] += culledRelativeResponseData[i][15][j]
                sentBytesByLabel[index][0] += culledRelativeResponseData[i][16][j]

    # for i in range(0,len(numberedReducedLabels)): 
        # print("%-50s   rx = %.2f   tx=%.2f" % (numberedReducedLabels[i],receivedBytesByLabel[i][0],sentBytesByLabel[i][0]))


    # for i in range(0,3):
        # print(culledRelativeResponseData[i])

    print("Number of devices = %d" % len(relativeResponseData))
    print("Culled Number of devices = %d" %len(culledRelativeResponseData))
    culledLocations = getColumn(getColumn(culledRelativeResponseData,3),3)

    #print("\nCulled Locations:")
    #for i in range(0,len(culledLocations)):
    #    print("%s" % culledLocations[i])
        
    print("\nAnalyzing Location data")
    startRelTimesAndMSPRsUnitedStatesMuxed = []
    startRelTimesAndMSPRsRussiaMuxed = []
    startRelTimesAndMSPRsOtherMuxed = []
    startRelTimesAndMSPRsAllMuxed = []
    clipTimeInSeconds = 4.00
    # getColumn(relativeResponseData[i],6)  # relative time
    # getColumn(relativeResponseData[i],7)  # response codes
    startRelTimesAndCodesUnitedStatesMuxed = []
    startRelTimesAndCodesRussiaMuxed = []
    startRelTimesAndCodesOtherMuxed = []
    startRelTimesAndCodesAllMuxed = []

    # for i in range(0,1):
    for i in range(0,len(culledRelativeResponseData)):
        # print(culledRelativeResponseData[i][3][4])
        startRelTimesAndMSPRsAllMuxed.append([culledRelativeResponseData[i][1],culledRelativeResponseData[i][2],culledRelativeResponseData[i][5] ])
        startRelTimesAndCodesAllMuxed.append([culledRelativeResponseData[i][6],culledRelativeResponseData[i][7],culledRelativeResponseData[i][11]])
        if culledRelativeResponseData[i][3][4]=="United States" :
            startRelTimesAndMSPRsUnitedStatesMuxed.append([culledRelativeResponseData[i][1],culledRelativeResponseData[i][2],culledRelativeResponseData[i][5] ])
            startRelTimesAndCodesUnitedStatesMuxed.append([culledRelativeResponseData[i][6],culledRelativeResponseData[i][7]])
        elif culledRelativeResponseData[i][3][4]=="Russia" :     
            startRelTimesAndMSPRsRussiaMuxed.append([culledRelativeResponseData[i][1],culledRelativeResponseData[i][2],culledRelativeResponseData[i][5] ])
            startRelTimesAndCodesRussiaMuxed.append([culledRelativeResponseData[i][6],culledRelativeResponseData[i][7]])
        else:
            startRelTimesAndMSPRsOtherMuxed.append([culledRelativeResponseData[i][1],culledRelativeResponseData[i][2],culledRelativeResponseData[i][5] ])
            startRelTimesAndCodesOtherMuxed.append([culledRelativeResponseData[i][6],culledRelativeResponseData[i][7]])

    startRelTimesAndMSPRsUnitedStates = [flattenList(getColumn(startRelTimesAndMSPRsUnitedStatesMuxed,0)),flattenList(getColumn(startRelTimesAndMSPRsUnitedStatesMuxed,1)),flattenList(getColumn(startRelTimesAndMSPRsUnitedStatesMuxed,2))]
    startRelTimesAndMSPRsRussia = [flattenList(getColumn(startRelTimesAndMSPRsRussiaMuxed,0)),flattenList(getColumn(startRelTimesAndMSPRsRussiaMuxed,1)),flattenList(getColumn(startRelTimesAndMSPRsRussiaMuxed,2))]
    startRelTimesAndMSPRsOther = [flattenList(getColumn(startRelTimesAndMSPRsOtherMuxed,0)),flattenList(getColumn(startRelTimesAndMSPRsOtherMuxed,1)),flattenList(getColumn(startRelTimesAndMSPRsOtherMuxed,2))]
    startRelTimesAndMSPRsAll = [flattenList(getColumn(startRelTimesAndMSPRsAllMuxed,0)),flattenList(getColumn(startRelTimesAndMSPRsAllMuxed,1)),flattenList(getColumn(startRelTimesAndMSPRsAllMuxed,2))]
    startRelTimesAndCodesUnitedStates = [flattenList(getColumn(startRelTimesAndCodesUnitedStatesMuxed,0)),flattenList(getColumn(startRelTimesAndCodesUnitedStatesMuxed,1))]
    startRelTimesAndCodesRussia = [flattenList(getColumn(startRelTimesAndCodesRussiaMuxed,0)),flattenList(getColumn(startRelTimesAndCodesRussiaMuxed,1))]
    startRelTimesAndCodesOther = [flattenList(getColumn(startRelTimesAndCodesOtherMuxed,0)),flattenList(getColumn(startRelTimesAndCodesOtherMuxed,1))]
    startRelTimesAndCodesAll = [flattenList(getColumn(startRelTimesAndCodesAllMuxed,0)),flattenList(getColumn(startRelTimesAndCodesAllMuxed,1)),flattenList(getColumn(startRelTimesAndCodesAllMuxed,2))]

    # identify labels on all the error codes (non-200)
    numberBadCodes = 0
    badCodesByLabel = [[[],numberedReducedLabels[i]] for i in range(0,len(numberedReducedLabels))]
    for i in range(0,len(startRelTimesAndCodesAll[0])):
        if startRelTimesAndCodesAll[1][i] != 200:
            code = startRelTimesAndCodesAll[1][i]
            label = startRelTimesAndCodesAll[2][i]
            if label in numberedReducedLabels:
                # print("code = %d    label = %s"%(code, label))
                numberBadCodes += 1
                index = numberedReducedLabels.index(label)
                badCodesByLabel[index][0].append(code)
    # print("numberBadCodes = %d"%(numberBadCodes))
    # print("badCodesByLabel = %s"%(badCodesByLabel))

    # print(len(startRelTimesAndMSPRsUnitedStates[0]))
    # print(len(startRelTimesAndMSPRsRussia[0]))
    # print(len(startRelTimesAndMSPRsOther[0]))
    # print(len(startRelTimesAndMSPRsAll[0]))
    # print(len(startRelTimesAndCodesUnitedStates[0]))
    # print(len(startRelTimesAndCodesRussia[0]))
    # print(len(startRelTimesAndCodesOther[0]))

    # now split out the response data by label
    startRelTimesAndMSPRsUnitedStatesByLabel = [[[],[],reducedLabels[i]] for i in range(0,len(reducedLabels))] 
    startRelTimesAndMSPRsRussiaByLabel = [[[],[],reducedLabels[i]] for i in range(0,len(reducedLabels))] 
    startRelTimesAndMSPRsOtherByLabel = [[[],[],reducedLabels[i]] for i in range(0,len(reducedLabels))] 
    # print("\n\nstartRelTimesAndMSPRsUnitedStatesByLabel = %s\n\n" % startRelTimesAndMSPRsUnitedStatesByLabel )

    for j in range(0,len(startRelTimesAndMSPRsUnitedStates[0])):
        label = startRelTimesAndMSPRsUnitedStates[2][j]
        index = reducedLabels.index(label)
        startRelTimesAndMSPRsUnitedStatesByLabel[index][0].append(startRelTimesAndMSPRsUnitedStates[0][j])
        startRelTimesAndMSPRsUnitedStatesByLabel[index][1].append(startRelTimesAndMSPRsUnitedStates[1][j])

    for j in range(0,len(startRelTimesAndMSPRsRussia[0])):
        label = startRelTimesAndMSPRsRussia[2][j]
        index = reducedLabels.index(label)
        startRelTimesAndMSPRsRussiaByLabel[index][0].append(startRelTimesAndMSPRsRussia[0][j])
        startRelTimesAndMSPRsRussiaByLabel[index][1].append(startRelTimesAndMSPRsRussia[1][j])

    for j in range(0,len(startRelTimesAndMSPRsOther[0])):
        label = startRelTimesAndMSPRsOther[2][j]
        index = reducedLabels.index(label)
        startRelTimesAndMSPRsOtherByLabel[index][0].append(startRelTimesAndMSPRsOther[0][j])
        startRelTimesAndMSPRsOtherByLabel[index][1].append(startRelTimesAndMSPRsOther[1][j])

    if False:
        print("\n\nlen(startRelTimesAndMSPRsUnitedStates[0]) = %i\n\n" % len(startRelTimesAndMSPRsUnitedStates[0]))

        for i in range(0,len(reducedLabels)):
            print("len(startRelTimesAndMSPRsUnitedStatesByLabel[%d][0]) = %d" % (i,len(startRelTimesAndMSPRsUnitedStatesByLabel[i][0])))

    # now we want to aggregate all of the numbered Label responses
    startRelTimesAndMSPRsByNumberedLabel = [[[],[],numberedReducedLabels[i]] for i in range(0,len(numberedReducedLabels))] 

    for i in range(0,len(culledRelativeResponseData)):
        for j in range(0,len(culledRelativeResponseData[i][10])):
            label = culledRelativeResponseData[i][10][j]
            index = numberedReducedLabels.index(label)
            startRelTimesAndMSPRsByNumberedLabel[index][0].append(
                culledRelativeResponseData[i][8][j])
            startRelTimesAndMSPRsByNumberedLabel[index][1].append(
                culledRelativeResponseData[i][9][j])
                    
    # for i in range(0,2): 
        # print(startRelTimesAndMSPRsByNumberedLabel[i])                

    # now put those into time bins and compute mean values
    startRelTimesAndMSPRsByNumberedLabelBinned = [[[],[],numberedReducedLabels[i]] for i in range(0,len(numberedReducedLabels))] 
    
    binSizeSeconds = 10
    for i in range(0,len(numberedReducedLabels)):
        if len(startRelTimesAndMSPRsByNumberedLabel[i][0])>0:
            maxTimeVal = max(startRelTimesAndMSPRsByNumberedLabel[i][0])
        else:
            maxTimeVal = 0
        numBins = int(np.floor(maxTimeVal / binSizeSeconds ) + 1)
        startTimesBinned = [i*binSizeSeconds for i in range(0,numBins)]
        responseTimesBinned = [[] for i in range(0,numBins)]
        meanResponseTimesBinned = np.zeros(numBins)
        for j in range(0, len(startRelTimesAndMSPRsByNumberedLabel[i][0])):
            bin = int(np.floor(startRelTimesAndMSPRsByNumberedLabel[i][0][j]/binSizeSeconds))
            responseTimesBinned[bin].append(startRelTimesAndMSPRsByNumberedLabel[i][1][j])

        for j in range(0,numBins):
            if len(responseTimesBinned[j])>0:
                meanResponseTimesBinned[j] = np.mean(responseTimesBinned[j])    
            else:
                meanResponseTimesBinned[j] = -1

        startTimesBinnedCleaned = []
        meanResponseTimesBinnedCleaned = []
        for j in range(0,numBins):
            if meanResponseTimesBinned[j]>=0:
                startTimesBinnedCleaned.append(startTimesBinned[j])
                meanResponseTimesBinnedCleaned.append(meanResponseTimesBinned[j])

        startRelTimesAndMSPRsByNumberedLabelBinned[i][0] = startTimesBinnedCleaned
        startRelTimesAndMSPRsByNumberedLabelBinned[i][1] = meanResponseTimesBinnedCleaned
        # print("maxTimeVal = %.2f, numBins = %d, startTimesBinned = %s, meanResponseTimesBinned = %s" % (maxTimeVal,numBins,startTimesBinned,meanResponseTimesBinned))

        # print("responseTimesBinned[last] = %s\nmeanResponseTimesBinned[last] = %s" % (responseTimesBinned[len(responseTimesBinned)-1], meanResponseTimesBinnedCleaned[len(meanResponseTimesBinnedCleaned)-1]))



    print("Determining Delivered Load")
    timeBinSeconds = 10
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
    edgeColor = (colorValue*.85, colorValue*.85, colorValue*.85)

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
    plt.savefig( outputDir+'/01_WorldMap.png', bbox_inches='tight')

    plotMarkerSize = 3
    plt.figure(10, figsize=figSize1)
    plt.plot(startRelTimesAndMSPRsUnitedStates[0],startRelTimesAndMSPRsUnitedStates[1], linestyle='', color=(0.0, 0.6, 1.0),marker='o',markersize=plotMarkerSize)
    plt.plot(startRelTimesAndMSPRsRussia[0],startRelTimesAndMSPRsRussia[1], linestyle='', color=(1.0, 0.0, 0.0),marker='o',markersize=plotMarkerSize)
    plt.plot(startRelTimesAndMSPRsOther[0],startRelTimesAndMSPRsOther[1], linestyle='', color=(0.0, 1.0, 0.0),marker='o',markersize=plotMarkerSize)
    if not logYWanted:
        plt.ylim([0,clipTimeInSeconds])
    else:
        plt.ylim( [.01, 10] )
        plt.yscale( 'log' )
        ax = plt.gca()
        ax.yaxis.set_major_locator( mpl.ticker.FixedLocator([ .02, .05, .1, .2, .5, 1, 2, 5, 10]) )
        ax.yaxis.set_major_formatter(mpl.ticker.ScalarFormatter())

    plt.title("Response Times (s)\n", fontsize=42*fontFactor)
    plt.xlabel("Time during Test (s)", fontsize=32*fontFactor)  
    plt.ylabel("Response Times (s)", fontsize=32*fontFactor)  
    plt.savefig( outputDir+'/05_ResponseTimesByRegion.png', bbox_inches='tight' )
    #plt.show()    
    # plt.clf()
    # plt.close()  


    plotMarkerSize = 6
    fig = plt.figure(20, figsize=figSize1)
    #ax = plt.gca()
    #box = ax.get_position()
    #ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])

    for i in range(0,len(reducedLabels)):
        fraction = 0.5+0.5*(i+1)/len(reducedLabels)
        plt.plot(startRelTimesAndMSPRsUnitedStatesByLabel[i][0],startRelTimesAndMSPRsUnitedStatesByLabel[i][1], linestyle='', color=(0.0, 0.6*fraction, 1.0*fraction),marker='o',markersize=plotMarkerSize,label="U.S.A.--" + reducedLabels[i])
        
    for i in range(0,len(reducedLabels)):
        fraction = 0.5+0.5*(i+1)/len(reducedLabels)
        plt.plot(startRelTimesAndMSPRsRussiaByLabel[i][0],startRelTimesAndMSPRsRussiaByLabel[i][1], linestyle='', color=(1.0*fraction, 0.0, 0.0),marker='o',markersize=plotMarkerSize,label="Russia--" + reducedLabels[i])

    for i in range(0,len(reducedLabels)):
        fraction = 0.5+0.5*(i+1)/len(reducedLabels)
        plt.plot(startRelTimesAndMSPRsOtherByLabel[i][0],startRelTimesAndMSPRsOtherByLabel[i][1], linestyle='', color=(0.0, 0.9*fraction, 0.0),marker='o',markersize=plotMarkerSize,label="Other--" + reducedLabels[i])
    plt.legend(loc="center left",ncol=1,bbox_to_anchor=(1, 0.5)) 
    plt.ylim([0,clipTimeInSeconds])
    plt.title("Response Times (s)\n", fontsize=42*fontFactor)
    plt.xlabel("Time during Test (s)", fontsize=32*fontFactor)  
    plt.ylabel("Response Times (s)", fontsize=32*fontFactor)  
    plt.savefig( outputDir+'/06_ResponseTimesByRegion2.png', bbox_inches='tight' )
    #plt.show()    
    # plt.clf()
    # plt.close()  

    if len(numberedReducedLabels)>0:
        # Numbered Label plots, only for JPetStore example

        plotMarkerSize = 6
        fig = plt.figure(29, figsize=figSize1)
        #ax = plt.gca()
        #box = ax.get_position()
        #ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])

        for i in range(0,len(numberedReducedLabels)):
            plt.plot(startRelTimesAndMSPRsByNumberedLabel[i][0],startRelTimesAndMSPRsByNumberedLabel[i][1], linestyle='', marker='o',markersize=plotMarkerSize,label=numberedReducedLabels[i])
        
        plt.legend(loc="center left",ncol=1,bbox_to_anchor=(1, 0.5)) 
        plt.ylim([0,clipTimeInSeconds])
        plt.title("Response Times (s)\n", fontsize=42*fontFactor)
        plt.xlabel("Relative Time during Test (s)", fontsize=32*fontFactor)  
        plt.ylabel("Response Times (s)", fontsize=32*fontFactor)  
        plt.savefig( outputDir+'/03_ResponseTimesByLabel.png', bbox_inches='tight' )


        plotMarkerSize = 12
        plotLineWidth = 6
        fig = plt.figure(39, figsize=figSize1)
        #ax = plt.gca()
        #box = ax.get_position()
        #ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])

        for i in range(0,len(numberedReducedLabels)):
            plt.plot(startRelTimesAndMSPRsByNumberedLabelBinned[i][0],startRelTimesAndMSPRsByNumberedLabelBinned[i][1], linestyle='-', linewidth=plotLineWidth, marker='o',markersize=plotMarkerSize,label=numberedReducedLabels[i])
        
        plt.legend(loc="center left",ncol=1,bbox_to_anchor=(1, 0.5)) 
        plt.ylim([0,clipTimeInSeconds])
        plt.title("Mean Response Times (s)\n", fontsize=42*fontFactor)
        plt.xlabel("Relative Time during Test (s)", fontsize=32*fontFactor)  
        plt.ylabel("Mean Response Times (s)", fontsize=32*fontFactor)  
        plt.savefig( outputDir+'/04_ResponseTimesByLabelMean.png', bbox_inches='tight' )


    plt.figure(2, figsize=figSize1)
    plt.plot( deliveredLoadTimes, deliveredLoad, linewidth=5, color=(0.0, 0.6, 1.0) )
    plt.fill_between( deliveredLoadTimes, deliveredLoad, alpha=0.3, color=(0.0, 0.6, 1.0) )
    # makeTimelyXTicks()
    # plt.xlim([0,270])
    plt.ylim(bottom=0)  
    plt.title("Hits/Second During Test\n", fontsize=42*fontFactor)
    plt.xlabel("Time during Test (s)", fontsize=32*fontFactor)  
    plt.ylabel("Hits per second", fontsize=32*fontFactor)  
    plt.savefig( outputDir+'/02_HitsPerSecond.png', bbox_inches='tight' )
    #plt.show()

    plt.figure(297, figsize=figSize1)
    plt.plot(getColumn(differentialThreadsForPlotting,0), getColumn(differentialThreadsForPlotting,1), linewidth=5, color=(0.0, 0.6, 1.0) )
    plt.fill_between(getColumn(differentialThreadsForPlotting,0), getColumn(differentialThreadsForPlotting,1), color=(0.0, 0.6, 1.0), alpha=0.3 )
    # makeTimelyXTicks()
    # plt.xlim([0,270])
    plt.ylim(bottom=0)  
    plt.title("Active Threads During Test\n", fontsize=42*fontFactor)
    plt.xlabel("Time during Test (s)", fontsize=32*fontFactor)  
    plt.ylabel("Active Threads", fontsize=32*fontFactor)  
    plt.savefig( outputDir+'/07_ActiveThreads.png', bbox_inches='tight' )
    #plt.show()



    if (rampStepDurationSeconds>0 and SLODurationSeconds>0 and SLOResponseTimeMaxSeconds>0):
        print("\nAnalyzing data for SLO Comparison\n")
        # compute means and 95th percentiles in each rampStepDurationSeconds window 
        MaxPlotValue = 1000
        startRelTimesAllFloat = [float(startRelTimesAndMSPRsAll[0][i]) for i in range(0,len(startRelTimesAndMSPRsAll[0]))]
        maxDurationFound = max(startRelTimesAllFloat)

        numWindows = int(maxDurationFound/rampStepDurationSeconds) + 1
        ResponseTimesInWindows = []
        MeanResponseTimesInWindows = []
        PercentileResponseTimesInWindows = []
        Percentile5ResponseTimesInWindows = []
        for i in range(0,numWindows):
            ResponseTimesInWindows.append([])
            MeanResponseTimesInWindows.append(0)
            PercentileResponseTimesInWindows.append(0)
            Percentile5ResponseTimesInWindows.append(0)

        # segment the values into the windows
        for i in range(0,len(startRelTimesAndMSPRsAll[0])):
            window = int(startRelTimesAndMSPRsAll[0][i]/rampStepDurationSeconds)
            ResponseTimesInWindows[window].append(startRelTimesAndMSPRsAll[1][i])

        # compute means and percentiles within each window
        for i in range(0,numWindows):
            rtw = ResponseTimesInWindows[i]
            if rtw:
                MeanResponseTimesInWindows[i] = np.mean(ResponseTimesInWindows[i])
                PercentileResponseTimesInWindows[i] = np.percentile(ResponseTimesInWindows[i],95)
                Percentile5ResponseTimesInWindows[i] = np.percentile(ResponseTimesInWindows[i],5)
            else:
                print( 'no response times in window', i )
                MeanResponseTimesInWindows[i] = 0
                PercentileResponseTimesInWindows[i] = 0
                Percentile5ResponseTimesInWindows[i] = 0

        # check 95th percentiles against SLO for PASS/FAIL
        numSLOwindows = int(min(SLODurationSeconds,maxDurationFound)/rampStepDurationSeconds)
        SLOstatus = "PASS"
        wasGood = True
        for i in range(0,numSLOwindows):
            if PercentileResponseTimesInWindows[i]>SLOResponseTimeMaxSeconds:
                SLOstatus = "FAIL"
                wasGood = False

        # prepare arrays for plotting
        meanPlotArray = []
        percentilePlotArray = []
        percentile5PlotArray = []
        SLOPlotArray = [[0,MaxPlotValue],[0,SLOResponseTimeMaxSeconds],[min(SLODurationSeconds,maxDurationFound),SLOResponseTimeMaxSeconds],[min(SLODurationSeconds,maxDurationFound),MaxPlotValue]]
        for i in range(0,numWindows):
            meanPlotArray.append([i*rampStepDurationSeconds,MeanResponseTimesInWindows[i]])
            meanPlotArray.append([min((i+1)*rampStepDurationSeconds,maxDurationFound),MeanResponseTimesInWindows[i]])
            percentilePlotArray.append([i*rampStepDurationSeconds,PercentileResponseTimesInWindows[i]])
            percentilePlotArray.append([min((i+1)*rampStepDurationSeconds,maxDurationFound),PercentileResponseTimesInWindows[i]])
            percentile5PlotArray.append([i*rampStepDurationSeconds,Percentile5ResponseTimesInWindows[i]])
            percentile5PlotArray.append([min((i+1)*rampStepDurationSeconds,maxDurationFound),Percentile5ResponseTimesInWindows[i]])

        # plot SLO Comparison 
        plotMarkerSize = 3
        fig = plt.figure(11, figsize=figSize1)
        ax1 = fig.add_subplot()

        plt.plot(startRelTimesAndMSPRsAll[0],startRelTimesAndMSPRsAll[1], linestyle='', color=(0.0, 0.6, 1.0),marker='o',markersize=plotMarkerSize, alpha=0.03)
        plt.plot(getColumn(percentilePlotArray,0),getColumn(percentilePlotArray,1), linewidth = 5,linestyle='-', color=(1.0, 0.0, 1.0), label="95%ile")
        plt.plot(getColumn(meanPlotArray,0),getColumn(meanPlotArray,1), linewidth = 5,linestyle='-', color=(1.0, 0.7, 0.2), label="Mean")
        plt.plot(getColumn(percentile5PlotArray,0),getColumn(percentile5PlotArray,1), linewidth = 5,linestyle='-', color=(0.5, 0.0, 0.5), label="5%ile")
        if SLOstatus=="PASS":
            plt.plot(getColumn(SLOPlotArray,0),getColumn(SLOPlotArray,1), linewidth = 8,linestyle='-', color=(0.0, 0.8, 0.0))
            ax1.text(min(SLODurationSeconds,maxDurationFound)/2, clipTimeInSeconds - 0.1, 'PASS SLO', fontsize=50, fontweight='bold',color=(0.0,0.8,0.0),verticalalignment='top', horizontalalignment='center')
        else:
            plt.plot(getColumn(SLOPlotArray,0),getColumn(SLOPlotArray,1), linewidth = 8,linestyle='-', color=(1.0, 0.0, 0.0))
            ax1.text(min(SLODurationSeconds,maxDurationFound)/2, clipTimeInSeconds - 0.1, 'FAIL SLO', fontsize=50, fontweight='bold',color=(1.0,0.0,0.0),verticalalignment='top', horizontalalignment='center')
    

        if not logYWanted:
            plt.ylim([0,clipTimeInSeconds])
        else:
            plt.ylim( [.01, 10] )
            plt.yscale( 'log' )
            ax = plt.gca()
            ax.yaxis.set_major_locator( mpl.ticker.FixedLocator([ .02, .05, .1, .2, .5, 1, 2, 5, 10]) )
            ax.yaxis.set_major_formatter(mpl.ticker.ScalarFormatter())
    
        lgnd = ax1.legend(fontsize='medium',loc="upper right")
        
        plt.title("Response Times (s) - Mean, 5th, 95th Percentile, and SLO\n", fontsize=42*fontFactor)  
        plt.xlabel("Time during Test (s)", fontsize=32*fontFactor)  
        plt.ylabel("Response Times (s)", fontsize=32*fontFactor)  
        plt.savefig( outputDir+'/11_SLOcomparison.png', bbox_inches='tight' )
        #plt.show()    
        # plt.clf()
        # plt.close()  

        print("Writing SLO Comparison testResults.xml file\n")
        xmlReportFilePath = outputDir + '/testResults.xml'
        xml = genXmlReport( wasGood )
        with open( xmlReportFilePath, 'w' ) as outFile:
            outFile.write( xml )

    # From Harinder Seera's plotJMeterHist.py:

    try:
        #default histogram settings
        print("Plotting 10_histogram2.png\n")
        plt.figure(figsize=(12,8))
        kwargs = dict(histtype='step', stacked=False, alpha=0.4, fill=True, bins=250)
        plt.xlim(0,4000)
        plt.xlabel('Response Time (ms)')
        plt.ylabel('Frequency')
        plt.title("Response Histograms by Location\n", fontsize=36*fontFactor)
        plt.grid(axis="x", color="black", alpha=.8, linewidth=0.2, linestyle=":")
        plt.grid(axis="y", color="black", alpha=.8, linewidth=0.2, linestyle=":")
        
        for i in range(0,len(culledRelativeResponseData)):
            dataList = [1000*culledRelativeResponseData[i][2][j] for j in range(0,len(culledRelativeResponseData[i][2]))]
            plt.hist(dataList,**kwargs)

        plt.savefig(outputDir + '/10_Histogram2.png')
    except Exception as e:
        raise e




    # From Harinder Seera's plotJMeterMulti.py:

    try:
        print("Plotting 08_Graphs2.png\n")
        mpl.rcParams.update({'font.size': 10})
        plotMarkerSize = 1
                
        # res = df.pivot(columns='location', values='latency')
        
        fig, axes = plt.subplots(3, 2, figsize=(14, 10), sharey=False) # set 3x2 plots
        plt.setp(axes[0,0].spines.values(), linewidth=1)
        plt.setp(axes[0,1].spines.values(), linewidth=1)
        plt.setp(axes[1,0].spines.values(), linewidth=1)
        plt.setp(axes[1,1].spines.values(), linewidth=1)
        plt.setp(axes[2,0].spines.values(), linewidth=1)
        plt.setp(axes[2,1].spines.values(), linewidth=1)
        fig.patch.set_facecolor('#bbe5f9') # light blue background
        plt.subplots_adjust(hspace = 0.3)
        color = {' USA':'#0000FF',' Russia':'#FF0000',' Other':'#00FF00' }

        # first subplot:  main scatterplot
        startRelTimesAndMSPRsUnitedStatesMS = [1000*startRelTimesAndMSPRsUnitedStates[1][i] for i in range(0,len(startRelTimesAndMSPRsUnitedStates[1]))]
        startRelTimesAndMSPRsRussiaMS = [1000*startRelTimesAndMSPRsRussia[1][i] for i in range(0,len(startRelTimesAndMSPRsRussia[1]))]
        startRelTimesAndMSPRsOtherMS = [1000*startRelTimesAndMSPRsOther[1][i] for i in range(0,len(startRelTimesAndMSPRsOther[1]))]

        axes[0,0].plot(startRelTimesAndMSPRsUnitedStates[0],startRelTimesAndMSPRsUnitedStatesMS, linestyle='', color=(0.0, 0.6, 1.0),marker='o',markersize=plotMarkerSize, label="USA")
        axes[0,0].plot(startRelTimesAndMSPRsRussia[0],startRelTimesAndMSPRsRussiaMS, linestyle='', color=(1.0, 0.0, 0.0),marker='o',markersize=plotMarkerSize, label="Russia")
        axes[0,0].plot(startRelTimesAndMSPRsOther[0],startRelTimesAndMSPRsOtherMS, linestyle='', color=(0.0, 0.9, 0.0),marker='o',markersize=plotMarkerSize, label="Other")
        axes[0,0].set(ylim=(0,4000))
        xticks = axes[0,0].get_xticks()
        lgnd = axes[0,0].legend(fontsize='medium',loc="upper right")
        lgnd.legendHandles[0]._legmarker.set_markersize(8)
        lgnd.legendHandles[1]._legmarker.set_markersize(8)
        lgnd.legendHandles[2]._legmarker.set_markersize(8)

        axes[0,0].set_title('Response Time Over Time')
        axes[0,0].set_xlabel('Time (s)')
        axes[0,0].set_ylabel('Response Time (ms)')
        
        # second subplot:  generate response time distribution graph
        listUSA = [1000*startRelTimesAndMSPRsUnitedStates[1][j] for j in range(0,len(startRelTimesAndMSPRsUnitedStates[1]))]
        listRussia = [1000*startRelTimesAndMSPRsRussia[1][j] for j in range(0,len(startRelTimesAndMSPRsRussia[1]))]
        listOther = [1000*startRelTimesAndMSPRsOther[1][j] for j in range(0,len(startRelTimesAndMSPRsOther[1]))]
        axes[0,1].hist(listUSA, color=(0.0, 0.6, 1.0), alpha=0.6, bins=400, label="USA", histtype='step', fill=True, linewidth=2)
        axes[0,1].hist(listRussia, color=(1.0, 0.0, 0.0), alpha=0.6, bins=400, label="Russia",  histtype='step', fill=True, linewidth=2)
        axes[0,1].hist(listOther, color=(0.0, 0.9, 0.0), alpha=0.6, bins=400, label="Other",  histtype='step', fill=True, linewidth=2)
        axes[0,1].legend(fontsize='medium',loc="upper right")
        axes[0,1].set(xlim=(0,4000))
        axes[0,1].set_title('Response Time Distribution')
        axes[0,1].set_xlabel('Response Time (ms)')
        axes[0,1].set_ylabel('Frequency')
        
        #generate latency/response time basic statistics 
        axes[1, 0].axis("off")

        numMetrics = 10
        numRegions = 3
        dataTable = [[0,0,0] for i in range(0,numMetrics)]
        for i in range(0,numRegions):
            if i==0:  
                listToProcess = listUSA
            elif i==1:
                listToProcess = listRussia
            else:
                listToProcess = listOther
            if len(listToProcess)>0:
                dataTable[0][i] = len(listToProcess)
                dataTable[1][i] = np.round(np.mean(listToProcess),2)
                dataTable[2][i] = np.round(np.std(listToProcess),2)
                dataTable[3][i] = np.round(np.min(listToProcess),2)
                dataTable[4][i] = np.round(np.percentile(listToProcess,25),2)
                dataTable[5][i] = np.round(np.percentile(listToProcess,50),2)
                dataTable[6][i] = np.round(np.percentile(listToProcess,75),2)
                dataTable[7][i] = np.round(np.percentile(listToProcess,90),2)
                dataTable[8][i] = np.round(np.percentile(listToProcess,95),2)
                dataTable[9][i] = np.round(np.max(listToProcess),2)

        RowLabels = ["count","mean","std","min","25%","50%","75%","90%","95%","max"]
        ColLabels = ["USA","Russia","Other"]

        table_result = axes[1, 0].table(cellText=dataTable,
                  rowLabels=RowLabels,
                  colLabels=ColLabels,
                  cellLoc = 'right', rowLoc = 'center',
                  loc='center')
        table_result.auto_set_font_size(False)
        table_result.set_fontsize(9)

        #generate percentile distribution       
        numMetrics = 13
        numRegions = 3
        dataTable2 = [[0,0,0] for i in range(0,numMetrics)]
        for i in range(0,numRegions):
            if i==0:  
                listToProcess = listUSA
            elif i==1:
                listToProcess = listRussia
            else:
                listToProcess = listOther
            if len(listToProcess)>0:
                dataTable2[0][i] = np.round(np.min(listToProcess),2)
                dataTable2[1][i] = np.round(np.percentile(listToProcess,10),2)
                dataTable2[2][i] = np.round(np.percentile(listToProcess,20),2)
                dataTable2[3][i] = np.round(np.percentile(listToProcess,30),2)
                dataTable2[4][i] = np.round(np.percentile(listToProcess,40),2)
                dataTable2[5][i] = np.round(np.percentile(listToProcess,50),2)
                dataTable2[6][i] = np.round(np.percentile(listToProcess,60),2)
                dataTable2[7][i] = np.round(np.percentile(listToProcess,70),2)
                dataTable2[8][i] = np.round(np.percentile(listToProcess,80),2)
                dataTable2[9][i] = np.round(np.percentile(listToProcess,90),2)
                dataTable2[10][i] = np.round(np.percentile(listToProcess,95),2)
                dataTable2[11][i] = np.round(np.percentile(listToProcess,99),2)
                dataTable2[12][i] = np.round(np.max(listToProcess),2)

        
        RowLabels2 = ["0%","10%","20%","30%","40%","50%","60%","70%","80%","90%","95%","99%","100%"]

        axes[1,1].plot(getColumn(dataTable2,0),linestyle='-', color=(0,0.6,1), label="USA")
        axes[1,1].plot(getColumn(dataTable2,1),linestyle='-', color=(1,0,0), label="Russia")
        axes[1,1].plot(getColumn(dataTable2,2),linestyle='-', color=(0,.9,0), label="Other")
        axes[1,1].legend(fontsize='medium')
        axes[1,1].set(ylim=(0,6000))
        axes[1,1].set_title('Percentile Distribution')
        axes[1,1].set_xlabel('Percentile')
        axes[1,1].set_ylabel('Response Time (ms)')
        axes[1,1].set_xticks(np.arange(0,len(RowLabels2)))
        axes[1,1].set_xticklabels(RowLabels2)

        # Plot Response Codes by Region
        axes[2,0].plot(startRelTimesAndCodesUnitedStates[0],startRelTimesAndCodesUnitedStates[1], linestyle='', color=(0.0, 0.6, 1.0),marker='o',markersize=plotMarkerSize, label="USA")
        axes[2,0].plot(startRelTimesAndCodesRussia[0],startRelTimesAndCodesRussia[1], linestyle='', color=(1.0, 0.0, 0.0),marker='o',markersize=plotMarkerSize, label="Russia")
        axes[2,0].plot(startRelTimesAndCodesOther[0],startRelTimesAndCodesOther[1], linestyle='', color=(0.0, 0.9, 0.0),marker='o',markersize=plotMarkerSize, label="Other")
        axes[2,0].set(ylim=(0,700))
        xticks = axes[2,0].get_xticks()
        lgnd = axes[2,0].legend(fontsize='medium',loc="upper right")
        lgnd.legendHandles[0]._legmarker.set_markersize(8)
        lgnd.legendHandles[1]._legmarker.set_markersize(8)
        lgnd.legendHandles[2]._legmarker.set_markersize(8)

        axes[2,0].set_title('Response Code Over Time')
        axes[2,0].set_xlabel('Time (s)')
        axes[2,0].set_ylabel('Response Code')
        
        #generate response code % distribution barplot
        codesAll = flattenList([startRelTimesAndCodesUnitedStates[1],startRelTimesAndCodesRussia[1],startRelTimesAndCodesOther[1]])
        uniqueCodesAll, countsAll = np.unique(codesAll, return_counts = True)

        if len(startRelTimesAndCodesUnitedStates[1])==0:
            pivotedCodesUSA = [["USA", uniqueCodesAll[i], 0] for i in range(0,len(countsAll))]
        else:
            pivotedCodesUSA = [["USA", uniqueCodesAll[i], 100.0*startRelTimesAndCodesUnitedStates[1].count(uniqueCodesAll[i])/len(startRelTimesAndCodesUnitedStates[1])] for i in range(0,len(countsAll))]

        if len(startRelTimesAndCodesRussia[1])==0:
            pivotedCodesRussia = [["Russia", uniqueCodesAll[i], 0] for i in range(0,len(countsAll))]
        else:
            pivotedCodesRussia = [["Russia", uniqueCodesAll[i], 100.0*startRelTimesAndCodesRussia[1].count(uniqueCodesAll[i])/len(startRelTimesAndCodesRussia[1])] for i in range(0,len(countsAll))]

        if len(startRelTimesAndCodesOther[1])==0:
            pivotedCodesOther = [["Other", uniqueCodesAll[i], 0] for i in range(0,len(countsAll))]
        else:
            pivotedCodesOther = [["Other", uniqueCodesAll[i], 100.0*startRelTimesAndCodesOther[1].count(uniqueCodesAll[i])/len(startRelTimesAndCodesOther[1])] for i in range(0,len(countsAll))]

        X = np.arange(len(uniqueCodesAll))
        axes[2,1].barh(X, getColumn(pivotedCodesUSA,2), color = (0,.6,1),height=.25, label="USA")
        axes[2,1].barh(X + .25, getColumn(pivotedCodesRussia,2), color = (1,0,0),height=.25, label="Russia")
        axes[2,1].barh(X + .5, getColumn(pivotedCodesOther,2), color = (0,0.9,0),height=.25, label="Other")
        axes[2,1].set_ylim([0-.25,len(uniqueCodesAll)-.25])

        axes[2,1].set_yticks(np.arange(0,len(uniqueCodesAll))+.25)
        axes[2,1].set_yticklabels(uniqueCodesAll)
        axes[2,1].legend().set_title('')
        axes[2,1].set_title('Response Code - % Distribution')
        axes[2,1].set_xlabel('% Distribution')
        axes[2,1].set_ylabel('Response Code')


        fig.tight_layout(pad=2)  
        plt.savefig(outputDir + '/08_Graphs2.png',facecolor=fig.get_facecolor(), edgecolor='none')
        plt.cla()
        plt.clf()
        plt.close()
    except Exception as e:
        raise e  


    # New Multi-panel Plot

    try:
        print("Plotting 09_Graphs3.png\n")
        mpl.rcParams.update({'font.size': 10})
        plotMarkerSize = 1
                
        # res = df.pivot(columns='location', values='latency')
        
        fig, axes = plt.subplots(3, 2, figsize=(14, 10), sharey=False) # set 3x2 plots
        plt.setp(axes[0,0].spines.values(), linewidth=1)
        plt.setp(axes[0,1].spines.values(), linewidth=1)
        plt.setp(axes[1,0].spines.values(), linewidth=1)
        plt.setp(axes[1,1].spines.values(), linewidth=1)
        plt.setp(axes[2,0].spines.values(), linewidth=1)
        plt.setp(axes[2,1].spines.values(), linewidth=1)
        fig.patch.set_facecolor('#bbe5f9') # light blue background
        plt.subplots_adjust(hspace = 0.3)
        color = {' USA':'#0000FF',' Russia':'#FF0000',' Other':'#00FF00' }

        # first subplot:  delivered Load
        axes[0,0].plot( deliveredLoadTimes, deliveredLoad, linewidth=2, color=(0.0, 0.6, 1.0) )
        axes[0,0].fill_between( deliveredLoadTimes, deliveredLoad, alpha=0.3, color=(0.0, 0.6, 1.0) )
        axes[0,0].set_title("Hits/Second During Test")
        axes[0,0].set_xlabel("Time during Test (s)")  
        axes[0,0].set_ylabel("Hits per second")  
        axes[0,0].set_ylim(bottom=0)  


        # second subplot:  generate Active Threads graph
        axes[0,1].plot(getColumn(differentialThreadsForPlotting,0), getColumn(differentialThreadsForPlotting,1), linewidth=2, color=(0.0, 0.6, 1.0) )
        axes[0,1].fill_between(getColumn(differentialThreadsForPlotting,0), getColumn(differentialThreadsForPlotting,1), color=(0.0, 0.6, 1.0), alpha=0.3 )
        axes[0,1].set_ylim(bottom=0)  
        axes[0,1].set_title("Active Threads During Test")
        axes[0,1].set_xlabel("Time during Test (s)")  
        axes[0,1].set_ylabel("Active Threads")  

        if False:
            # used to be histogram in this place
            listUSA = [1000*startRelTimesAndMSPRsUnitedStates[1][j] for j in range(0,len(startRelTimesAndMSPRsUnitedStates[1]))]
            listRussia = [1000*startRelTimesAndMSPRsRussia[1][j] for j in range(0,len(startRelTimesAndMSPRsRussia[1]))]
            listOther = [1000*startRelTimesAndMSPRsOther[1][j] for j in range(0,len(startRelTimesAndMSPRsOther[1]))]
            axes[0,1].hist(listUSA, color=(0.0, 0.6, 1.0), alpha=0.6, bins=400, label="USA", histtype='step', fill=True, linewidth=2)
            axes[0,1].hist(listRussia, color=(1.0, 0.0, 0.0), alpha=0.6, bins=400, label="Russia",  histtype='step', fill=True, linewidth=2)
            axes[0,1].hist(listOther, color=(0.0, 0.9, 0.0), alpha=0.6, bins=400, label="Other",  histtype='step', fill=True, linewidth=2)
            axes[0,1].legend(fontsize='medium',loc="upper right")
            axes[0,1].set(xlim=(0,4000))
            axes[0,1].set_title('Response Time Distribution')
            axes[0,1].set_xlabel('Response Time (ms)')
            axes[0,1].set_ylabel('Frequency')
        
        # third subplot:  main scatterplot
        startRelTimesAndMSPRsUnitedStatesMS = [1000*startRelTimesAndMSPRsUnitedStates[1][i] for i in range(0,len(startRelTimesAndMSPRsUnitedStates[1]))]
        startRelTimesAndMSPRsRussiaMS = [1000*startRelTimesAndMSPRsRussia[1][i] for i in range(0,len(startRelTimesAndMSPRsRussia[1]))]
        startRelTimesAndMSPRsOtherMS = [1000*startRelTimesAndMSPRsOther[1][i] for i in range(0,len(startRelTimesAndMSPRsOther[1]))]

        axes[1,0].plot(startRelTimesAndMSPRsUnitedStates[0],startRelTimesAndMSPRsUnitedStatesMS, linestyle='', color=(0.0, 0.6, 1.0),marker='o',markersize=plotMarkerSize, label="USA")
        axes[1,0].plot(startRelTimesAndMSPRsRussia[0],startRelTimesAndMSPRsRussiaMS, linestyle='', color=(1.0, 0.0, 0.0),marker='o',markersize=plotMarkerSize, label="Russia")
        axes[1,0].plot(startRelTimesAndMSPRsOther[0],startRelTimesAndMSPRsOtherMS, linestyle='', color=(0.0, 0.9, 0.0),marker='o',markersize=plotMarkerSize, label="Other")
        axes[1,0].set(ylim=(0,4000))
        xticks = axes[1,0].get_xticks()
        lgnd = axes[1,0].legend(fontsize='medium',loc="upper right")
        lgnd.legendHandles[0]._legmarker.set_markersize(8)
        lgnd.legendHandles[1]._legmarker.set_markersize(8)
        lgnd.legendHandles[2]._legmarker.set_markersize(8)

        axes[1,0].set_title('Response Time Over Time')
        axes[1,0].set_xlabel('Time (s)')
        axes[1,0].set_ylabel('Response Time (ms)')
        

        # Plot Harinder's distributions
        kwargs = dict(histtype='step', stacked=False, alpha=0.4, fill=True, bins=250)
        axes[1,1].set_xlim(0,4000)
        axes[1,1].set_xlabel('Response Time (ms)')
        axes[1,1].set_ylabel('Frequency')
        axes[1,1].set_title("Response Histograms by Location")
        axes[1,1].grid(axis="x", color="black", alpha=.8, linewidth=0.2, linestyle=":")
        axes[1,1].grid(axis="y", color="black", alpha=.8, linewidth=0.2, linestyle=":")
        
        for i in range(0,len(culledRelativeResponseData)):
            dataList = [1000*culledRelativeResponseData[i][2][j] for j in range(0,len(culledRelativeResponseData[i][2]))]
            axes[1,1].hist(dataList,**kwargs)


        # plot SLO Comparison 
        plotMarkerSize = 1

        axes[2,0].plot(startRelTimesAndMSPRsAll[0],startRelTimesAndMSPRsAll[1], linestyle='', color=(0.0, 0.6, 1.0),marker='o',markersize=plotMarkerSize, alpha=0.03)
        axes[2,0].plot(getColumn(percentilePlotArray,0),getColumn(percentilePlotArray,1), linewidth = 3,linestyle='-', color=(1.0, 0.0, 1.0), label="95%ile")
        axes[2,0].plot(getColumn(meanPlotArray,0),getColumn(meanPlotArray,1), linewidth = 3,linestyle='-', color=(1.0, 0.7, 0.2), label="Mean")
        axes[2,0].plot(getColumn(percentile5PlotArray,0),getColumn(percentile5PlotArray,1), linewidth = 3,linestyle='-', color=(0.5, 0.0, 0.5), label="5%ile")
        if SLOstatus=="PASS":
            axes[2,0].plot(getColumn(SLOPlotArray,0),getColumn(SLOPlotArray,1), linewidth = 3,linestyle='-', color=(0.0, 0.8, 0.0))
            axes[2,0].text(min(SLODurationSeconds,maxDurationFound)/2, clipTimeInSeconds - 0.1, 'PASS SLO', fontsize=20, fontweight='bold',color=(0.0,0.8,0.0),verticalalignment='top', horizontalalignment='center')
        else:
            axes[2,0].plot(getColumn(SLOPlotArray,0),getColumn(SLOPlotArray,1), linewidth = 3,linestyle='-', color=(1.0, 0.0, 0.0))
            axes[2,0].text(min(SLODurationSeconds,maxDurationFound)/2, clipTimeInSeconds - 0.1, 'FAIL SLO', fontsize=20, fontweight='bold',color=(1.0,0.0,0.0),verticalalignment='top', horizontalalignment='center')
    

        axes[2,0].set_ylim([0,clipTimeInSeconds])

        lgnd = axes[2,0].legend(fontsize='medium',loc="upper right")
    
        axes[2,0].set_title("Response Times (s) - Mean, 5th, 95th Percentile, and SLO")  
        axes[2,0].set_xlabel("Time during Test (s)")  
        axes[2,0].set_ylabel("Response Times (s)")  

        # plot world map
        markerSizeValue = 5
        # Turn off tick labels
        axes[2,1].set_yticklabels([])
        axes[2,1].set_xticklabels([])
        axes[2,1].set_yticks([])
        axes[2,1].set_xticks([])
        # ax.set_aspect('equal')
        # for i in range(0,20) :
        colorValue = 0.95
        edgeColor = (colorValue*.85, colorValue*.85, colorValue*.85)

        for i in range(0,len(CountryData)) :
            if isinstance( CountryData[i][1], np.ndarray ):
                axes[2,1].add_artist(plt.Polygon(CountryData[i][1],edgecolor=edgeColor,
                    facecolor=(colorValue,colorValue,colorValue),aa=True))
            else :
                for j in range(0,len(CountryData[i][1])) :
                    axes[2,1].add_artist(plt.Polygon(CountryData[i][1][j],edgecolor=edgeColor,
                        facecolor=(colorValue,colorValue,colorValue),aa=True))

        axes[2,1].plot(getColumn(mappedFrameNumLocationUnitedStates,2),getColumn(mappedFrameNumLocationUnitedStates,1),linestyle='', color=(0.0, 0.5, 1.0),marker='o',markersize=markerSizeValue, markeredgewidth=.2, markeredgecolor = 'black')
        axes[2,1].plot(getColumn(mappedFrameNumLocationRussia,2),getColumn(mappedFrameNumLocationRussia,1),linestyle='', color=(1.0, 0.0, 0.0),marker='o',markersize=markerSizeValue, markeredgewidth=.2, markeredgecolor = 'black')
        axes[2,1].plot(getColumn(mappedFrameNumLocationOther,2),getColumn(mappedFrameNumLocationOther,1),linestyle='', color=(0.0, 0.9, 0.0),marker='o',markersize=markerSizeValue, markeredgewidth=.2, markeredgecolor = 'black')
        axes[2,1].set_xlim([-180,180])
        axes[2,1].set_ylim([-60,90])
        #plt.show()


        fig.tight_layout(pad=2)  
        plt.savefig(outputDir + '/09_Graphs3.png',facecolor=fig.get_facecolor(), edgecolor='none')
        plt.cla()
        plt.clf()
        plt.close()
    except Exception as e:
        raise e  



    # Generate TestResults.html

    # datetime object containing current date and time
    primaryDateString1 = datetime.now().strftime("%B %d, %Y  %H:%M:%S")

    outputFileName = outputDir + "/TestResults.html"
    copyfile("./LoadTestHeader_005.jpg", outputDir + "/LoadTestHeader_005.jpg")
    outputFile = open(outputFileName, "w",encoding='utf-8')

    print("<HTML>",file=outputFile)
    print("<HEAD>",file=outputFile)
    print("<TITLE>%s</TITLE>" % (primaryDateString1 + "_SecuritySummary.html"),file=outputFile)
    print("<META HTTP-EQUIV=\"Content-Type\" CONTENT=\"text/html; charset=iso-8859-1\">",file=outputFile)
    print("<style> table, th, td { border: 2px solid #444444; border-collapse:collapse; }\n th, td { padding: 5px; }</style>",file=outputFile) 
    print("</HEAD>",file=outputFile)
    print("<Body style=\"background-color:#eeeeee;\">",file=outputFile)
    print("<center>",file=outputFile)


    print("<TABLE style=\"border:3px solid #888888;background-color:White \"><TR><TD>",file=outputFile)

    print("<center>",file=outputFile)

    print("<img src=\"./LoadTestHeader_005.jpg\" width=1000>",file=outputFile)

    print("<TABLE style=\"border:2px solid #444444;background-color:White;font-family:'Arial';color:Black;font-size:16pt;font-weight:bold\">",file=outputFile)
    print("<TR><TD>Test Date:</TD><TD>%s</TD></TR>" % primaryDateString1,file=outputFile)
    print("<TR><TD>Number of Virtual Users:</TD><TD>%i</TD></TR>" % maxThreads,file=outputFile)
    print("<TR><TD>Number of Instances:</TD><TD>%i</TD></TR>" % len(culledRelativeResponseData),file=outputFile)
    print("<TR><TD>Maximum Hits/Second:</TD><TD>%.2f</TD></TR>" % max(deliveredLoad),file=outputFile)
    print("<TR><TD>Test Result:</TD><TD>%s</TD></TR>" % SLOstatus,file=outputFile)
    print("</TABLE>",file=outputFile)
    print("<BR><BR>",file=outputFile)

    print("<img src=\"./09_Graphs3.png\" width=900>",file=outputFile)
    print("<BR><BR><BR><BR>",file=outputFile)

    print("</center>",file=outputFile)
    print("</TD></TR></TABLE>",file=outputFile)
    print("<BR><BR>",file=outputFile)

    print("<TABLE style=\"border:3px solid #888888;background-color:White;width:1016px \"><TR><TD>",file=outputFile)

    print("<center>",file=outputFile)

    print("<BR><BR><BR>",file=outputFile)
    print("<img src=\"./08_Graphs2.png\" width=900>",file=outputFile)
    print("<BR><BR><BR>",file=outputFile)

    print("<img src=\"./01_WorldMap.png\" width=600>",file=outputFile)
    print("<BR><BR><BR>",file=outputFile)

    print("</center>",file=outputFile)
    print("</TD></TR></TABLE>",file=outputFile)

    if len(numberedReducedLabels)>0:
        # create another page for ResponseTimesByLabel plots
        print("<BR><BR>",file=outputFile)
    
        print("<TABLE style=\"border:3px solid #888888;background-color:White;width:1016px \"><TR><TD>",file=outputFile)
    
        print("<center>",file=outputFile)
    
        print("<BR><BR><BR>",file=outputFile)

        print("<img src=\"./03_ResponseTimesByLabel.png\" width=600>",file=outputFile)
        print("<BR><BR><BR>",file=outputFile)

        print("<img src=\"./04_ResponseTimesByLabelMean.png\" width=600>",file=outputFile)
        print("<BR><BR><BR>",file=outputFile)

        print("</center>",file=outputFile)
        print("</TD></TR></TABLE>",file=outputFile)

        # create another page for Response Time Table, Split By Transaction
        print("<BR><BR>",file=outputFile)
    
        print("<TABLE style=\"border:3px solid #888888;background-color:White;width:1016px \"><TR><TD>",file=outputFile)
    
        print("<center>",file=outputFile)
    
        print("<BR><BR><BR>",file=outputFile)

        # Here is the Response Time Table, Split By Transaction
        # print("<TABLE style=\"border:2px solid #444444;background-color:White;font-family:'Arial';color:Black;font-size:16pt;font-weight:bold\">",file=outputFile)
        print("<TABLE style=\"border:1px solid #888888;background-color:White;font-family:'Arial';color:Black;font-size:9pt;width:900px \">",file=outputFile)
        print("<TR>",file=outputFile)
        print("<TH style=\"background-color:#8bc0e6\">Requests</TH>",file=outputFile)
        print("<TH colspan=\"3\" style=\"background-color:#8bc0e6\">Executions</TH>",file=outputFile)
        print("<TH colspan=\"7\" style=\"background-color:#8bc0e6\">Response Times (ms)</TH>",file=outputFile)
        print("<TH style=\"background-color:#8bc0e6\">Throughput</TH>",file=outputFile)
        print("<TH colspan=\"2\" style=\"background-color:#8bc0e6\">Network (KB/sec)</TH>",file=outputFile)
        print("</TR>",file=outputFile)
        print("<TR>",file=outputFile)
        print("<TH style=\"background-color:#8bc0e6\">Label</TH>",file=outputFile)
        print("<TH style=\"background-color:#8bc0e6\"># Samples</TH>",file=outputFile)
        print("<TH style=\"background-color:#8bc0e6\">FAIL</TH>",file=outputFile)
        print("<TH style=\"background-color:#8bc0e6\">Error %</TH>",file=outputFile)
        print("<TH style=\"background-color:#8bc0e6\">Average</TH>",file=outputFile)
        print("<TH style=\"background-color:#8bc0e6\">Min</TH>",file=outputFile)
        print("<TH style=\"background-color:#8bc0e6\">Max</TH>",file=outputFile)
        print("<TH style=\"background-color:#8bc0e6\">Median</TH>",file=outputFile)
        print("<TH style=\"background-color:#8bc0e6\">90th pct</TH>",file=outputFile)
        print("<TH style=\"background-color:#8bc0e6\">95th pct</TH>",file=outputFile)
        print("<TH style=\"background-color:#8bc0e6\">99th pct</TH>",file=outputFile)
        print("<TH style=\"background-color:#8bc0e6\">Transactions/s</TH>",file=outputFile)
        print("<TH style=\"background-color:#8bc0e6\">Received</TH>",file=outputFile)
        print("<TH style=\"background-color:#8bc0e6\">Sent</TH>",file=outputFile)
        print("</TR>",file=outputFile)

        totalNumSamples = numberBadCodes # total = bad + good
        for i in range(0,len(badCodesByLabel)):
            totalNumSamples += len(startRelTimesAndMSPRsByNumberedLabel[i][0])
        totalBadCodePercentage = numberBadCodes/totalNumSamples*100.0
        # numSamples = len(startRelTimesAndMSPRsAll[0])
        # print("totalNumSamples = %d    numSamples = %d"%(totalNumSamples,numSamples))
        averageMs = 1000.0*np.mean(startRelTimesAndMSPRsAll[1])
        minMs = 1000.0*np.min(startRelTimesAndMSPRsAll[1])
        maxMs = 1000.0*np.max(startRelTimesAndMSPRsAll[1])
        medianMs = 1000.0*np.median(startRelTimesAndMSPRsAll[1])
        percentile90ms = 1000.0*np.percentile(startRelTimesAndMSPRsAll[1],90)
        percentile95ms = 1000.0*np.percentile(startRelTimesAndMSPRsAll[1],95)
        percentile99ms = 1000.0*np.percentile(startRelTimesAndMSPRsAll[1],99)
        testStartTime = np.min(startRelTimesAndMSPRsAll[0])
        testEndTime = np.max(startRelTimesAndMSPRsAll[0])
        testDuration = testEndTime - testStartTime
        transactionsPerSecond = totalNumSamples/testDuration
        # print("testDuration = %f seconds"%testDuration)
        # print("totalNumSamples = %d"%(totalNumSamples))
        # print("transactionsPerSecond = %f"%(transactionsPerSecond))
        totalReceivedKBytesPerSecond = totalReceivedBytes/testDuration/1000.0
        totalSentKBytesPerSecond = totalSentBytes/testDuration/1000.0
        # print("totalReceivedKBytesPerSecond = %f"%(totalReceivedKBytesPerSecond))
        # print("totalSentKBytesPerSecond = %f"%(totalSentKBytesPerSecond))


        print("<TR>",file=outputFile)
        print("<TD style=\"background-color:#d9d9d9;font-weight:bold\">Total</TD>",file=outputFile)
        print("<TD style=\"background-color:#d9d9d9;font-weight:bold\">%d</TD>"%totalNumSamples,file=outputFile)
        print("<TD style=\"background-color:#d9d9d9;font-weight:bold\">%d</TD>"%numberBadCodes,file=outputFile)
        print("<TD style=\"background-color:#d9d9d9;font-weight:bold\">%.2f%%</TD>"%totalBadCodePercentage,file=outputFile)
        print("<TD style=\"background-color:#d9d9d9;font-weight:bold\">%.2f</TD>"%averageMs,file=outputFile)
        print("<TD style=\"background-color:#d9d9d9;font-weight:bold\">%.2f</TD>"%minMs,file=outputFile)
        print("<TD style=\"background-color:#d9d9d9;font-weight:bold\">%.2f</TD>"%maxMs,file=outputFile)
        print("<TD style=\"background-color:#d9d9d9;font-weight:bold\">%.2f</TD>"%medianMs,file=outputFile)
        print("<TD style=\"background-color:#d9d9d9;font-weight:bold\">%.2f</TD>"%percentile90ms,file=outputFile)
        print("<TD style=\"background-color:#d9d9d9;font-weight:bold\">%.2f</TD>"%percentile95ms,file=outputFile)
        print("<TD style=\"background-color:#d9d9d9;font-weight:bold\">%.2f</TD>"%percentile99ms,file=outputFile)
        print("<TD style=\"background-color:#d9d9d9;font-weight:bold\">%.2f</TD>"%transactionsPerSecond,file=outputFile)
        print("<TD style=\"background-color:#d9d9d9;font-weight:bold\">%.2f</TD>"%totalReceivedKBytesPerSecond,file=outputFile)
        print("<TD style=\"background-color:#d9d9d9;font-weight:bold\">%.2f</TD>"%totalSentKBytesPerSecond,file=outputFile)
        print("</TR>",file=outputFile)

        
        for i in range(0,len(numberedReducedLabels)):
            if i%2==0:
                bgColorString = "#e9f2fa"
            else:
                bgColorString = "#ffffff"

            label = numberedReducedLabels[i]
            index = getColumn(startRelTimesAndMSPRsByNumberedLabel,2).index(label)
            numSamples = len(startRelTimesAndMSPRsByNumberedLabel[index][0])
            if numSamples>0:
                averageMs = 1000.0*np.mean(startRelTimesAndMSPRsByNumberedLabel[index][1])
                minMs = 1000.0*np.min(startRelTimesAndMSPRsByNumberedLabel[index][1])
                maxMs = 1000.0*np.max(startRelTimesAndMSPRsByNumberedLabel[index][1])
                medianMs = 1000.0*np.median(startRelTimesAndMSPRsByNumberedLabel[index][1])
                percentile90ms = 1000.0*np.percentile(startRelTimesAndMSPRsByNumberedLabel[index][1],90)
                percentile95ms = 1000.0*np.percentile(startRelTimesAndMSPRsByNumberedLabel[index][1],95)
                percentile99ms = 1000.0*np.percentile(startRelTimesAndMSPRsByNumberedLabel[index][1],99)
            else:
                averageMs = 0
                minMs = 0
                maxMs = 0
                medianMs = 0
                percentile90ms = 0
                percentile95ms = 0
                percentile99ms = 0

            index = getColumn(badCodesByLabel,1).index(label)
            badCodeCount = len(badCodesByLabel[index][0])
            totalNumSamples = numSamples + badCodeCount

            if numSamples>0:
                testStartTime = np.min(startRelTimesAndMSPRsByNumberedLabel[index][0])
                testEndTime = np.max(startRelTimesAndMSPRsByNumberedLabel[index][0])
                testDuration = testEndTime - testStartTime
                transactionsPerSecond = totalNumSamples/testDuration
                receivedKBytesPerSecond = receivedBytesByLabel[i][0]/testDuration/1000.0
                sentKBytesPerSecond = sentBytesByLabel[i][0]/testDuration/1000.0
            else:
                testStartTime = 0
                testEndTime = 0
                testDuration = 0
                transactionsPerSecond = 0
                receivedKBytesPerSecond = 0
                sentKBytesPerSecond = 0

            if totalNumSamples>0:
                badCodePercentage = badCodeCount/totalNumSamples*100.0
            else:
                badCodePercentage = 0

            # print("testDuration = %f seconds"%testDuration)
            # print("numSamples = %d"%(numSamples))
            # print("transactionsPerSecond = %f"%(transactionsPerSecond))

            print("<TR>",file=outputFile)
            print("<TD style=\"background-color:%s\">%s</TD>"%(bgColorString,label),file=outputFile)
            print("<TD style=\"background-color:%s\">%d</TD>"%(bgColorString,totalNumSamples),file=outputFile)
            print("<TD style=\"background-color:%s\">%d</TD>"%(bgColorString,badCodeCount),file=outputFile)
            print("<TD style=\"background-color:%s\">%.2f%%</TD>"%(bgColorString,badCodePercentage),file=outputFile)
            print("<TD style=\"background-color:%s\">%.2f</TD>"%(bgColorString,averageMs),file=outputFile)
            print("<TD style=\"background-color:%s\">%.2f</TD>"%(bgColorString,minMs),file=outputFile)
            print("<TD style=\"background-color:%s\">%.2f</TD>"%(bgColorString,maxMs),file=outputFile)
            print("<TD style=\"background-color:%s\">%.2f</TD>"%(bgColorString,medianMs),file=outputFile)
            print("<TD style=\"background-color:%s\">%.2f</TD>"%(bgColorString,percentile90ms),file=outputFile)
            print("<TD style=\"background-color:%s\">%.2f</TD>"%(bgColorString,percentile95ms),file=outputFile)
            print("<TD style=\"background-color:%s\">%.2f</TD>"%(bgColorString,percentile99ms),file=outputFile)
            print("<TD style=\"background-color:%s\">%.2f</TD>"%(bgColorString,transactionsPerSecond),file=outputFile)
            print("<TD style=\"background-color:%s\">%.2f</TD>"%(bgColorString,receivedKBytesPerSecond),file=outputFile)
            print("<TD style=\"background-color:%s\">%.2f</TD>"%(bgColorString,sentKBytesPerSecond),file=outputFile)
            print("</TR>",file=outputFile)

        print("</TABLE>",file=outputFile)
        print("<BR><BR><BR>",file=outputFile)

        print("</center>",file=outputFile)
        print("</TD></TR></TABLE>",file=outputFile)
        print("<BR><BR><BR>",file=outputFile)
    print("</center>",file=outputFile)
    print("</Body>",file=outputFile)
    print("</HTML>",file=outputFile)
    outputFile.close()  
    print("Writing Output to %s" % outputFileName)
    print("Done.")

