#!/usr/bin/env python3
"""
plots all available loadtest results produced by runBatchJMeter
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
from mpl_toolkits.mplot3d import Axes3D
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
            try:
                decoded = json.loads( line )
                # print( 'decoded', decoded ) # just for debugging, would be verbose
                # iid = decoded.get( 'instanceId', '<unknown>')
                if 'args' in decoded:
                    # print( decoded['args'] )
                    if 'state' in decoded['args']:
                        if decoded['args']['state'] == 'retrieved':
                            # print("%s  %s" % (decoded['args']['frameNum'],decoded['instanceId']))
                            instanceList.append([decoded['args']['frameNum'],decoded['instanceId']])
            except Exception as exc:
                logger.warning( 'exception decoding results (%s) %s', type(exc), exc )
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
    ap.add_argument( '--dataDirPath', help='the path to directory for input and output data',
        default ='./data/' )
    ap.add_argument( '--dataDirKey', help='a substring for selecting subdirectories from data dir',
        default = '_20' )
    ap.add_argument( '--logY', type=boolArg, help='whether to use log scale on Y axis', default=False)
    ap.add_argument( '--rampStepDuration', type=float, default=60, help='duration, in seconds, of ramp step' )
    ap.add_argument( '--SLODuration', type=float, default=240, help='SLO duration, in seconds' )
    ap.add_argument( '--SLOResponseTimeMax', type=float, default=1.5, help='SLO RT threshold, in seconds' )

    args = ap.parse_args()

    # parse data directory, determine number of data directories
    dataDir = args.dataDirPath
    dataDirKey = args.dataDirKey
    dataDirNames = os.listdir(dataDir)
    #qualifiedDataDirNames = np.sort([dataDirNames[i] for i in range(0,len(dataDirNames)) if "_20" in dataDirNames[i] ])  # catches names like "jmeter_2021-04-10_050846" and "petstore_2021-06-03_003033", will work until the year 2100
    qualifiedDataDirNames = [dirName for dirName in dataDirNames if dataDirKey in dirName]
    qualifiedDataDirNames = sorted( qualifiedDataDirNames )
    print( len(qualifiedDataDirNames), 'seleted data dirs:', qualifiedDataDirNames)

    print("")
    print("found %i data directories\n" %len(qualifiedDataDirNames))
    # print("%s" %qualifiedDataDirNames)

    # Create labels for training data
    trainingLabels = []
    for i in range(0,len(qualifiedDataDirNames)):
        if i==4 or i==5:
            trainingLabels.append("FAIL")
        elif i==6:
            trainingLabels.append("CHECK")
        else:
            trainingLabels.append("PASS")
    # print("%s" % trainingLabels)


    # new arguments for SLOcomparison plot
    rampStepDurationSeconds = args.rampStepDuration
    SLODurationSeconds = args.SLODuration
    SLOResponseTimeMaxSeconds = args.SLOResponseTimeMax

    #mpl.rcParams.update({'font.size': 28})
    #mpl.rcParams['axes.linewidth'] = 2 #set the value globally
    logYWanted = args.logY

    startingTime = 0
    spacingTime = 125
    regressionData = []
    for iii in range(0,len(qualifiedDataDirNames)):
        print(qualifiedDataDirNames[iii])
        outputDir = dataDir + qualifiedDataDirNames[iii]
        launchedJsonFilePath = outputDir + "/recruitLaunched.json"
        # print("launchedJsonFilePath = %s" % launchedJsonFilePath)

        jlogFilePath = outputDir + "/batchRunner_results.jlog"
        # print("jlogFilePath = %s" % jlogFilePath)

        if not os.path.isfile( launchedJsonFilePath ):
            logger.warning( 'file not found: %s', launchedJsonFilePath )
            continue

        launchedInstances = []
        with open( launchedJsonFilePath, 'r') as jsonInFile:
            try:
                launchedInstances = json.load(jsonInFile)  # an array
            except Exception as exc:
                print("    could not load json (%s) %s" % (type(exc), exc) )
                # sys.exit( 'could not load json (%s) %s' % (type(exc), exc) )
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
        print("    Number of Completed Jobs = %i" % len(completedJobs))

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
        print("    Number of Result Files   = %i" % numResultFiles)


        # read the result .csv file to find out what labels are present
        labels = []
        for i in range(0,numResultFiles):
            inFilePath = outputDir + "/" + resultFileNames[i]
            fields = getFieldsFromFileNameCSV3(inFilePath,firstRecord=1) 
            if not fields:
                # logger.info( 'no fields in %s', inFilePath )
                continue
            for j in range(0,len(fields)):
                labels.append(fields[j][2])
        reducedLabels = list(np.unique(labels))
        print("    reducedLabels = %s" % reducedLabels)

        # read the result .csv files
        responseData = []
        for i in range(0,numResultFiles):
            inFilePath = outputDir + "/" + resultFileNames[i]
            fields = getFieldsFromFileNameCSV3(inFilePath) 
            if not fields:
                # logger.info( 'no fields in %s', inFilePath )
                continue
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
            startTimesAllCodes = []
            codes = []
            for j in range(0,len(fields)):
                if len(fields[j]) <= 3:
                    logger.info( 'fields[j]: %s from %s', fields[j], resultFileNames[i] )
                if (len(fields[j]) > 3) and (fields[j][2] in reducedLabels) and fields[j][3] == "200":
                # if (fields[j][2] == "HTTP Request" or fields[j][2] == "GetWorkload" or fields[j][2] == "GetStarttime" or fields[j][2] == "GetDistribution")  and fields[j][3] == "200":
                    startTimes.append(int(fields[j][0])/1000.0)
                    elapsedTimes.append(int(fields[j][1])/1000.0)         
                    labels.append(fields[j][2])         
                if (len(fields[j]) > 3) and (fields[j][2] in reducedLabels):
                    startTimesAllCodes.append(int(fields[j][0])/1000.0)
                    truncatedResponseCode = fields[j][3]
                    if not truncatedResponseCode.isdigit():
                        truncatedResponseCode = 599
                    codes.append(int(truncatedResponseCode))
            if startTimes:
                minStartTimeForDevice = min(startTimes)
                jIndex = -1
                for j in range (0,len(mappedFrameNumLocation)):
                    if frameNum == mappedFrameNumLocation[j][0]:
                        jIndex = j
                responseData.append([frameNum,minStartTimeForDevice,startTimes,elapsedTimes,mappedFrameNumLocation[jIndex],labels,startTimesAllCodes,codes])
        if not responseData:
            print("    no plottable data was found\n")
            # sys.exit( 'no plottable data was found' )

        # first, time-shift all startTimes by subtracting the minStartTime for each device
        # and compute the maxStartTime (i.e. test duration) for each device
        relativeResponseData = []
        for i in range(0,len(responseData)):
            relativeStartTimes = []
            relativeStartTimesAllCodes = []
            for ii in range(0,len(responseData[i][2])):
                # difference = responseData[i][2][ii]-globalMinStartTime
                # if i==2 and ii<3700 and difference > 500:
                #     print("i = %d   ii = %d   difference = %f    data = %f" % (i,ii,difference,responseData[i][2][ii] ))
                # relativeStartTimes.append(responseData[i][2][ii]-globalMinStartTime)
                relativeStartTimes.append(responseData[i][2][ii]-responseData[i][1])
            for ii in range(0,len(responseData[i][6])):
                relativeStartTimesAllCodes.append(responseData[i][6][ii]-responseData[i][1])
            maxStartTime = max(relativeStartTimes)
            relativeResponseData.append([responseData[i][0],relativeStartTimes,responseData[i][3],responseData[i][4],maxStartTime,responseData[i][5],relativeStartTimesAllCodes,responseData[i][7]])
    
        # compute median maxStartTime
        if len(getColumn(relativeResponseData,4))>0:
            medianMaxStartTime = np.median(getColumn(relativeResponseData,4))
            duration = max(getColumn(relativeResponseData,4))
        else:   
            medianMaxStartTime = 0
            duration = 0
        print("    medianMaxStartTime = %f" % medianMaxStartTime)
        print("    duration = %f" % duration)
        print("    startingTime = %f" % startingTime)
    
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
    
        print("    Number of devices = %d" % len(relativeResponseData))
        print("    Culled Number of devices = %d" %len(culledRelativeResponseData))
        culledLocations = getColumn(getColumn(culledRelativeResponseData,3),3)

    
        #print("\nCulled Locations:")
        #for i in range(0,len(culledLocations)):
        #    print("%s" % culledLocations[i])

        print("    Analyzing Location data")
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

        for i in range(0,len(culledRelativeResponseData)):
            # print(culledRelativeResponseData[i][3][4])
            startRelTimesAndMSPRsAllMuxed.append([culledRelativeResponseData[i][1],culledRelativeResponseData[i][2],culledRelativeResponseData[i][5] ])
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
    

        print("    Determining Delivered Load")
        timeBinSeconds = 5
        culledRequestTimes = []
        for i in range(0,len(culledRelativeResponseData)):
            # print("min, max = %f  %f" % (min(culledRelativeResponseData[i][1]),max(culledRelativeResponseData[i][1])))
            culledRequestTimes.append(culledRelativeResponseData[i][1])
    
        flattenedCulledRequestTimes = flattenList(culledRequestTimes)
        if len(flattenedCulledRequestTimes)>0:
            maxCulledRequestTimes = max(flattenedCulledRequestTimes)
        else:
            maxCulledRequestTimes = 0
        print("    Number of Responses = %d" %len(flattenedCulledRequestTimes))
        print("    Max Culled Request Time = %.2f" % maxCulledRequestTimes)
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

        figSize1 = (19.2, 10.8)
        fontFactor = 0.75
        mpl.rcParams.update({'font.size': 22})
        mpl.rcParams['axes.linewidth'] = 2 #set the value globally
        markerSizeValue = 10

        if (rampStepDurationSeconds>0 and SLODurationSeconds>0 and SLOResponseTimeMaxSeconds>0):
            print("    Analyzing data for SLO Comparison")
            # compute means and 95th percentiles in each rampStepDurationSeconds window 
            MaxPlotValue = 1000
            startRelTimesAllFloat = [float(startRelTimesAndMSPRsAll[0][i]) for i in range(0,len(startRelTimesAndMSPRsAll[0]))]
            if len(startRelTimesAllFloat)>0:
                maxDurationFound = max(startRelTimesAllFloat)
            else:
                maxDurationFound = 0

    
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

            dropInitialInterval = 5 # drop samples in first 5 seconds
            # segment the values into the windows
            for i in range(0,len(startRelTimesAndMSPRsAll[0])):
                window = int(startRelTimesAndMSPRsAll[0][i]/rampStepDurationSeconds)
                if startRelTimesAndMSPRsAll[0][i]>dropInitialInterval:
                    ResponseTimesInWindows[window].append(startRelTimesAndMSPRsAll[1][i])

            # compute means and percentiles within each window
            for i in range(0,numWindows):
                if len(ResponseTimesInWindows[i])>0:
                    MeanResponseTimesInWindows[i] = np.mean(ResponseTimesInWindows[i])
                    PercentileResponseTimesInWindows[i] = np.percentile(ResponseTimesInWindows[i],95)
                    Percentile5ResponseTimesInWindows[i] = np.percentile(ResponseTimesInWindows[i],5)
                else:
                    MeanResponseTimesInWindows[i] = 0
                    PercentileResponseTimesInWindows[i] = 0
                    Percentile5ResponseTimesInWindows[i] = 0

            # compute mean and percentiles for the whole data set
            if len(startRelTimesAndMSPRsAll[0])>0:
                GlobalMeanResponseTime = np.mean(startRelTimesAndMSPRsAll[1])
                GlobalPercentileResponseTime = np.percentile(startRelTimesAndMSPRsAll[1],95)
                GlobalPercentile5ResponseTime = np.percentile(startRelTimesAndMSPRsAll[1],5)
            else:
                GlobalMeanResponseTime = 0
                GlobalPercentileResponseTime = 0
                GlobalPercentile5ResponseTime = 0
    
            print("    GlobalPercentileResponseTime  = %f" % GlobalPercentileResponseTime)
            print("    GlobalMeanResponseTime        = %f" % GlobalMeanResponseTime)
            print("    GlobalPercentile5ResponseTime = %f" % GlobalPercentile5ResponseTime)
            print("")

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
            SLOPlotArray = [[0,SLOResponseTimeMaxSeconds],[min(SLODurationSeconds,maxDurationFound),SLOResponseTimeMaxSeconds]]
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
     
            # lgnd = ax1.legend(fontsize='medium',loc="upper right", bbox_to_anchor=(0.5,0.5))
            lgnd = ax1.legend(fontsize='medium',bbox_to_anchor=(0.5,0.5))
         
            plt.title("Response Times (s) - Mean, 5th, 95th Percentile, and SLO\n", fontsize=42*fontFactor)  
            plt.xlabel("Time during Test (s)", fontsize=32*fontFactor)  
            plt.ylabel("Response Times (s)", fontsize=32*fontFactor)  

            # plt.savefig( outputDir+'/SLOcomparison.png', bbox_inches='tight' )

            # plt.show()    
            # plt.clf()
            # plt.close()  

            plt.cla()
            plt.clf()
            plt.close()


        shiftedStartTimes = [(startRelTimesAndMSPRsAll[0][i] + startingTime) for i in range(0,len(startRelTimesAndMSPRsAll[0]))]

        regressionData.append([startingTime, duration, shiftedStartTimes, startRelTimesAndMSPRsAll[1],percentilePlotArray,meanPlotArray,percentile5PlotArray,SLOPlotArray,SLOstatus,GlobalMeanResponseTime, GlobalPercentileResponseTime, GlobalPercentile5ResponseTime])
        startingTime += duration + spacingTime

    print("Computing Regression Results")
    print("")

    relativeDifferenceVectors = [[0,0,0,0,"PASS"]]
    lastNonFailIndex = 0
    for i in range(1,len(regressionData)):
        deltaP95 = 0
        if regressionData[lastNonFailIndex][10]>0:
            deltaP95 = (regressionData[i][10]/regressionData[lastNonFailIndex][10]) - 1.0
        deltaMean = 0
        if regressionData[lastNonFailIndex][9]>0:
            deltaMean = (regressionData[i][9]/regressionData[lastNonFailIndex][9]) - 1.0
        deltaP5 = 0
        if regressionData[lastNonFailIndex][11]>0:
            deltaP5 = (regressionData[i][11]/regressionData[lastNonFailIndex][11]) - 1.0
        meanDelta = (deltaMean + deltaP95 + deltaP5)/3.0 # simple flattening to 1D

        if meanDelta > 0.1:
            classification = "FAIL"
        elif meanDelta < -0.1:
            classification = "CHECK"
        else:
            classification = "PASS"

        relativeDifferenceVectors.append([deltaMean, deltaP95, deltaP5,meanDelta, classification])

        # if trainingLabels[i]=="PASS" or trainingLabels[i]=="CHECK":  # training
        if classification=="PASS" or classification=="CHECK":  # production
            lastNonFailIndex = i

    for i in range(0,len(regressionData)):
        print("    %2i  %6.3f  %6.3f  %6.3f   %6.3f  %-6s    %-6s" % (i,relativeDifferenceVectors[i][0],relativeDifferenceVectors[i][1],relativeDifferenceVectors[i][2],relativeDifferenceVectors[i][3],relativeDifferenceVectors[i][4],trainingLabels[i]))
    print("")

    if (len(regressionData))==0:
        print("No Data Found, Exiting.")
        exit()  # temporary

    print("Plotting Regression Results")
    print("")

    plotMarkerSize = 3
    fig = plt.figure(11, figsize=figSize1)
    ax1 = fig.add_subplot()
    
    #  for iii in range(0,1):
    for iii in range(0,len(regressionData)):
        # print("%i   %i" %(len(regressionData[iii][1]),len(regressionData[iii][2])))
                    
        plt.plot(regressionData[iii][2],regressionData[iii][3], linestyle='', color=(0.0, 0.6, 1.0),marker='o',markersize=plotMarkerSize, alpha=0.03)

        # print(getColumn(regressionData[iii][4],0))
        # print(getColumn(regressionData[iii][4],1))
        shiftedXVals = [getColumn(regressionData[iii][4],0)[i]+regressionData[iii][0] for i in range(0,len(regressionData[iii][4]))]

        # only put label on first mini-plot to avoid redundant legend
        if iii==0:
            plt.plot(shiftedXVals,getColumn(regressionData[iii][4],1), linewidth = 5,linestyle='-', color=(1.0, 0.0, 1.0), label="95%ile")
            plt.plot(shiftedXVals,getColumn(regressionData[iii][5],1), linewidth = 5,linestyle='-', color=(1.0, 0.7, 0.2), label="Mean")
            plt.plot(shiftedXVals,getColumn(regressionData[iii][6],1), linewidth = 5,linestyle='-', color=(0.5, 0.0, 0.5), label="5%ile")
        else:
            plt.plot(shiftedXVals,getColumn(regressionData[iii][4],1), linewidth = 5,linestyle='-', color=(1.0, 0.0, 1.0))
            plt.plot(shiftedXVals,getColumn(regressionData[iii][5],1), linewidth = 5,linestyle='-', color=(1.0, 0.7, 0.2))
            plt.plot(shiftedXVals,getColumn(regressionData[iii][6],1), linewidth = 5,linestyle='-', color=(0.5, 0.0, 0.5))

        shiftedXVals = [getColumn(regressionData[iii][7],0)[i]+regressionData[iii][0] for i in range(0,len(regressionData[iii][7]))]
        if regressionData[iii][8]=="PASS":
            plt.plot(shiftedXVals,getColumn(regressionData[iii][7],1), linewidth = 8,linestyle='-', color=(0.0, 0.8, 0.0))
            ax1.text(regressionData[iii][0], clipTimeInSeconds - 0.2, 'PASS SLO', fontsize=14, fontweight='bold',color=(0.0,0.8,0.0),verticalalignment='top', horizontalalignment='left')
        else:
            plt.plot(shiftedXVals,getColumn(regressionData[iii][7],1), linewidth = 8,linestyle='-', color=(1.0, 0.0, 0.0))
            ax1.text(regressionData[iii][0], clipTimeInSeconds - 0.2, 'FAIL SLO', fontsize=14, fontweight='bold',color=(1.0,0.0,0.0),verticalalignment='top', horizontalalignment='left')
     
        plotTrainingLabels = False
        if plotTrainingLabels:
            if trainingLabels[iii]=="PASS":
                ax1.text(regressionData[iii][0], clipTimeInSeconds - 0.07, 'PASS', fontsize=18, fontweight='bold',color=(0.0,0.8,0.0),verticalalignment='top', horizontalalignment='left')
            elif trainingLabels[iii]=="CHECK":
                ax1.text(regressionData[iii][0], clipTimeInSeconds - 0.07, 'CHECK', fontsize=18, fontweight='bold',color=(1.0,0.75,0.0),verticalalignment='top', horizontalalignment='left')
            else:
                ax1.text(regressionData[iii][0], clipTimeInSeconds - 0.07, 'FAIL', fontsize=18, fontweight='bold',color=(1.0,0.0,0.0),verticalalignment='top', horizontalalignment='left')
        else: 
            # plot classification results
            if relativeDifferenceVectors[iii][4]=="PASS":
                ax1.text(regressionData[iii][0], clipTimeInSeconds - 0.07, 'PASS', fontsize=18, fontweight='bold',color=(0.0,0.8,0.0),verticalalignment='top', horizontalalignment='left')
            elif relativeDifferenceVectors[iii][4]=="CHECK":
                ax1.text(regressionData[iii][0], clipTimeInSeconds - 0.07, 'CHECK', fontsize=18, fontweight='bold',color=(1.0,0.75,0.0),verticalalignment='top', horizontalalignment='left')
            else:
                ax1.text(regressionData[iii][0], clipTimeInSeconds - 0.07, 'FAIL', fontsize=18, fontweight='bold',color=(1.0,0.0,0.0),verticalalignment='top', horizontalalignment='left')

    plt.ylim([0,clipTimeInSeconds])
    ax1.set_xticks([getColumn(regressionData,0)[i]+getColumn(regressionData,1)[i]/2 for i in range(0,len(regressionData))])

    dateStrings = []
    for i in range(0,len(qualifiedDataDirNames)):
        # extract the date part of the name, like "2021-06-11"
        newString = qualifiedDataDirNames[i].split("_")[1][0:10]
        dateStrings.append(newString)

    ax1.set_xticklabels(dateStrings, rotation=-90)
    plt.title("Daily Regression Test Results and SLO Comparisons\n", fontsize=36*fontFactor)
    plt.xlabel('Date of Test')
    plt.ylabel('Response Time (s)')

    # lgnd = ax1.legend(fontsize='medium',loc="upper right")
    lgnd = ax1.legend(fontsize='medium',loc="upper right", bbox_to_anchor=(1.0,0.93))

    plt.savefig( dataDir+'/regressionTest.png', bbox_inches='tight' )

    plt.cla()
    plt.clf()
    plt.close()

    if False:

        # plot classification boundaries and all data 3D data points

        fig = plt.figure(911, figsize=figSize1)
        ax = fig.add_subplot(111, projection='3d')

        markerSize3D = 300

        for i in range(0,len(relativeDifferenceVectors)):
            if relativeDifferenceVectors[i][4]=="PASS":
                ax.scatter([relativeDifferenceVectors[i][2]], [relativeDifferenceVectors[i][0]], [relativeDifferenceVectors[i][1]], color=(0.0,0.8,0.0), marker='o',s=markerSize3D,edgecolors='black')
            elif relativeDifferenceVectors[i][4]=="CHECK":
                ax.scatter([relativeDifferenceVectors[i][2]], [relativeDifferenceVectors[i][0]], [relativeDifferenceVectors[i][1]], color=(1.0,0.75,0.0), marker='o',s=markerSize3D,edgecolors='black')
            else:
                ax.scatter([relativeDifferenceVectors[i][2]], [relativeDifferenceVectors[i][0]], [relativeDifferenceVectors[i][1]], color=(1.0,0.0,0.0), marker='o',s=markerSize3D,edgecolors='black')
    
        # plot the classifier boundaries
        # a plane is a*x+b*y+c*z+d=0
        # [a,b,c] is the normal. Thus, we have to calculate
        # d and we're set
    
        # we want these two planes:  (x+y+z)/3 = 0.1   and (x+y+z)/3 = -0.1.
    
        # create x,y
        xx, yy = np.meshgrid(np.arange(-0.4, 0.4, 0.1), np.arange(-0.4, 0.4, 0.1))
    
        # calculate corresponding z
        z1 = (-xx -yy - 0.3) 
        z2 = (-xx -yy + 0.3) 
        ax.plot_surface(xx, yy, z1, alpha=0.2,linewidth=0.5, edgecolors='black')
        ax.plot_surface(xx, yy, z2, alpha=0.2,linewidth=0.5, edgecolors='black')
    
        ax.set_xlabel('Delta 5th %ile', fontsize=15)
        ax.set_ylabel('Delta Mean',     fontsize=15)
        ax.set_zlabel('Delta 95th %ile',fontsize=15)
    
        ax.xaxis.set_tick_params(labelsize=10)
        ax.yaxis.set_tick_params(labelsize=10)
        ax.zaxis.set_tick_params(labelsize=10)
    
        ax.axes.set_xlim3d(left=-0.5, right=0.5)
        ax.axes.set_ylim3d(bottom=-0.5, top=0.5)
        ax.axes.set_zlim3d(bottom=-0.5, top=0.5)
    
        plt.savefig( dataDir+'/regressionPoints.png', bbox_inches='tight' )
        ax.view_init(elev=10., azim=45)
        for ii in range(2990,3110,1):
            ax.view_init(elev=10., azim=ii/10+0.5)
            # plt.savefig( dataDir+'/regressionPoints%04d.png'%ii, bbox_inches='tight' )
            plt.savefig( dataDir+'/regressionPoints%04d.png'%ii)
            # savefig("movie%d.png" % ii)
    
        plt.cla()
        plt.clf()
        plt.close()

        exit()  # temporary


