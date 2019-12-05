#!/usr/bin/env python3
"""
analyze and plot statistics from load test
"""


# standard library modules
import argparse
import logging
import math
import sys
# third-party modules
import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd
import json
import os
import platform
import numpy as np
import math


logger = logging.getLogger(__name__)


def getColumn(inputList,column):
    return [inputList[i][column] for i in range(0,len(inputList))]

def makeTimelyXTicks():
    # x-axis tick marks at multiples of 60 and 10
    ax = plt.gca()
    ax.xaxis.set_major_locator( mpl.ticker.MultipleLocator(60) )
    ax.xaxis.set_minor_locator( mpl.ticker.MultipleLocator(10) )

#mpl.rcParams.update({'font.size': 28})
#mpl.rcParams['axes.linewidth'] = 2 #set the value globally


if __name__ == "__main__":
    # configure logger formatting
    logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
    logDateFmt = '%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
    logging.basicConfig(format=logFmt, datefmt=logDateFmt)
    logger.setLevel(logging.INFO)
    logger.debug('the logger is configured')

    ap = argparse.ArgumentParser( description=__doc__, fromfile_prefix_chars='@', formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    ap.add_argument( '--statsFilePath', default='G:/Share/Neo/loadtest/data/2019-08-13T14/locustStats.csv', help='the path to locustStats csv file' )
    ap.add_argument( '--launchedFilePath', default='G:/Share/Neo/loadtest/data/2019-08-13T14/launched.json', help='the path to launched json file' )
    ap.add_argument( '--mapFilePath', default='WorldCountryBoundaries.csv', help='the path to launched json file' )
    ap.add_argument( '--outDirPath', default='data', help='the path to to dir for output' )
    args = ap.parse_args()
    
    statsFilePath = args.statsFilePath
    launchedFilePath = args.launchedFilePath
    outDirPath = args.outDirPath

    os.makedirs( outDirPath, exist_ok=True )
    
    figSize1 = (10,6)
    #figSize1 = None
    fontFactor = .5


    logger.info("Reading World Map data")
    #mapFileName = "C:\\neocortix\\CloudServices\\2019-08-10_LoadTestData\\WorldCountryBoundaries.csv"
    mapFileName = args.mapFilePath
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


        
    logger.info("Reading launched json file")
    #launchedFilePath = 'C:\\neocortix\\CloudServices\\2019-08-10_LoadTestData\\2019-08-11_launched.json'
    #launchedFilePath = 'data/2019-08-13T14/launched.json'
    with open( launchedFilePath, 'r') as jsonInFile:
        launched = json.load(jsonInFile)
    # print(len(launched))
    # print(launched[0])
    # print(launched[0]['instanceId'])
    # print(launched[0]['device-location']['country'])
             
    instanceIdsAndCountries = []
    for i in range(0,len(launched)):
        instanceIdsAndCountries.append([launched[i]['instanceId'],launched[i]['device-location']['country'],launched[i]['device-location']['latitude'],launched[i]['device-location']['longitude']])
    
    # for i in range(0,len(launched)):
    #     print(instanceIdsAndCountries[i]) 

    logger.info("Reading locustStats csv file")

    #rawStats = pd.read_csv( 'G:/Share/Neo/loadtest/data/locustStats_examples/locustStats_small.csv' )
    #rawStats = pd.read_csv( 'G:/Share/Neo/loadtest/data/locustStats_examples/locustStats_big.csv' )
    # rawStats = pd.read_csv( 'G:/Share/Neo/loadtest/data/locustStats_examples/locustStats_08-06.csv' )
    # rawStats = pd.read_csv( 'C:\\neocortix\\CloudServices\\2019-08-10_LoadTestData\\locustStats_08-06.csv' )
    #rawStats = pd.read_csv( 'C:\\neocortix\\CloudServices\\2019-08-10_LoadTestData\\2019-08-11_locustStats.csv' )
    rawStats = pd.read_csv( statsFilePath )
    rawStats = rawStats[rawStats.nFails == 0].reset_index()

    
    # parse calculable time values from strings
    rawStats['startPdts'] = pd.to_datetime( rawStats.dateTime )
    unixTimestamps = rawStats.startPdts.map( lambda x: x.to_pydatetime().timestamp())
    rawStats['startRelTime'] = unixTimestamps - unixTimestamps.min()
    rawStats['endRelTime'] = rawStats['startRelTime']+3

#     print(len(rawStats))
#     print(len(rawStats.mspr))
#     print(rawStats.mspr[0:10])
#     print(rawStats['startPdts'][0:10])

    logger.info("Analyzing location data")

    startRelTimesAndPhoneIDsAndMSPRsAndCountries = []
    startRelTimesAndMSPRsUnitedStates = []
    startRelTimesAndMSPRsRussia = []
    startRelTimesAndMSPRsOther = []
    startRelTimesAndMSPRsUnitedStatesLoaded = []
    startRelTimesAndMSPRsRussiaLoaded = []
    startRelTimesAndMSPRsOtherLoaded = []
    clipTimeInMs = 3000
    startRelTimeMin = 150
    startRelTimeMax = 250
    
    for i in range(0,len(rawStats)):
        worker = rawStats.worker[i][7:] 
        if worker in getColumn(instanceIdsAndCountries,0):
            index = getColumn(instanceIdsAndCountries,0).index(worker)
            country = getColumn(instanceIdsAndCountries,1)[index]
            if rawStats['startRelTime'][i] <= clipTimeInMs:
                startRelTimesAndPhoneIDsAndMSPRsAndCountries.append([rawStats['startRelTime'][i],rawStats.worker[i],rawStats.mspr[i],country])
                if country == 'United States':
                    startRelTimesAndMSPRsUnitedStates.append([rawStats['startRelTime'][i],rawStats.mspr[i]])
                    if rawStats['startRelTime'][i]>=startRelTimeMin and rawStats['startRelTime'][i]<=startRelTimeMax:
                        startRelTimesAndMSPRsUnitedStatesLoaded.append([rawStats['startRelTime'][i],rawStats.mspr[i]])                        
                elif country == 'Russia':
                    startRelTimesAndMSPRsRussia.append([rawStats['startRelTime'][i],rawStats.mspr[i]])
                    if rawStats['startRelTime'][i]>=startRelTimeMin and rawStats['startRelTime'][i]<=startRelTimeMax:
                        startRelTimesAndMSPRsRussiaLoaded.append([rawStats['startRelTime'][i],rawStats.mspr[i]])                        
                else:
                    startRelTimesAndMSPRsOther.append([rawStats['startRelTime'][i],rawStats.mspr[i]])
                    if rawStats['startRelTime'][i]>=startRelTimeMin and rawStats['startRelTime'][i]<=startRelTimeMax:
                        startRelTimesAndMSPRsOtherLoaded.append([rawStats['startRelTime'][i],rawStats.mspr[i]])                        
                
#     print(len(startRelTimesAndPhoneIDsAndMSPRsAndCountries))
#     for i in range(0,100):
#         print(startRelTimesAndPhoneIDsAndMSPRsAndCountries[i])        

#     print(len(startRelTimesAndMSPRsUnitedStates))  
#     print(len(startRelTimesAndMSPRsRussia))  
#     print(len(startRelTimesAndMSPRsOther))  

    
    
    
    # index the data by start timne, for efficient selection
    istats = rawStats.set_index( 'startRelTime', drop=False )
    istats = istats.sort_index()
    
    nrThresh = 0*10000 # threshold below which frames have too few requests
    windowLen = 6
    stepSize = 1
    endTime = math.floor( istats.startRelTime.max() )
 
    logger.info("Doing Temporal Integration")

    # temporal integration loop
    dicts=[]  # list of integrated data records
    for xx in range( windowLen, endTime, stepSize ):
        startRelTime = xx-windowLen
        subset = istats.loc[ xx-windowLen : xx ]
        nr = subset.nr.sum()
        rpsMean = nr / windowLen if nr else float('nan')
        if nr <= nrThresh:
            msprMed = float('nan')
        else:
            msprMed = subset.msprMed.median()
        if nr <= nrThresh:
            msprMean = float('nan')
        else:
            msprMean = (subset.mspr * subset.nr).sum() / subset.nr.sum()
        
        nFails = subset.nFails.sum()
        if nr <= nrThresh:
            failRate = float('nan')
        else:
            failRate = nFails / nr if nr else 0

        nw = len(subset.worker.unique() )
        upwMean = subset.nUsers.mean()
        nUsersMean = upwMean * nw

        dicts.append( {'startRelTime': xx-windowLen, 'endRelTime': xx, 'nr': nr,
            'rps': rpsMean, 'msprMed': msprMed, 'msprMean': msprMean,
            'nWorkers': nw, 'nUsersMean': nUsersMean,
            'failRate': failRate, 'upwMean': upwMean } )
    # convert to dataframe
    outDf = pd.DataFrame( dicts )
    outDf.to_csv( outDirPath+'/integratedStats.csv' )
    
    logger.info("Plotting")
   
    plt.figure()
    plt.plot( outDf.startRelTime, outDf.nWorkers )
    plt.title("# workers present" )
    plt.savefig( outDirPath+'/nWorkers.png' )

    plt.figure()
    plt.plot( outDf.startRelTime, outDf.nUsersMean )
    plt.title("# simulated users" )
    plt.savefig( outDirPath+'/simulatedUsers.png' )

    
    if False:
        # pandas-style plotting
        outDf.plot( x='startRelTime', y=['rps','msprMed', 'msprMean', 'failRate'], subplots=True )
        # x-axis tick marks
        makeTimelyXTicks()
        
        # pyplot-style plotting
        # plt.figure()
        plt.figure(3,figsize=(25,20))
        plt.plot( outDf.startRelTime, outDf.msprMean )
        plt.plot( outDf.startRelTime, outDf.msprMed )
        makeTimelyXTicks()
    
    plt.figure(figsize=figSize1)  # was figure 2
    plt.plot( outDf.startRelTime, outDf.rps, linewidth=5, color=(0.0, 0.6, 1.0) )
    makeTimelyXTicks()
    plt.xlim([0, outDf.startRelTime.max()+10])
    plt.title("Delivered Load During Test\n", fontsize=42*fontFactor)
    plt.xlabel("Time during Test (s)", fontsize=32*fontFactor)  
    plt.ylabel("Requests per second", fontsize=32*fontFactor)  
    #plt.show()    
    plt.savefig( outDirPath+'/rps.png' )

    plt.figure(figsize=figSize1)  # was figure 10
    plt.plot(getColumn(startRelTimesAndMSPRsUnitedStates,0),getColumn(startRelTimesAndMSPRsUnitedStates,1), linestyle='', color=(0.0, 0.6, 1.0),marker='o',markersize=2)
    plt.plot(getColumn(startRelTimesAndMSPRsRussia,0),getColumn(startRelTimesAndMSPRsRussia,1), linestyle='', color=(1.0, 0.0, 0.0),marker='o',markersize=2)
    plt.plot(getColumn(startRelTimesAndMSPRsOther,0),getColumn(startRelTimesAndMSPRsOther,1), linestyle='', color=(0.0, 1.0, 0.0),marker='o',markersize=2)
    plt.ylim([0,clipTimeInMs])
    plt.title("Response Times (ms)\n", fontsize=42*fontFactor)
    plt.xlabel("Time during Test (s)", fontsize=32*fontFactor)  
    plt.ylabel("Response Times (ms)", fontsize=32*fontFactor)  
    # plt.savefig(outputPath + "\\plot_benchmarkVsDPR.png",bbox_inches='tight')   
    #plt.show()    
    # plt.clf()
    # plt.close()        
    plt.savefig( outDirPath+'/msprScatter1.png' )


    if True:
        plt.figure(figsize=figSize1)  # was figure 4
        plt.hist(getColumn(startRelTimesAndMSPRsUnitedStates,1),bins=4000, density=False, facecolor=(0,0.2,1), alpha=0.75)
        plt.hist(getColumn(startRelTimesAndMSPRsRussia,1),bins=4000, density=False, facecolor=(1.0,0.0,0), alpha=0.75)
        plt.hist(getColumn(startRelTimesAndMSPRsOther,1),bins=4000, density=False, facecolor=(0,1.0,0), alpha=0.75)
        plt.xlim([0,clipTimeInMs])
        plt.title("Duration Histograms (Overall)\n", fontsize=32*fontFactor)    
        plt.xlabel("Response Time (ms)", fontsize=32*fontFactor)  
        plt.ylabel("Number of Occurrences", fontsize=32*fontFactor) 
        props={'size': 32*fontFactor}
        plt.gca().legend(["United States","Russia","Other"],loc='center right', prop=props, bbox_to_anchor=(1, 0.50))
        # plt.savefig(outputPath + "\\plot_durationHistogram.png",bbox_inches='tight')
        #plt.show() 
        # plt.clf()
        # plt.close()    
        plt.savefig( outDirPath+'/durationHistogram.png' )

    
    plt.figure(figsize=figSize1)  # was figure 5
    plt.hist(getColumn(startRelTimesAndMSPRsUnitedStatesLoaded,1),bins=4000, density=False, facecolor=(0,0.2,1), alpha=0.75)
    plt.hist(getColumn(startRelTimesAndMSPRsRussiaLoaded,1),bins=4000, density=False, facecolor=(1.0,0.0,0), alpha=0.75)
    plt.hist(getColumn(startRelTimesAndMSPRsOtherLoaded,1),bins=4000, density=False, facecolor=(0,1.0,0), alpha=0.75)
    plt.xlim([0,clipTimeInMs])
    plt.title("Duration Histograms (Loaded)\n", fontsize=32*fontFactor)    
    plt.xlabel("Response Time (ms)", fontsize=32*fontFactor)  
    plt.ylabel("Number of Occurrences", fontsize=32*fontFactor)  
    props={'size': 32*fontFactor}
    plt.gca().legend(["United States","Russia","Other"],loc='center right', prop=props, bbox_to_anchor=(1, 0.50))
    # plt.savefig(outputPath + "\\plot_durationHistogram.png",bbox_inches='tight')
    #plt.show() 
    # plt.clf()
    # plt.close()  
    plt.savefig( outDirPath+'/durationHistogramLoaded.png' )

    if True :
   
        # plot filled boundaries
        fig = plt.figure(figsize=figSize1)  # was figure 3
        ax = fig.gca()
        # Turn off tick labels
        ax.set_yticklabels([])
        ax.set_xticklabels([])
        # ax.set_aspect('equal')
        # for i in range(0,20) :
        colorValue = 0.85
        for i in range(0,len(CountryData)) :
            if len(np.shape(CountryData[i][1]))==2 :
                # plt.plot(np.transpose(CountryData[i][1])[0],np.transpose(CountryData[i][1])[1])
                ax.add_artist(plt.Polygon(CountryData[i][1],edgecolor='None', facecolor=(colorValue,colorValue,colorValue),aa=True))           
        
            else :      
                # print("%s       %s" % (CountryData[i][0],np.shape(CountryData[i][1])[0]))
                for j in range(0,np.shape(CountryData[i][1])[0]) :
                    # print("%s" % CountryData[i][1][j])
                    # plt.plot(np.transpose(CountryData[i][1][j])[0],np.transpose(CountryData[i][1][j])[1])        
                    ax.add_artist(plt.Polygon(CountryData[i][1][j],edgecolor='None', facecolor=(colorValue,colorValue,colorValue),aa=True))           
        plt.plot(getColumn(instanceIdsAndCountries,3),getColumn(instanceIdsAndCountries,2),linestyle='', color=(0.0, 0.5, 1.0),marker='o',markersize=15*fontFactor)
        plt.xlim([-180,180])
        plt.ylim([-60,90])
        #plt.show()    
        plt.savefig( outDirPath+'/countryData.png' )

    
    logger.info("finished")
