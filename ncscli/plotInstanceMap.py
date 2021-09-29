#!/usr/bin/env python3
"""
plots locations of NCS instances on a world map
"""
# standard library modules
import argparse
import csv
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

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def getColumn(inputList,column):
    return [inputList[i][column] for i in range(0,len(inputList))]

def scriptDirPath():
    '''returns the absolute path to the directory containing this script'''
    return os.path.dirname(os.path.realpath(__file__))

def plotInstanceMap( instances, outFilePath ):
    mappedFrameNumLocation = []
    mappedFrameNumLocationUnitedStates = []
    mappedFrameNumLocationRussia = []
    mappedFrameNumLocationOther = []
    
    logger.debug( 'plotting %d instances', len(instances))
    for j in range(0,len(instances)):
        inst = instances[j]
        #locInfo = inst.get( 'device-location', {} )
        #latLon = locInfo.get( 'latitude', None), locInfo.get( 'longitude', None)
        #logger.info( 'loc: %s, %s', latLon, locInfo['display-name'] )
        #print( latLon, locInfo['display-name'] )

        mappedFrameNumLocation.append([j,
            inst["device-location"]["latitude"],
            inst["device-location"]["longitude"],
            inst["device-location"]["display-name"],
            inst["device-location"]["country"]
            ])
        if inst["device-location"]["country"] =="United States":
            mappedFrameNumLocationUnitedStates.append([j,
                inst["device-location"]["latitude"],
                inst["device-location"]["longitude"],
                inst["device-location"]["display-name"],
                inst["device-location"]["country"]
                ])
        elif inst["device-location"]["country"] == "Russia":
            mappedFrameNumLocationRussia.append([j,
                inst["device-location"]["latitude"],
                inst["device-location"]["longitude"],
                inst["device-location"]["display-name"],
                inst["device-location"]["country"]
                ])
        else:
            mappedFrameNumLocationOther.append([j,
                inst["device-location"]["latitude"],
                inst["device-location"]["longitude"],
                inst["device-location"]["display-name"],
                inst["device-location"]["country"]
                ])

    logger.debug("Locations:")
    for i in range(0,len(mappedFrameNumLocation)):
        logger.debug("%s", mappedFrameNumLocation[i][3])

    logger.debug("reading World Map data")
    # assume the base map is in the same dir as this running script
    mapFilePath =  os.path.join( scriptDirPath(), "WorldCountryBoundaries.csv" )
    mapFile = open(mapFilePath, "r")
    mapLines = mapFile.readlines()
    mapFile.close()
    mapNumLines = len(mapLines)    

    CountryData = []
    CountrySphericalData = []

    for i in range(1,mapNumLines) :
        firstSplitString = mapLines[i].split("\"")
        nonCoordinateString = firstSplitString[2]    
        noncoordinates = nonCoordinateString.split(",")
        countryString = noncoordinates[6]

        if firstSplitString[1].startswith('<Polygon><outerBoundaryIs><LinearRing><coordinates>') and firstSplitString[1].endswith('</coordinates></LinearRing></outerBoundaryIs></Polygon>'):
            coordinateString = firstSplitString[1].replace('<Polygon><outerBoundaryIs><LinearRing><coordinates>','').replace('</coordinates></LinearRing></outerBoundaryIs></Polygon>','').replace(',0 ',',0,')
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

            CountryData.append([countryString,coordinateList])
            CountrySphericalData.append([countryString,coordinateSphericalList])
        else :
            reducedCoordinateString = firstSplitString[1].replace('<MultiGeometry>','').replace('</MultiGeometry>','').replace('<Polygon>','').replace('</Polygon>','').replace('<outerBoundaryIs>','').replace('</outerBoundaryIs>','').replace('<innerBoundaryIs>','').replace('</innerBoundaryIs>','').replace('<LinearRing>','').replace('</LinearRing>','').replace('</coordinates>','').replace(',0 ',',0,')
            coordinateStringSets = reducedCoordinateString.split("<coordinates>")
            coordinateSets= []
            for j in range(1,len(coordinateStringSets)) :
                coordinateSets.append([float(k) for k in coordinateStringSets[j].split(",")])
            coordinateList = []
            coordinateSphericalList = []
            for j in range(0,len(coordinateSets)) :
                coordinateList.append(np.zeros([int(len(coordinateSets[j])/3),2]))
                for k in range(0,len(coordinateList[j])) :
                    coordinateList[j][k,:] = coordinateSets[j][k*3:k*3+2]
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

    logger.debug("Plotting")
    figSize1 = (19.2, 10.8)
    fontFactor = 0.75
    mpl.rcParams.update({'font.size': 22})
    mpl.rcParams['axes.linewidth'] = 2 #set the value globally
    markerSize = 10
    markeredgecolor='black'

    # plot world map
    fig = plt.figure(3, figsize=figSize1)
    ax = fig.gca()
    # Turn off tick labels
    ax.set_yticklabels([])
    ax.set_xticklabels([])
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

    plt.plot(getColumn(mappedFrameNumLocationUnitedStates,2),
        getColumn(mappedFrameNumLocationUnitedStates,1),
        linestyle='',color=(0.0, 0.5, 1.0),marker='o',
        markersize=markerSize, markeredgecolor='black', markeredgewidth=0.75
        )
    plt.plot(getColumn(mappedFrameNumLocationRussia,2),
        getColumn(mappedFrameNumLocationRussia,1),
        linestyle='', color=(1.0, 0.0, 0.0),marker='o',
        markersize=markerSize, markeredgecolor='black', markeredgewidth=0.75
        )
    plt.plot(getColumn(mappedFrameNumLocationOther,2),
        getColumn(mappedFrameNumLocationOther,1),
        linestyle='', color=(0.0, 0.9, 0.0),marker='o',
        markersize=markerSize, markeredgecolor='black', markeredgewidth=0.75
        )
    plt.xlim([-180,180])
    plt.ylim([-60,90])
    #plt.show()
    plt.savefig( outFilePath, bbox_inches='tight')


if __name__ == "__main__":
    # configure logger formatting
    logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
    logDateFmt = '%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
    logging.basicConfig(format=logFmt, datefmt=logDateFmt)

    # treat numpy deprecations as errors
    warnings.filterwarnings('error', category=np.VisibleDeprecationWarning)

    ap = argparse.ArgumentParser( description=__doc__, fromfile_prefix_chars='@', formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    #ap.add_argument( '--dataDirPath', required=True, help='the path to to directory for input and output data' )
    ap.add_argument( 'launchedJsonFilePath', help='the path to the instances file to map' )
    ap.add_argument( 'outFilePath', help='the path to the png file to create' )
    args = ap.parse_args()

    launchedJsonFilePath = args.launchedJsonFilePath
    logger.debug("launchedJsonFilePath: %s", launchedJsonFilePath)

    outFilePath = args.outFilePath
    # make sure outFilePath ends with a supported file-type extension (.png, ,pdf, or whatever)
    extension = os.path.splitext(outFilePath)[1]
    if extension:
        extension = extension[1:]
    allowedTypes = set(plt.figure().canvas.get_supported_filetypes().keys() )
    if extension not in allowedTypes:
        outFilePath += '.png'
        logger.info( 'saving to PNG file')
        logger.info( 'supported file types: %s', allowedTypes )
    logger.info("outFilePath: %s", outFilePath)

    if not os.path.isfile( launchedJsonFilePath ):
        logger.error( 'file not found: %s', launchedJsonFilePath )
        sys.exit( 1 )

    launchedInstances = []
    with open( launchedJsonFilePath, 'r') as jsonInFile:
        try:
            launchedInstances = json.load(jsonInFile)  # an array
        except Exception as exc:
            sys.exit( 'could not load json (%s) %s' % (type(exc), exc) )

    logger.debug("number of launchedInstances: %d", len(launchedInstances))

    plotInstanceMap( launchedInstances, outFilePath )
