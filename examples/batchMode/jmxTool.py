#!/usr/bin/env python3
'''parses a JMeter .jmx file, gives information about the TestPlan'''
import argparse
import logging
import sys

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# try imorting defusedxml, which avoids some vulnerabilities
try:
    import defusedxml.ElementTree as ET
except ImportError:
    # fall back to using the standrad xml package
    print( 'using standard xml package', file=sys.stderr)
    import xml.etree.ElementTree as ET


def isNumber( sss ):
    try:
        float(sss)
        return True
    except ValueError:
        return False

def numberOrZero( txt ):
    if txt is None:
         return 0
    elif txt.isnumeric():
        return int( txt )
    elif isNumber( txt ):
        return float( txt )
    else:
        return 0

def parseJmxFile( jmxFilePath ):
    '''parses the jmx file as xml, returns the elementTree'''
    tree = ET.parse( jmxFilePath )
    return tree

def getDuration( tree ):
    root = tree
    # works juast as well with a tree as with the root element of a tree
    #root = tree.getroot()
    totDur = 0
    attribNames = set()
    groups =root.findall("./hashTree/hashTree/ThreadGroup")
    nThreadGroups = len(groups)
    #print( nThreadGroups, 'ThreadGroups' )
    for group in groups:
        delay = dur = 0
        for elem in group:
            attribNames.add( elem.attrib.get( 'name' ) )
            if elem.attrib.get( 'name' ) == 'ThreadGroup.num_threads':
                delay = numberOrZero(elem.text)
            elif elem.attrib.get( 'name' ) == 'ThreadGroup.delay':
                delay = numberOrZero(elem.text)
            elif elem.attrib.get( 'name' ) == 'ThreadGroup.duration':
                dur = numberOrZero(elem.text)
            elif elem.attrib.get( 'name' ) == 'ThreadGroup.ramp_time':
                delay = numberOrZero(elem.text)
        effDur = delay + dur
        totDur = max( totDur, effDur )
    return totDur


if __name__ == "__main__":
    # configure logger formatting
    logger = logging.getLogger(__name__)
    logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
    logDateFmt = '%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
    logging.basicConfig(format=logFmt, datefmt=logDateFmt)

    ap = argparse.ArgumentParser( description=__doc__,
        fromfile_prefix_chars='@', formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    ap.add_argument( 'jmxFilePath', help='the JMeter test plan file path' )
    args = ap.parse_args()


    jmxFilePath = 'TestPlan.jmx'
    jmxFilePath = 'TestPlan_RampLong_LessSlow.jmx'
    #jmxFilePath = 'jmeterWorker/JPetstore_JMeter5.4.1.jmx'
    jmxFilePath = 'TestPlan_RampLonger.jmx'
    jmxFilePath = args.jmxFilePath

    #nThreadGroups = 0

    tree = parseJmxFile( jmxFilePath )

    #print( 'EXTRACTED duration', getDuration( tree ) )
    root = tree.getroot()
    if False:  # enable this for debuigging
        totDur = 0
        
        # for iterating everything down to greatGrandChildren
        for child in root:
            #print( 'child', child )
            print(child.tag, child.attrib)
            for grandChild in child:
                print( '  ', grandChild.tag )
                #print( '  ', grandChild.tag, grandChild.attrib)
                for greatGrandChild in grandChild:
                    #print( '    ', greatGrandChild.tag )
                    print( '    ', greatGrandChild.tag, grandChild.attrib )
                if grandChild.tag == 'hashTree':
                    groups = grandChild.findall( 'ThreadGroup' )
                    print( len(groups), 'ThreadGroups' )
                    for group in groups:
                        print( '  threadGroup:')
                        delay = dur = effDur = 0
                        for elem in group:
                            print( '    ', elem.tag, elem.attrib, elem.text )
                            if elem.attrib.get( 'name' ) == 'ThreadGroup.delay':
                                delay = numberOrZero(elem.text)
                                print( '      DELAY', delay )
                            if elem.attrib.get( 'name' ) == 'ThreadGroup.duration':
                                dur = numberOrZero(elem.text)
                                print( '      DURATION', dur )
                        effDur = delay + dur
                        totDur = max( totDur, effDur )
                        print( '      EFFDUR', effDur )
        print( 'totDur', totDur )
        
    '''
    print()
    print( 'iterating flatly' )
    for elem in root.iter():
        print( elem.tag, elem.attrib, elem.text )
    '''
    print()
    print( 'version info', root.attrib )
    print( 'jmeter version', root.attrib.get( 'jmeter') )
    
    print( 'iterating with XPATH' )
    totDur = 0
    attribNames = set()
    groups = root.findall("./hashTree/hashTree/ThreadGroup")
    nThreadGroups = len(groups)
    print( nThreadGroups, 'ThreadGroups' )
    for group in groups:
        print( '  threadGroup:')
        delay = dur = 0
        for elem in group:
            #print( '    ', elem.tag, elem.attrib, elem.text )
            attribNames.add( elem.attrib.get( 'name' ) )
            if elem.attrib.get( 'name' ) == 'ThreadGroup.num_threads':
                delay = numberOrZero(elem.text)
                print( '    num_threads', delay )
            if elem.attrib.get( 'name' ) == 'ThreadGroup.delay':
                delay = numberOrZero(elem.text)
                print( '    delay', delay )
            if elem.attrib.get( 'name' ) == 'ThreadGroup.duration':
                dur = numberOrZero(elem.text)
                print( '    duration', dur )
            if elem.attrib.get( 'name' ) == 'ThreadGroup.ramp_time':
                delay = numberOrZero(elem.text)
                print( '    ramp_time', delay )
        effDur = delay + dur
        totDur = max( totDur, effDur )
        print( '    EFFDUR', effDur )
    print( 'attribs found:', attribNames )
    print( 'TOTDUR', totDur )
