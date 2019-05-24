#!/usr/bin/env python3
"""
eventTiming helps when timing events
"""
# standard library modules
import datetime


class eventTiming(object):
    '''stores name and beginning and ending of an arbitrary "event"'''
    def __init__(self, eventName, startDateTime=None, endDateTime=None):
        self.eventName = eventName
        self.startDateTime = startDateTime if startDateTime else datetime.datetime.now(datetime.timezone.utc)
        self.endDateTime = endDateTime
    
    def __repr__( self ):
        return str(self.toStrList())

    def finish(self):
        self.endDateTime = datetime.datetime.now(datetime.timezone.utc)

    def duration(self):
        if self.endDateTime:
            return self.endDateTime - self.startDateTime
        else:
            return datetime.timedelta(0)

    def toStrList(self):
        return [self.eventName, 
            self.startDateTime.isoformat(), 
            self.endDateTime.isoformat() if self.endDateTime else None
            ]

