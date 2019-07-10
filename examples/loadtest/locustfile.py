import json
import os
import sys

from locust import HttpLocust, TaskSet

def login(l):
    l.client.post("/login", {"username":"ellen_key", "password":"education"})

def index(l):
    l.client.get("")

def index_orig(l):
    l.client.get("/index_orig.html")

def favicon(l):
    l.client.get("/favicon.ico")

def image(l):
    l.client.get("/img/module_table_top.png")
    #l.client.get("/img/nginx.png")

g_targetUris = []

def getBunch(l):
    for uri in g_targetUris:
        l.client.get(uri, name=uri)

def doNothing(l):
    pass

class UserBehavior(TaskSet):
    global g_targetUris
    targetUriFilePath = 'targetUris.json'
    if os.path.isfile( targetUriFilePath ):
        with open( targetUriFilePath ) as inFile:
            g_targetUris = json.load( inFile )
        tasks = {getBunch: 1}
    else:
        #print( targetUriFilePath, 'NOT FOUND', file=sys.stderr )
        #tasks = {doNothing: 1}
        tasks = {index: 1}
    #tasks = {index: 2, image: 2, favicon: 1}

    #def on_start(self):
    #    login(self)

class WebsiteUser(HttpLocust):
    task_set = UserBehavior
    min_wait = 0  # 1000
    max_wait = 0  # 2000
