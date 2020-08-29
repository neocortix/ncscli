import json
import os

from locust import HttpLocust, TaskSet
# newer versions of Locust would use HttpUser instead of HttpLocust

def index(l):
    l.client.get("")

g_targetUris = []

def getTargets(l):
    for uri in g_targetUris:
        l.client.get(uri, name=uri)

class UserBehavior(TaskSet):
    global g_targetUris
    targetUriFilePath = 'targetUris.json'
    if os.path.isfile( targetUriFilePath ):
        with open( targetUriFilePath ) as inFile:
            g_targetUris = json.load( inFile )
        tasks = {getTargets: 1}
    else:
        # will just fetch the top-level url if no targetUris on file
        tasks = {index: 1}

class WebsiteUser(HttpLocust):
    task_set = UserBehavior
    min_wait = 0  # 1000
    max_wait = 0  # 2000
