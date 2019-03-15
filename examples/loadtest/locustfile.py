from locust import HttpLocust, TaskSet

def login(l):
    l.client.post("/login", {"username":"ellen_key", "password":"education"})

def index(l):
    l.client.get("/")

def index_orig(l):
    l.client.get("/index_orig.html")

def favicon(l):
    l.client.get("/favicon.ico")

def image(l):
    l.client.get("/img/module_table_top.png")
    #l.client.get("/img/nginx.png")

class UserBehavior(TaskSet):
    tasks = {index: 1}
    #tasks = {index: 2, image: 2, favicon: 1}

    #def on_start(self):
    #    login(self)

class WebsiteUser(HttpLocust):
    task_set = UserBehavior
    min_wait = 0  # 1000
    max_wait = 0  # 2000
