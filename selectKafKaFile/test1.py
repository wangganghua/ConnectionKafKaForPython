# coding=utf8
import threading, Queue, redis

r = redis.Redis(host='117.122.192.50', port=6479, db=0)
myq = Queue.Queue(10)
mysq = Queue.Queue(10)
results = []

def create():
    for i in range(10000):
        r.lpush("test11", ("xxx"))

def saveData(loopid):
    ress = []
    while 1:
        wdata = mysq.get()
        ress.append(wdata)
        with open("con_{0}.txt".format(loopid), "ab") as fp:
            fp.write(wdata)
        if len(ress) % 10000 == 0:
            with open("con_{0}.txt".format(loopid), "w") as fp:
                fp.write("")
        mysq.task_done()

def getdata():
    for i in range(1000):
        data = r.lpop("test11")
        # r.lpush("test11", ("xxx"))
        data = ""
        mysq.put(data)
    myq.get()
    myq.task_done()

if __name__ == "__main__":
    # create()
    print "begin"
    for i in  range(10):
        t = threading.Thread(target=getdata, args=())
        t.setDaemon(False)
        t.start()
        myq.put("")
    for i in  range(2):
        t = threading.Thread(target=saveData, args=(i,))
        t.setDaemon(False)
        t.start()
        myq.put("")
    myq.join()
    print len(results)
    print "end"