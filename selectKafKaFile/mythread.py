# -*- encoding:utf-8 -*-
import threading
import time

# 创建一个进程类


class TheardClass(threading.Thread):
    def __init__(self,threadname):
        threading.Thread.__init__(self, name=threadname)

    def run(self):
        for i in xrange(10):
            print self.getName(), i
            time.sleep(1)
