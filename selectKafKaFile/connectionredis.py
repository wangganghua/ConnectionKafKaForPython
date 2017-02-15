# -*- encoding:utf-8 -*-
# time: 2016-12-20
# author: wgh

import redis
import json
import time
from saveerrorlogs import *


class ConnectionRedis:

    host = "117.122.192.50"  # 主机ip
    port = 6479  # 端口
    db = 0  # key存放的数据库
    key = "wgh_test:items"  # key名称
    pool = ""  # 连接池
    ropen = ""  # redis open

    def __init__(self, keys):
        # print ("读取数据key %s " % keys)
        self.key = keys
        isValue = True
        while(isValue):
            try:
                # python 处理redis时使用pool，节省速度和资源
                # self.pool = redis.ConnectionPool(host="%s" % self.host, port="%s" % self.port, db="%s" % self.db)
                # # self.ropen = redis.Redis(connection_pool=self.pool, charset="gbk", decode_responses=True)
                # self.ropen = redis.Redis(connection_pool=self.pool)
                self.ropen = redis.Redis(host='117.122.192.50', port=6479, db=0)
                self.ropen.ping()
                isValue = False
            except Exception, ex:
                isValue = True
                print("redis连接错误信息:%s"), ex
                wr = SaveErrorLogsFile("redis连接错误信息,等待1分钟：%s".encode("gbk") % ex.message)
                wr.saveerrorlog()
                time.sleep(60)

    def getredisdatalrange(self):
        isValue = True
        while(isValue):
            try:
                # r = redis.Redis(connection_pool=self.pool)
                # print ("开始读取数据")
                redisvalues =self.ropen.lrange(self.key, 0, 0)
                # 添加此处exception 异常
                isValue = False
                self.ropen.ping()
                return redisvalues
            except Exception, ex:
                isValue = True
                print("redis读取key错误信息:%s"), ex
                wr = SaveErrorLogsFile("redis连接错误信息,等待1分钟：%s".encode("gbk") % ex.message)
                wr.saveerrorlog()
                time.sleep(60)

    def getredisdatarpop(self):
        isValue = True
        while(isValue):
            try:
                # print ("开始读取数据")
                redisvalues =self.ropen.rpop(self.key)
                # self.ropen.pop
                # 添加此处exception 异常
                self.ropen.ping()
                isValue = False
                return redisvalues
            except Exception, ex:
                isValue = True
                print("redis连接错误信息,等待1分钟:%s" % ex)
                wr = SaveErrorLogsFile("redis连接错误信息,等待1分钟：%s".encode("gbk") % ex.message)
                wr.saveerrorlog()
                time.sleep(60)

    def getredisdatalpop(self):
        isValue = True
        while(isValue):
            try:
                # print ("开始读取数据")
                redisvalues =self.ropen.lpop(self.key)
                # self.ropen.pop
                # 添加此处exception 异常
                self.ropen.ping()
                isValue = False
                return redisvalues
            except Exception, ex:
                isValue = True
                print("redis读取key信息错误:%s" % ex)
                wr = SaveErrorLogsFile("redis读取key信息,等待1分钟：%s".encode("gbk") % ex.message)
                wr.saveerrorlog()
                time.sleep(60)

    def getrediskeyname(self):
        isValue = True
        while (isValue):
            try:
                # print ("开始查询是否存在key %s " % self.key)
                iskey = self.ropen.keys("%s" % self.key)
                # 添加此处exception 异常
                self.ropen.ping()
                isValue = False
                if iskey:
                    # print "is value %s" % iskey
                    return 1
                else:
                    # print "not fount %s" % iskey
                    return 0

            except Exception, ex:
                isValue = True
                # print("查询是否存在key:%s"), ex
                wr = SaveErrorLogsFile("redis查询是否存在key：%s".encode("gbk") % ex.message)
                wr.saveerrorlog()


