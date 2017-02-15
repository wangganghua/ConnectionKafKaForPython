# -*- encoding:utf-8 -*-

from datetime import datetime
import sys
from connectionsqlserver import *
from connectionredis import *
import threading
import redis
reload(sys)
sys.setdefaultencoding('utf8')
sys.path.append(sys.path)

rconnection = redis.Redis(host='1.119.7.234', port=26479, db=0, charset="utf-8")


def insertsqlserver_new(sqltablename):
    keyname = "dpc_phone_url:dpc_phone_data"
    startime = datetime.now()   # 开始时间
    errorCount = 0  # 失败条数json类型转换失败
    index = 0  # 记录条数,没10条插入SqlServer数据库
    indexCount = 0  # 记录总条数
    listax = []  # 存放结果集
    while True:
        try:
            axw = rconnection.lpop(keyname)
        except Exception, e:
            print e
            if "Error 10051" in e.message:
                wr = SaveErrorLogsFile("网络断开，等待1分钟......：%s".encode("gbk") % e.message)
                wr.saveerrorlog()
                print ("网络断开，等待1分钟......")
                time.sleep(60)
                continue
            else:
                wr = SaveErrorLogsFile("连接redis错误：%s".encode("gbk") % e.message)
                wr.saveerrorlog()
        if axw:
            # ♡
            axw = axw.replace("\u2661", "").replace("\u25dd", "").replace("\u1d17", "").replace("\u25dc", "")\
                .replace("\u20e3", "").replace("\ufe0f", "")
            isTrue = True  # 查找是否存在特殊符号
            # 剔除\u00a0 不知道是什么符号
            while isTrue:
                if axw.find("\ud") != -1:
                    a = axw.find("\ud", 0)
                    axw = axw.replace(axw[a:a + 5], "")  # 因为utf编码是5位 所以a+5截取
                else:
                    isTrue = False
            isTrue = True  # 重新赋值
            while isTrue:
                if axw.find("\u00") != -1:
                    a = axw.find("\u00", 0)
                    axw = axw.replace(axw[a:a + 5], "")  # 因为utf编码是5位 所以a+5截取
                else:
                    isTrue = False
            isTrue = True  # 重新赋值
            while isTrue:
                if axw.find("\u200") != -1:
                    a = axw.find("\u200", 0)
                    axw = axw.replace(axw[a:a + 5], "")  # 因为utf编码是5位 所以a+5截取
                else:
                    isTrue = False

            try:
                indexCount += 1
                hjson = json.loads(axw.replace('\r\n', ''))
                buyer_name = hjson["buyer_name"]

                shopname = hjson["shopname"]

                seller_name = hjson["seller_name"]

                color = hjson["color"]

                pl_time = hjson["pl_time"]

                url = hjson["url"]

                versions = hjson["version"]

                category = hjson["attrs"]["category"]

                urlweb = hjson["attrs"]["urlweb"]

                brand = hjson["attrs"]["brand"]

                model = hjson["attrs"]["model"]

                memory = hjson["memory"]
                buy_type = hjson["buy_type"]
                network_type = hjson["network_type"]
                pl = hjson["pl"]
                writetime = datetime.now()

            except Exception, e:
                errorCount += 1
                wr = SaveErrorLogsFile("redis数据获取json格式错误信息：%s --内容：%s".encode("gbk") % (e.message, axw))
                wr.saveerrorlog()
                print (e.message)
            try:
                listax.append((buyer_name, shopname, seller_name, color, pl_time, url, versions, category, urlweb, brand, model, memory, buy_type, network_type, pl, writetime))
                index += 1
            except Exception, e:
                wr = SaveErrorLogsFile("redis result encode gbk error ：%s : 内容--- %s".encode("gbk") % (e.message, axw))
                wr.saveerrorlog()
                print ("redis result encode gbk error：%s" % e.message)
            if (index >= 100):  # 每100条数据插入一次数据库
                wx = ConnectionSqlServer("insert into {0}(buyer_name, shopname, seller_name, color, pl_time, url, versions, category, urlweb, brand, model, memory, buy_type, network_type, pl, writetime)"
                                         " values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(sqltablename))
                wx.inserintosqlserver(listax)
                print ("load : %s---%s bar.....insert into %s bar......\r\n" % (datetime.now(), indexCount,  index))
                listax = []  # 清空数组
                index = 0
        else:
            # 剩余的数据注入
            wx = ConnectionSqlServer(
                "insert into {0}(buyer_name, shopname, seller_name, color, pl_time, url, versions, category, urlweb, brand, model, memory, buy_type, network_type, pl, writetime)"
                " values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(sqltablename))
            wx.inserintosqlserver(listax)
            listax = []  # 清空数组
            endtime = datetime.now()
            indexCount = 0
            errorCount = 0
            print ("end -- total %s  , fail %s ;total time ：%s" % (indexCount, errorCount, (endtime - startime)))

            if int(datetime.now().hour) >= 0 and int(datetime.now().hour) <= 10:
                print ("没有读取到数据，等待10分钟......")
                time.sleep(10 * 60)

            print ("return----")
            continue

def createtablename():
    tablename = "_PHONE_DATA"

    ax = ConnectionSqlServer("SELECT COUNT(*) FROM SYSOBJECTS WHERE TYPE='U' AND NAME ='%s'" % tablename)
    axw = ax.selectsqlserverandreturn()
    istable = 0  # 判断是否存在表，0 表示不存在，1 表示存在
    if axw:
        for i in axw:
            istable = i[0]
    else:
        istable = 0

    if istable == 0:    # 如果不存在表 TableName，则创建表
        createtable = ConnectionSqlServer("CREATE TABLE  %s "
                                          "(ZZID INT IDENTITY(1,1),"
                                          "buyer_name nvarchar(100),"
                                          "shopname nvarchar(100),"
                                          "seller_name nvarchar(100),"
                                          "color nvarchar(100),"
                                          "pl_time nvarchar(30),"
                                          "url nvarchar(500),"
                                          "versions nvarchar(100),"
                                          "category nvarchar(50),"
                                          "urlweb nvarchar(20),"
                                          "brand nvarchar(100),"
                                          "model nvarchar(100),"
                                          "memory nvarchar(200),"
                                          "buy_type nvarchar(100),"
                                          "network_type nvarchar(400),"
                                          "pl nvarchar(4000),"
                                          "writetime datetime)"  % tablename)
        createtable.executesqlserver()
    else:
        print ("the table already exists,go on......%s" % tablename)


    # 开始下载数据 设置多线程 15个
    threads = []
    threadcount = 10
    print ("join start {0}".format(datetime.now()))
    print("%s : begin download data.....set %s thread" % (datetime.now(), threadcount))
    for ai in range(threadcount):
        threads.append(threading.Thread(target=insertsqlserver_new, args={tablename, }))
    for tx in threads:
        tx.start()
    for tx in threads:
        tx.join()
    print ("join end {0}".format(datetime.now()))

if True:
    createtablename()
