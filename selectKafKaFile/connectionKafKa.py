# -*- encoding:utf-8 -*-
# --------consumer--------
from datetime import datetime
import sys
import os
from connectionsqlserver import *
from connectionredis import *
import threading
import redis
import re
import Tkinter
import tkMessageBox
reload(sys)
sys.setdefaultencoding('utf8')
sys.path.append(sys.path)

rconnection = redis.Redis(host='117.122.192.50', port=6479, db=0)

# 连接redis


def connectionredis():
    print ("wang is 'hh'")
    # python 处理redis时使用pool，节省速度和资源
    # pool= redis.ConnectionPool(host='117.122.192.50', port=6479, db=0, charset="gbk", decode_responses=True)
    pool = redis.ConnectionPool(host='117.122.192.50', port=6479, db=0)
    try:
        r = redis.Redis(connection_pool=pool, charset="gbk", decode_responses=True)
        # r = redis.Redis(host='127.0.0.1', port=6379, db=0, charset="gbk", decode_responses=True)
        # r.set( "liwumei" ,list_ax)
        # print ("成功注入redis")
        print ("开始读取数据")
        wgh = r.lindex("wgh_test:items", 0)
        print (wgh)
        hjson = json.loads(wgh)
        print (hjson)
        print (hjson["url"])
        print (hjson["attr"])
        print (r.dbsize())
        print (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        sx = r.lrange("wgh_test:items", 0, 859721)
        print (len(sx))
        if len(sx) > 0:
            a = 0
            for ix in sx:
                a = a + 1
                print (str(a) + ix)
        print (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        # 添加此处exception 异常
        r.ping()
    except Exception, ex:
        print("redis连接错误信息:%s" % ex)


# 连接SqlServer


def connectionsqlserverSxx():
    # root= Tkinter.Tk()
    # b= Tkinter.Button(root, text= "是按钮")
    # b.pack()
    # root.mainloop()
    # tkMessageBox._show(title="提示",message="开始连接SqlServer")
    print (u"开始连接SqlServer")
    wgh = True
    wa = 1
    while wgh:
        try:
            con = pymssql.connect(user="sa", password="All_View_Consulting_2014@", host="192.168.2.236", timeout=30)
        except pymssql.OperationalError, e:
            wr = SaveErrorLogsFile("连接SqlServer数据库错误：%s".encode("gbk") % e.message)
            wr.saveerrorlog()
            wgh = True
            wa += 1
            print ("开始连接SqlServer第 %s 次" % wa)
        else:
            wgh = False
            cur = con.cursor()
            print (u"成功打开SqlServer连接")
            sql = "select top 10 url,全部评论,中评,好评,差评 from DBA.dbo._bf_jd_url"
            sql = sql.encode("gbk")
            cur.execute(sql)
            for ix in cur:
                print (str(ix[0]) + "-" + str(ix[1]) + "-" + str(ix[2]) + "-" + str(ix[3]) + "-" + str(ix[4]))
            cur.close()
            con.close()
            print (u"完成，关闭SqlServer连接")
        # tkMessageBox.showinfo(title="提示", message="完成,关闭SqlServer连接")
        #  inputx = raw_input("请输入任意键结束：".encode("gbk"))
        #  if inputx == "" or len(inputx) > 0:
        #     print ("退出".encode("gbk"))


def sextdd():
    ax = ConnectionRedis("wgh_test:items")
    axw = ax.getredisdatalrange()
    listax = []
    if axw:
        startime = datetime.now()
        print ("%s : 共 %s 条数据，开始 0 条" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), len(axw)))
        index = 0
        indexCount = 0  # 执行的总条数
        errorCount = 0  # 失败条数json类型转换失败
        while (indexCount <= len(axw)):
            axw2 = ax.getredisdatalpop()
            print ("%s --: %s" % (indexCount, axw2))
            if axw2:
                index += 1
                indexCount += 1
                try:
                    hjson = json.loads(axw2.replace('\r\n', ''))
                    url = hjson["url"]
                    urlleibie = hjson["attr"]["urlleibie"]
                    urlweb = hjson["attr"]["urlweb"]
                    brand = hjson["attr"]["brand"]
                    model = hjson["attr"]["model"]
                except Exception, e:
                    errorCount += 1
                    wr = SaveErrorLogsFile("redis数据获取json格式错误信息：%s".encode("gbk") % e.message)
                    wr.saveerrorlog()
                    print (e.message)
                    continue
                else:
                    listax.append(
                            (url.encode("gbk"), urlleibie.encode("gbk"), urlweb.encode("gbk"), brand.encode("gbk")))
                    if (index >= 1000):  # 每1k条数据插入一次数据库
                        wx = ConnectionSqlServer("insert into 备份测试表(备份,备份2,备份3,备份4) values (%s,%s,%s,%s)")
                        wx.inserintosqlserver(listax)
                        listax = []  # 清空数组
                        print ("%s : 共 %s 条数据，开始 %s 条" % (
                            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), len(axw), indexCount))
                        index = 0
        # 剩余的数据注入
        wx = ConnectionSqlServer("insert into 备份测试表(备份,备份2,备份3,备份4) values (%s,%s,%s,%s)")
        wx.inserintosqlserver(listax)
        print ("%s : 共 %s 条数据，开始 %s 条" % (
                    time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), len(axw), len(axw)))
        listax = []  # 清空数组

        # for i in axw:
        #     index += 1
        #     indexCount += 1
            # try:
            #     hjson = json.loads(i.replace('\r\n', ''))
            #     url = hjson["url"]
            #     urlleibie = hjson["attr"]["urlleibie"]
            #     urlweb = hjson["attr"]["urlweb"]
            #     brand = hjson["attr"]["brand"]
            #     model = hjson["attr"]["model"]
            # except Exception, e:
            #     errorCount += 1
            #     wr = SaveErrorLogsFile("redis数据获取json格式错误信息：%s".encode("gbk") % e.message)
            #     wr.saveerrorlog()
            #     print e.message
            #     continue


        #     listax.append((url.encode("gbk"), urlleibie.encode("gbk"), urlweb.encode("gbk"), brand.encode("gbk")))
        #     if (index >= 1000):  # 每1k条数据插入一次数据库
        #         # wx = ConnectionSqlServer("insert into 备份测试表(备份,备份2,备份3,备份4) values (%s,%s,%s,%s)")
        #         # wx.executesqlserver(listax)
        #         listax = []  # 清空数组
        #         print ("%s : 共 %s 条数据，开始 %s 条" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), len(axw), indexCount))
        #         index = 0
        # 剩余的数据注入
        # wx = ConnectionSqlServer("insert into 备份测试表(备份,备份2,备份3,备份4) values (%s,%s,%s,%s)")
        # wx.executesqlserver(listax)
        # print ("%s : 共 %s 条数据，开始 %s 条" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), len(axw), len(axw)))
        # listax = []  # 清空数组
    print ("结束 -- 共 %s 条 , 失败 %s 条" % (len(axw), errorCount))
    endtime = datetime.now()
    print (endtime - startime)

# 创建表CJDATAXXXX


def insertsqlserver_new(sqltablename):
    keyname = "SqlServerInsertAllUrl:items"
    startime = datetime.now()   # 开始时间
    # ax = ConnectionRedis("SqlServerInsertAllUrl:items.bak")
    errorCount = 0  # 失败条数json类型转换失败
    index = 0  # 记录条数,没10条插入SqlServer数据库
    indexCount = 0  # 记录总条数
    listax = []  # 存放结果集
    while True:
        # axw = ax.getredisdatalpop()
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
                spchuxiao = hjson["cx_info"]
                if len(spchuxiao) > 200:
                    spchuxiao = spchuxiao[0:200]
                spurl = hjson["sp_url"]
                if len(spurl) > 1000:
                    spurl = spurl[0:1000]
                spxinghao = hjson["attr"]["model"]
                if spxinghao == "model" or spxinghao == "":
                    spxinghao = hjson["sp_models"]
                cxprice = hjson["sp_web_price"]
                if cxprice == "":
                    cxprice = "0"
                spjiage = hjson["sp_web_price"]
                if spjiage == "":
                    spjiage = "0"
                sppinpai = hjson["attr"]["brand"]
                if sppinpai == "brand" or sppinpai == "":
                    sppinpai = hjson["sp_brand"]

                spshop = hjson["sp_dianp"]

                spname = hjson["sp_name"]
                if len(spname) > 200:
                    spname = spname[0:200]
                spplriqi = hjson["sp_plriqi"]

                sppinglun = hjson["sp_pl"]

                if len(sppinglun) > 2000:
                    sppinglun = sppinglun[0:2000]

                spmaijia = hjson["sp_maijia"]

                spleibie = hjson["attr"]["urlleibie"]
                urlweb = hjson["attr"]["urlweb"]
                if urlweb == "京东商城":
                    urlweb = "JD"
                elif urlweb == "天猫商城":
                    urlweb = "TM"
                elif urlweb == "苏宁易购":
                    urlweb = "SN"
                elif urlweb == "国美在线":
                    urlweb = "GM"
                elif urlweb == "亚马逊":
                    urlweb = "YMX"
                elif urlweb == "1号店":
                    urlweb = "YHD"
                elif urlweb == "当当网":
                    urlweb = "DD"
                elif urlweb == "淘宝网":
                    urlweb = "TB"
                webprice = hjson["sp_web_price"]
                if webprice == "":
                    webprice = "0"
                spgmnum = 1
                cjjiage = hjson["sp_web_price"]
                spkey = ""
                writetime = datetime.now()
                if "热水器" in spleibie:
                    spleibie = "热水器".encode("gbk")
                elif "榨汁机" in spleibie:
                    spleibie = "食品料理机".encode("gbk")


            except Exception, e:
                errorCount += 1
                wr = SaveErrorLogsFile("redis数据获取json格式错误信息：%s --内容：%s".encode("gbk") % (e.message, axw))
                wr.saveerrorlog()
                print (e.message)
            try:
                # listax.append((spurl.decode("utf8").encode("gbk"), spname.decode("utf8").encode("gbk"),
                #            spchuxiao.decode("utf8").encode("gbk"), spleibie.decode("utf8").encode("gbk"),
                #            sppinpai.decode("utf8").encode("gbk"), spxinghao.decode("utf8").encode("gbk"),
                #            spjiage.decode("utf8").encode("gbk"), webprice.decode("utf8").encode("gbk"),
                #            cxprice.decode("utf8").encode("gbk"), cjjiage.decode("utf8").encode("gbk"),
                #            sppinglun.decode("utf8").encode("gbk"), spplriqi.decode("utf8").encode("gbk"),
                #            spmaijia.decode("utf8").encode("gbk"), writetime, spshop.decode("utf8").encode("gbk"),
                #            spkey.decode("utf8").encode("gbk"), spgmnum, urlweb.decode("utf8").encode("gbk")))
                listax.append((spurl, spname,spchuxiao, spleibie,sppinpai, spxinghao, spjiage, webprice, cxprice, cjjiage,
                               sppinglun, spplriqi,spmaijia, writetime, spshop, spkey, spgmnum, urlweb))
                index += 1
            except Exception, e:
                wr = SaveErrorLogsFile("redis result encode gbk error ：%s : 内容--- %s".encode("gbk") % (e.message, axw))
                wr.saveerrorlog()
                print ("redis result encode gbk error：%s" % e.message)
            if (index >= 100):  # 每100条数据插入一次数据库
                wx = ConnectionSqlServer("insert into {0}("
                                     "spurl,spname,spchuxiao,spleibie,"
                                     "sppinpai,spxinghao,spjiage,webprice,"
                                     "cxprice,cjjiage,sppinglun,spplriqi,"
                                     "spmaijia,writetime,spshop,spkey,"
                                     "spgmnum,urlweb"
                                     ") values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(sqltablename))
                wx.inserintosqlserver(listax)
                print ("load : %s---%s bar.....insert into %s bar......\r\n" % (datetime.now(), indexCount,  index))
                listax = []  # 清空数组
                index = 0
        else:
            # 剩余的数据注入
            # print "注入剩余数据 %s：%s" % (len(listax), listax)
            wx = ConnectionSqlServer("insert into {0}("
                                 "spurl,spname,spchuxiao,spleibie,"
                                 "sppinpai,spxinghao,spjiage,webprice,"
                                 "cxprice,cjjiage,sppinglun,spplriqi,"
                                 "spmaijia,writetime,spshop,spkey,"
                                 "spgmnum,urlweb"
                                 ") values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(sqltablename))
            wx.inserintosqlserver(listax)
            listax = []  # 清空数组
            endtime = datetime.now()
            indexCount = 0
            errorCount = 0
            print ("end -- total %s  , fail %s ;total time ：%s" % (indexCount, errorCount, (endtime - startime)))

            if int(datetime.now().hour) >= 0 and int(datetime.now().hour) <= 10:
                print ("没有读取到数据，等待10分钟......")
                time.sleep(10 * 60)
            elif int(datetime.now().hour) >= 11 and int(datetime.now().hour) <= 23:
                print ("没有读取到数据，等待2小时......")
                time.sleep(60 * 2 * 60)
            else:
                print ("没有读取到数据，等待30分钟......%s" % keyname)
                time.sleep(60 * 30)
            print ("return----")
            continue


def createtablename():
    tablename = "_bf_a_wgh_cjjd17W01"
    # 先查询存放周度,如果是周一，则存放数据至上周
    if time.strftime("%A") == "Monday":  # 判断是否为周一
        axweek = ConnectionSqlServer("SELECT TOP 1 标准周度 FROM 日期对应周度表 WHERE convert(nvarchar(10),标准日期,120)=convert(nvarchar(10),getdate()-1,120)")
    else:
        axweek = ConnectionSqlServer("SELECT TOP 1 标准周度 FROM 日期对应周度表 WHERE convert(nvarchar(10),标准日期,120)=convert(nvarchar(10),getdate(),120)")
    axwweek = axweek.selectsqlserverandreturn()

    if axwweek:
        for i in axwweek:
            tablename = "_bf_a_wgh_cjjd17W01"
            print (tablename)
    else:
        print ("error---colud not found week,create table is faile!")
        wr = SaveErrorLogsFile("错误---没有找到周度，没法创建表")
        wr.saveerrorlog()
        return

    ax = ConnectionSqlServer("SELECT COUNT(*) FROM SYSOBJECTS WHERE TYPE='U' AND NAME ='%s'" % tablename)
    axw = ax.selectsqlserverandreturn()
    istable = 0  # 判断是否存在表，0 表示不存在，1 表示存在
    if axw:
        for i in axw:
            istable = i[0]
    else:
        istable = 0

    if istable == 0:    # 如果不存在表 TableName，则创建表
        createtable = ConnectionSqlServer(
            "CREATE TABLE  %s "
            "([ZZID] [BIGINT] IDENTITY(1,1) NOT NULL,"
            "SPURL [VARCHAR](2000) NULL,"
            "SPNAME [VARCHAR](500) NULL,"
            "SPCHUXIAO [VARCHAR](500) NULL,"
            "[SPLEIBIE] [VARCHAR](50) NULL,"
            "[SPSHOPCITY] [VARCHAR](100) NULL,"
            "[SPPINPAI] [VARCHAR](100) NULL,"
            "[SPXINGHAO] [VARCHAR](200) NULL,"
            "[SPJIAGE] [VARCHAR](100) NULL,"
            "[WEBPRICE] [VARCHAR](100) NULL,"
            "[CXPRICE] [VARCHAR](100) NULL,"
            "[CJJIAGE] [VARCHAR](100) NULL,"
            "[SPPINGLUN] [NVARCHAR](2000) NULL,"
            "[SPGUIGE] [VARCHAR](50) NULL,"
            "[SPGMRIQI] [VARCHAR](50) NULL,"
            "[SPPLRIQI] [VARCHAR](50) NULL,"
            "[SPMAIJIA] [VARCHAR](50) NULL,"
            "[SEX] [VARCHAR](50) NULL,"
            "[ADDRESS] [VARCHAR](50) NULL,"
            "[IPADDRES] [VARCHAR](50) NULL,"
            "[WRITETIME] [DATETIME] NULL,"
            "[SPSHOP] [NVARCHAR](50) NULL,"
            "[SPKEY] [NVARCHAR](50) NULL,"
            "[SPGMNUM] [VARCHAR](50) NULL,"
            "[URLWEB] [VARCHAR](50) NULL,"
            "[FLAW] [SMALLINT] NULL,"
            "[DD] [BIT] NULL)" % tablename)
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

# 查找是否存在key,如果不存在，则暂停10分钟后查找


def selectrediskey(keyvalue):
    nx = ConnectionRedis(keyvalue)
    while True:
        if nx.getrediskeyname() == 1:
            break
        else:
            wr = SaveErrorLogsFile("没有找到指定key,等待10分钟")
            wr.saveerrorlog()
            print ("%s" % time.time())
            print ("没有找到指定的key,等待10分钟......: %s".encode("gbk") % datetime.now())
            time.sleep(60 * 10)  # 如果没有找到key则暂停10分钟
            print ("end : %s" % datetime.now())

if __name__ == "__main__":
    createtablename()