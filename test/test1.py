# -*- coding: UTF-8 -*-
import sys, re, time, json, logging
from scrapy.selector import Selector
from scrapy_redis.spiders import RedisSpider
from scrapy.http import Request
import scrapy
from tools import citys, cs, logger
from tools import analy_re_search, analy_re_findall, analy_xpath, analy_meta, _sub, append_data, analy_error
reload(sys)
sys.setdefaultencoding('utf-8')
import pymongo
conn = pymongo.MongoClient('121.42.177.109', 27017)
mdb = conn["prices_crawler"]


class ProductItem(scrapy.Item):
    url = scrapy.Field()
    collectionTime = scrapy.Field()
    requestTime = scrapy.Field()
    # spiderName = scrapy.Field()
    attrs = scrapy.Field()
    platform = scrapy.Field()
    result = scrapy.Field()

class ProductSpider(RedisSpider):
    name = "prices0"#prices2
    redis_key = '%s:start_urls' % name

    start_urls = {
        # -1
        # '{"url": "http://item.jd.com/1649509967.html", "requestTime": "", "spiderName": "", "attr": ""}',
        # '{"url": "https://detail.tmall.com/item.htm?spm=a220m.1000858.1000725.274.8pJFV9&id=527519855205&skuId=3139175575301&cat_id=50900004&rn=29a5cee96bdad079ee03d0d26ed3d172&user_id=2434089723&is_b=1", "requestTime": "", "spiderName": "", "attr": ""}',
        # '{"url": "https://detail.tmall.com/item.htm?id=38856513010", "requestTime": "", "spiderName": "", "attr": ""}',

        # '{"url": "http://product.dangdang.com/60601290.html", "requestTime": "", "spiderName": "", "attr": ""}',
        # '{"url": "https://rwww.amazon.cn/dp/B01DVNFLME", "requestTime": "", "spiderName": "", "attr": ""}',
        # '{"url": "http://item.yhd.com/item/61228509", "requestTime": "", "spiderName": "", "attr": ""}',

        # -2
        # '{"url": "http://product.suning.com/156275796.html", "requestTime": "", "spiderName": "", "attr": ""}',
        # '{"url": "http://item.jd.com/1966956.html", "requestTime": "", "spiderName": "", "attr": ""}',
        # '{"url": "http://item.gome.com.cn/A0005682122-pop8008001589.html", "requestTime": "", "spiderName": "", "attr": ""}',
        # '{"url": "https://detail.tmall.com/item.htm?id=43384650238", "requestTime": "", "spiderName": "", "attr": ""}',
        # '{"url": "http://item.yhd.com/item/61228510", "requestTime": "", "spiderName": "", "attr": ""}',
        # '{"url": "http://product.dangdang.com/60625499.html", "requestTime": "", "spiderName": "", "attr": ""}',
        # '{"url": "http://www.amazon.cn/dp/B00DY1M6IQ", "requestTime": "", "spiderName": "", "attr": ""}',

        # -3
        # '{"url": "http://product.suning.com/101222512.html", "requestTime": "", "spiderName": "", "attr": ""}',

        # 0
        # '{"url": "https://detail.tmall.com/item.htm?spm=a220m.1000858.1000725.6.hKgYoD&id=536394295667&skuId=3202650992773&areaId=110100&cat_id=2&rn=f498be39e6b4d3bf902521beb0fc7f1b&user_id=1085181034&is_b=1", "requestTime": "", "spiderName": "", "attr": ""}',

        # '{"url": "http://item.gome.com.cn/9125594932-1114820037.html", "requestTime": "", "spiderName": "", "attr": ""}',
        # '{"url": "http://item.jd.com/10422909379.html", "requestTime": "", "spiderName": "", "attr": ""}',
        # '{"url": "http://item.jd.com/10461620311.html", "requestTime": "", "spiderName": "", "attr": ""}',
        # '{"url": "http://product.dangdang.com/1016840981.html", "requestTime": "", "spiderName": "", "attr": ""}',
        # '{"url": "http://product.suning.com/107054838.html", "requestTime": "", "spiderName": "", "attr": ""}',
        # '{"url": "https://detail.tmall.com/item.htm?id=527549374349&sku_properties=5919063:6536025", "requestTime": "", "spiderName": "", "attr": ""}',
        # '{"url": "https://detail.tmall.com/item.htm?spm=a220m.1000858.1000725.1.fRe1P1&id=532598988376&skuId=3201948901281&areaId=110100&cat_id=2&rn=def3e39b0e43f626ef594a36752c04f6&user_id=1660485345&is_b=1", "requestTime": "", "spiderName": "", "attr": ""}',
        # '{"url": "https://detail.tmall.com/item.htm?spm=a220m.1000858.1000725.1.fRe1P1&id=532598988376&skuId=3216591814822&areaId=110100&cat_id=2&rn=def3e39b0e43", "requestTime": "", "spiderName": "", "attr": ""}',
        # '{"url": "http://product.suning.com/0000000000/126457696.html", "requestTime": "", "spiderName": "", "attr": ""}',
        # '{"url": "http://item.gome.com.cn/9133620194-1122410075.html", "requestTime": "", "spiderName": "", "attr": ""}',

        # '{"url": "http://product.suning.com/0070089953/143070853.html", "requestTime": "", "spiderName": "", "attr": ""}',
        # '{"url": "http://item.yhd.com/item/2257145", "requestTime": "", "spiderName": "", "attr": ""}',
        # '{"url": "http://item.yhd.com/item/61228510", "requestTime": "", "spiderName": "", "attr": ""}',
        # '{"url": "http://item.yhd.com/item/61228509", "requestTime": "", "spiderName": "", "attr": ""}',
        # '{"url": "http://product.suning.com/0000000000/123921612.html", "requestTime": "", "spiderName": "", "attr": ""}',
    }

    def make_requests_from_url(self, url):
        try:
            taskjson = json.loads(url)
            urlinfo = taskjson["url"]
            logger.info("requests url : {0}".format(urlinfo))
            item_data = {"url": urlinfo,
                         "collectionTime": time.strftime("%Y-%m-%d %H:%M:%S"),
                         "requestTime": taskjson["requestTime"] if "requestTime" in taskjson else None,
                         # "spiderName": taskjson["spiderName"] if "spiderName" in taskjson else None,
                         "attrs": taskjson["attrs"] if "attrs" in taskjson else [],
                         "platform": taskjson["platform"] if "platform" in taskjson else None,
                         "result": {}
                         }
        except Exception, e:
            logger.error("requests url error : {0}".format(e))
            return Request("http://www.baidu.com", callback=self.parseError, dont_filter=True, meta={})

        meta = {"item_data": item_data,
                "headers": {
                       "Accept": "*/*",
                       "Accept-Encoding": "gzip,deflate",
                       "Accept-Language": "en-US,en;q=0.8,zh-TW;q=0.6,zh;q=0.4",
                       "Connection": "keep-alive",
                       "Content-Type": " application/x-www-form-urlencoded; charset=UTF-8",
                       "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko",
                        },
                "reqtype": 1,
                }
        if 'item.jd.com' in str(urlinfo):
            return Request(urlinfo, callback=self.parseJD, dont_filter=True, meta=meta)
        elif 'www.amazon.cn' in str(urlinfo):
            return Request(urlinfo, callback=self.parseYMX, dont_filter=True, meta=meta)
        elif 'item.yhd.com' in str(urlinfo):
            return Request(urlinfo, callback=self.parseYHD, dont_filter=True, meta=meta)
        elif 'product.suning.com' in str(urlinfo):
            return Request(urlinfo, callback=self.parseSN, dont_filter=True, meta=meta)
        elif 'detail.tmall.com' in str(urlinfo):
            return Request(urlinfo, callback=self.parseTM, dont_filter=True, meta=meta)
        elif 'dangdang.com' in str(urlinfo):
            return Request(urlinfo, callback=self.parseDD, dont_filter=True, meta=meta)
        elif 'gome.com.cn' in str(urlinfo):
            return Request(urlinfo, callback=self.parseGM, dont_filter=True, meta=meta)
        # elif 'item.yixun.com' in str(urlinfo):
        #     return Request(urlinfo, callback=self.parseYX, dont_filter=True, meta=meta)
        elif 'taobao.com' in str(urlinfo):
            return Request(urlinfo, callback=self.parseTB, dont_filter=True, meta=meta)
        else:
            logger.error("requests url's website error : {0}".format(urlinfo))
            return Request(urlinfo, callback=self.parseError, dont_filter=True, meta=meta)

    def parseError(self, response):
        pass

    def update_item(self, meta):
        item = ProductItem()
        item["url"] = meta["url"] if "_url" not in meta else meta["_url"]
        item["collectionTime"] = meta["collectionTime"]
        item["requestTime"] = meta["requestTime"]
        # item["spiderName"] = meta["spiderName"]
        item["attrs"] = meta["attrs"]
        item["platform"] = meta["platform"]
        item["result"] = {"result0": meta["tp"] if "tp" in meta else "",
                          "result1": meta["city"] if "city" in meta else "",
                          "result2": meta["price_web"] if "price_web" in meta else None,
                          "result3": meta["price_mobile"] if "price_mobile" in meta else None,
                          "result4": meta["title"].strip() if "title" in meta else "",
                          "result5": meta["promotions"].strip() if "promotions" in meta else "",
                          "result6": meta["shopname"].strip() if "shopname" in meta else "",
                          "result7": meta["promotions_title"].strip() if "promotions_title" in meta else "",
                          # "result8": meta["price_original"] if "price_original" in meta else None,
                          }
        try:
            mdb["data_result"].insert({"time": time.strftime("%Y-%m-%d %H:%M:%S"),
                                       "url": meta["url"],
                                       "result": meta,
                                       })
        except Exception,e:
            logger.error("upload mdb data error : {0} {1}".format(e, meta))
        return item

    def analy_tm(self, content, meta):
        # with open("content.txt", "wb") as fp:
        #     fp.write(content)
        try:
            result = json.loads(content)
            result_p = result["defaultModel"]["itemPriceResultDO"]["priceInfo"]
            result_p = result_p[meta["pid"]] if meta["pid"] in result_p.keys() else result_p["def"] if "def" in result_p else result_p[result_p.keys()[0]]

            price_original = result_p["price"]
            promotionList = result_p["promotionList"] if "promotionList" in result_p else []
            promotionList = result_p["suggestivePromotionList"] if "suggestivePromotionList" in result_p and len(promotionList) == 0 else promotionList

            for promotion in promotionList:
                if "promText" not in promotion:
                    price = promotion["price"]
                    break
            else:
                price = price_original
            return {"price": price,
                    "price_original": price_original,
                    }
        except Exception, error:
            print "error : {0}".format(error)
            return {}

    def parseALL_price_web(self, response):
        # with open("content.txt", "wb") as fp:
        #     fp.write(response.body)
        meta = response.meta["meta"]
        try:
            expre = []
            content = response.body

            meta["city"] = response.meta["city"]
            website = response.meta["website"]
            if website == "YHD":
                expre = citys["YHD"]["web"]["expre"]
            elif website == "SN":
                expre = citys["SN"]["web"]["expre"]
                expre_original = ['"refPrice":"([\d\.]+?)"']
                meta["price_original"] = analy_re_search(content, expre_original, israise=False)
            elif website == "TM":
                content = analy_re_search(
                    content.decode("gbk").encode("utf8").strip(),
                    ['jsonp[\s\S]+?\(([\s\S]*)\)'], israise=True
                )

            if website == "TM":
                result = self.analy_tm(content, meta)
                if "price" in result:
                    meta["price_web"] = result["price"]
                if "price_original" in result:
                    meta["price_original"] = result["price_original"]
            else:
                try:
                    price_web = analy_re_search(content, expre, israise=False)
                    if price_web and price_web != "" and str(price_web) != "0":
                        meta["price_web"] = price_web
                except Exception, e:
                    pass

            if meta["price_web"] is None:
                self.log('parseALLpriceweb2 price_web is null : {0}  {1}'.format(meta["url"]), level=logging.ERROR)
                # return
        except Exception, e:
            # with open("content.txt", "wb") as fp:
            #     fp.write(content)
            self.log('parseALLpriceweb : {0}  {1}'.format(meta["url"], str(e)), level=logging.ERROR)

        item = self.update_item(meta)
        yield item

    def parseJD(self, response):
        meta = response.meta["item_data"]
        reqtype = response.meta["reqtype"] if "reqtype" in response.meta else 1
        logger.info("begin JD : {0}".format(meta["url"]))
        try:
            analy_error(meta, response, "JD")
            # if int(meta["tp"]) < 0:
            #     raise Exception, "_error {0}".format(meta["tp"])

            analy_meta(meta, response, citys["JD"])

            shopid = analy_re_search(response.body, ["shopId:'(\d+)'"], israise=False)
            venderid = analy_re_search(response.body, ["venderId:(\d+)"], israise=False)
            if shopid == "":
                try:
                    shopid = analy_re_search(response.xpath('//div[@class="seller-infor"]/a/@href')[0].extract(), 'index-(\d+)')
                except:
                    shopid = analy_re_search(response.body, ['mall.jd.com/index-(.+?)\.html',
                                                             "shopId:'(\d+)'", "venderId:(\d+)", 'data-vid="(\d+)"'
                                                             ], israise=False)
            if shopid == "":
                meta["tp"] = -1
                raise Exception, "_error {0}".format(meta["tp"])
            if venderid == "":
                logger.error("JD venderid is none : {0}".format(meta["url"]))
                venderid = shopid

            cat = analy_re_search(response.body, ['cat: \[(.+?)\],'])

            promotion_url = "http://cd.jd.com/promotion/v2?callback=jQuery5089243&skuId={1}&area=1_72_2799_0&shopId={0}&venderId={3}&cat={2}&_=1464079267293".format(
                    shopid, meta["pid"], cat, venderid)

            c03cn_url = "http://c0.3.cn/stock?skuId={0}&area=1_72_2799_0&venderId={1}&cat={2}&buyNum=1&extraParam=".format(
                meta["pid"], venderid, cat) + r"{%22originid%22:%221%22}&ch=1&pduid=1842884465&pdpin="

            meta["promotion_url"] = promotion_url

            # logger.info("pid:{0}\nshopid:{1}\nvenderid:{2}\ncat:{3}".format(meta["pid"], shopid, venderid, cat))
            logger.info("reqtype: {0}".format(reqtype))
            # print c03cn_url

            if reqtype == 1:
                yield Request(c03cn_url, meta=meta, dont_filter=True, callback=self.parseJD_c03cn)
            else:
                yield Request(promotion_url, meta=meta, dont_filter=True, callback=self.parseJD_title)
        except Exception, e:
            self.log('parseJD : {0}  {1}'.format(meta["url"], str(e)), level=logging.ERROR)
            for c in cs:
                meta["city"] = c
                item = self.update_item(meta)
                yield item

    def parseJD_c03cn(self, response):
        meta = response.meta
        # with open("content.txt", "wb") as fp:
        #     fp.write(response.body)
        try:
            realpid = analy_re_search(response.body, ['"realSkuId":(\d+),', ])
            print str(realpid), str(meta["pid"])
            if str(realpid) == str(meta["pid"]):
                yield Request(meta["promotion_url"], meta=meta, dont_filter=True, callback=self.parseJD_title)
            else:
                urlinfo = "http://item.jd.com/{0}.html".format(realpid)
                new_meta = {"item_data": {"url": urlinfo,
                                          "_url": meta["url"],
                                         "collectionTime": meta["collectionTime"],
                                         "requestTime": meta["requestTime"],
                                         "attrs": meta["attrs"],
                                         "result": {}
                                         },
                            "headers": {
                                "Accept": "*/*",
                                "Accept-Encoding": "gzip,deflate",
                                "Accept-Language": "en-US,en;q=0.8,zh-TW;q=0.6,zh;q=0.4",
                                "Connection": "keep-alive",
                                "Content-Type": " application/x-www-form-urlencoded; charset=UTF-8",
                                "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko",
                            },
                            "reqtype": 2,
                            }
                yield Request(urlinfo, callback=self.parseJD, dont_filter=True, meta=new_meta)
        except Exception, e:
            self.log('parseJD_c02cn : {0}  {1}'.format(meta["url"], str(e)), level=logging.ERROR)
            for c in cs:
                meta["city"] = c
                item = self.update_item(meta)
                yield item

    def parseJD_title(self, response):
        meta = response.meta
        try:
            try:
                content = re.search('.*?\(([\s\S]+)\)', response.body).group(1).decode("gbk")
                result = json.loads(content)

                ads = result["ads"] if "ads" in result else []
                ads_quan = result["quan"] if "quan" in result else None
                ads_pro = result["prom"] if "prom" in result else None
                ads_pro_pick = ads_pro["pickOneTag"] if ads_pro and "pickOneTag" in ads_pro else []
                ads_pro_tag = ads_pro["tags"] if ads_pro and "tags" in ads_pro else []
                ads_skucoupon = result["skuCoupon"] if "skuCoupon" in result else []

                promotion = {}
                meta["promotions"] = ""
                meta["promotions_title"] = ""
                for index, adi in enumerate(ads):
                    meta["promotions_title"] += _sub(adi["ad"], [('<.+?>', ' '), ]).strip()
                    if index != len(ads) - 1:
                        meta["promotions_title"] += " ; "

                for value in ads_pro_pick:
                    try:
                        if value["name"] not in promotion:
                            promotion[value["name"]] = []
                        promotion[value["name"]].append(value["content"])
                    except Exception, error:
                        logger.debug("*** ads_pro_pick key error : {0}".format(error))
                for value in ads_pro_tag:
                    try:
                        if value["name"] not in promotion:
                            promotion[value["name"]] = []
                        promotion[value["name"]].append(value["content"])
                    except Exception, error:
                        logger.debug("*** ads_pro_tag key error : {0}".format(error))
                promotion["领劵"] = []
                for value in ads_skucoupon:
                    try:
                        promotion["领劵"].append("满{0}减{1}".format(value["quota"], value["discount"]))
                    except Exception, error:
                        logger.debug("*** ads_skucoupon key error : {0}".format(error))
                if ads_quan and "title" in ads_quan:
                    promotion["满额返劵"] = [ads_quan["title"]]

                for index, key in enumerate(promotion.keys()):
                    message = ""
                    for value in promotion[key]:
                        if message != "":
                            message += ", "
                        message += value
                    if message == "":
                        continue
                    if meta["promotions"] != "":
                        meta["promotions"] += "###"
                    meta["promotions"] += "{0}##{1}".format(key, message)
                # for index, adi in enumerate(ads_pro_pick):
                #     try:
                #         message = "{0}: {1}".format(adi["name"], adi["content"])
                #     except Exception, error:
                #         logger.debug("*** ads_pro_pick key error : {0}".format(error))
                #         continue
                #     meta["promotions"] += message
                #     if index != len(ads_pro_pick) - 1:
                #         meta["promotions"] += " ; "
                # if meta["promotions"] != "" and len(ads_pro_tag) != 0:
                #     meta["promotions"] += " ; "
                # for index, adi in enumerate(ads_pro_tag):
                #     try:
                #         message = "{0}: {1}".format(adi["name"], adi["content"])
                #     except Exception, error:
                #         logger.debug("*** ads_pro_tag key error : {0}".format(error))
                #         continue
                #     meta["promotions"] += message
                #     if index != len(ads_pro_tag) - 1:
                #         meta["promotions"] += " ; "
                # if ads_quan and "title" in ads_quan:
                #     if meta["promotions"] != "":
                #         meta["promotions"] += " ; "
                #     meta["promotions"] += "满额返劵: " + ads_quan["title"]
            except Exception, error:
                logger.debug("JD promotion json error : {0}".format(error))
                meta["promotions"] = analy_re_findall(response.body, ['"content":"(.*?)"'])
                if meta["promotions"] is None:
                    meta["promotions"] = ""
                meta["promotions"] += analy_re_findall(response.body, ['"title":"(.+?)"'])
                meta["promotions_title"] = analy_re_search(response.body, '"ad":"([\s\S]+?)["<]')

            yield Request(
                "http://item.m.jd.com/product/{0}.html".format(meta["pid"]),
                meta=meta, dont_filter=True, callback=self.parseJD_price_mb
            )
        except Exception, e:
            self.log('parseJDtitle : {0}  {1}'.format(meta["url"], str(e)), level=logging.ERROR)
            for c in cs:
                meta["city"] = c
                item = self.update_item(meta)
                yield item

    def parseJD_price_mb(self, response):
        meta = response.meta
        try:
            meta["price_mobile"] = analy_re_search(response.body, 'name="jdPrice" value="([\d\.]+)"')

            yield Request(
                "http://p.3.cn/prices/get?type=1&area=1_72_2799&pdtk=&pdpin=&pdbp=0&skuid=J_{0}&callback=cnp".format(
                    meta["pid"]),
                meta=meta, dont_filter=True, callback=self.parseJD_price_web
            )
        except Exception, e:
            self.log('parseJDpricemb : {0}  {1}'.format(meta["url"], str(e)), level=logging.ERROR)
            for c in cs:
                meta["city"] = c
                item = self.update_item(meta)
                yield item

    def parseJD_price_web(self, response):
        meta = response.meta
        try:
            re_price = re.search(r'"p":"(?P<pp>.*?)"', response.body)
            if re_price is None:
                raise Exception, "price is error !"

            meta["price_web"] = re_price.group('pp')
            if "-" in meta["price_web"]:
                with open("error.txt", "ab") as fp:
                    fp.write("prices - : {0} : {1}\n".format(meta["url"], response.meta))

            if meta["shopname"]:
                meta["shopname"] = meta["shopname"].decode("utf8").encode("utf8")
            meta["title"] = meta["title"].decode("utf8").encode("utf8")
            meta["promotions"] = meta["promotions"]#.decode("gbk").encode("utf8")
            meta["promotions_title"] = meta["promotions_title"]#.decode("gbk").encode("utf8")
        except Exception, e:
            self.log('parseJDpriceweb : {0}  {1}'.format(meta["url"], str(e)), level=logging.ERROR)
        for c in cs:
            meta["city"] = c
            item = self.update_item(meta)
            yield item
        logger.info("end JD : {0}".format(meta["url"]))

    def parseYMX(self, response):
        meta = response.meta["item_data"]
        logger.info("begin YMX : {0}".format(meta["url"]))
        try:
            analy_error(meta, response, "YMX")
            # if int(meta["tp"]) < 0:
            #     raise Exception, "_error {0}".format(meta["tp"])

            analy_meta(meta, response, citys["YMX"])

            content = analy_re_search(response.body, ['\&quot;sims-fbt\&quot;.+?>([\s\S]*?)</script>'], israise=False)
            try:
                result = json.loads(content)["itemDetails"]

                for key, value in result.items():
                    if "asin" in value and value["asin"] == meta["pid"] and "price" in value:
                        meta["price_web"] = value["price"]
                        break

            except Exception, e:
                pass

            yield Request("https://www.amazon.cn/gp/aw/d/{0}/ref=mp_s_a_1_1".format(meta["pid"]),
                          meta=meta,
                          dont_filter=True,
                          callback=self.parseYMX_price_mb,
                          )
            # promotion_url = analy_re_search(response.body,
            #                                 '(/gp/collect-coupon/handler/get_more_coupons.html.+?)&quot;')
            # if promotion_url:
            #     promotion_url = "https://www.amazon.cn" + promotion_url
            #     promotion_url = _sub(promotion_url, [['&amp;', '&'], ])
            #     yield Request(promotion_url,
            #                   meta=meta,
            #                   dont_filter=True,
            #                   callback=self.parseYMX_promotion,
            #                   )
        except Exception, e:
            self.log('parseYMX : {0}  {1}'.format(meta["url"], str(e)), level=logging.ERROR)
            for c in cs:
                meta["city"] = c
                item = self.update_item(meta)
                yield item

    def parseYMX_promotion(self, response):
        meta = response.meta
        try:
            datas = response.xpath('//span[@class="apl_m_font"]/text()').extract()
            meta["promotions"] = append_data(datas)

            yield Request("https://www.amazon.cn/gp/aw/d/{0}/ref=mp_s_a_1_1".format(meta["pid"]),
                          meta=meta,
                          dont_filter=True,
                          callback=self.parseYMX_price_mb,
                          )
        except Exception, e:
            self.log('parseYMX_Promotion : {0}  {1}'.format(meta["url"], str(e)), level=logging.ERROR)
            for c in cs:
                meta["city"] = c
                item = self.update_item(meta)
                yield item

    def parseYMX_price_mb(self, response):
        meta = response.meta
        try:
            meta["price_mobile"] = analy_re_search(response.body, 'class="dpOurPrice">[\s\S]*?([\d\.,]+)\s*</span>')
        except Exception, e:
            self.log('parseYMXpricemb : {0}  {1}'.format(meta["url"], str(e)), level=logging.ERROR)

        for c in cs:
            meta["city"] = c
            item = self.update_item(meta)
            yield item
        logger.info("end YMX : {0}".format(meta["url"]))

    def parseYHD(self, response):
        meta = response.meta["item_data"]
        logger.info("begin YHD : {0}".format(meta["url"]))
        try:
            analy_error(meta, response, "YHD")
            # if int(meta["tp"]) < 0:
            #     raise Exception, "_error {0}".format(meta["tp"])

            analy_meta(meta, response, citys["YHD"])

            productid = analy_re_search(response.body, ['id="mainProductId" value="(\d+)"', 'productId:(\d+),', '"productId","(\d+)"'], raisemsg="productID")
            merchantid = analy_re_search(response.body, ['id="merchantId" value="(.+?)"', 'merchantId:(.+?),'], raisemsg="merchantID")
            promerid = analy_re_search(response.body, ['id="productMercantId" value="(.+?)"', 'pmId:(.+?),'], raisemsg="productMercantID")
            categoryid = analy_re_search(response.body, ['id="categoryId" value="(.+?)"', ], raisemsg="categoryId")
            brandid = analy_re_search(response.body, ['id="brandID" value="(.+?)"', ], raisemsg="brandID")
            isyhd = analy_re_search(response.body, ['id="isYiHaoDian" value="(.+?)"', 'isYiHaoDian:(.+?),'], raisemsg="isYihaodian")
            uid = analy_re_search(response.body, ['paramSignature:"(.+?)"'], raisemsg="uid")

            promotion_url = "http://item.yhd.com/item/ajax/ajaxProductPromotion.do"
            promotion_url += "?productID={0}&merchantID={1}&productMercantID={2}&categoryId={3}".format(productid, merchantid, promerid, categoryid)
            promotion_url += "&brandId={0}&isYihaodian={1}&uid={2}&version=version_c".format(brandid, isyhd, uid)
            # print promotion_url

            yield Request(promotion_url, callback=self.parseYHD_promotion, dont_filter=True, meta=meta,
                          headers = {"Host": "pms.yhd.com",
                                     "Connection": "keep-alive",
                                     "Referer": meta["url"],
                                     "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:47.0) Gecko/20100101 Firefox/47.0",
                                     }
                          )
        except Exception, e:
            self.log('parseYHD : {0}  {1}'.format(meta["url"], str(e)), level=logging.ERROR)
            for c in cs:
                meta["city"] = c
                item = self.update_item(meta)
                yield item

    def parseYHD_promotion(self, response):
        meta = response.meta
        try:
            content = analy_re_search(response.body, ['"value":"([\s\S]+)"\}'], israise=False)
            if content and content == "":
                meta["promotions"] = analy_re_findall(response.body, ['"promotionDes":"(.*?)"'])
            else:
                meta["promotions"] = ""
                # Selector(text=content)
                promotions = re.findall('(<li[\s\S]+?/li>)', content)
                for index, promi in enumerate(promotions):
                    prom = _sub(promi, [('<[\s\S]+?>', ' '), (r'\\', ''), ('[rtn]', ''), ('\s+', ' ')])
                    meta["promotions"] += prom.strip()
                    if index != len(promotions) - 1:
                        meta["promotions"] += " ; "

            yield Request("http://item.m." + analy_re_search(meta["url"], 'http://item\.(.+)'),
                          callback=self.parseYHD_price_mb,
                          dont_filter=True,
                          meta=meta
                          )
        except Exception, e:
            self.log('parseYHD_Promotion : {0}  {1}'.format(meta["url"], str(e)), level=logging.ERROR)
            for c in cs:
                meta["city"] = c
                item = self.update_item(meta)
                yield item

    def parseYHD_price_mb(self, response):
        meta = response.meta
        try:
            meta["price_mobile"] = analy_re_search(response.body, 'currentPrice:(\d+)')

            for c in cs:
                url = "http://gps.yhd.com/restful/detail?mcsite=1{0}&pmId={1}".format(citys["YHD"]["web"][c], meta["pid"])
                yield Request(url, callback=self.parseALL_price_web,
                              dont_filter=True,
                              meta={"meta": meta,
                                    "city": c,
                                    "website": "YHD"
                                    }
                              )
        except Exception, e:
            self.log('parseYHDpricemb : {0}  {1}'.format(meta["url"], str(e)), level=logging.ERROR)
            for c in cs:
                meta["city"] = c
                item = self.update_item(meta)
                yield item
        logger.info("end YHD : {0}".format(meta["url"]))

    def parseSN(self, response):
        meta = response.meta["item_data"]
        logger.info("begin SN : {0}".format(meta["url"]))
        try:
            analy_error(meta, response, "SN")
            # if int(meta["tp"]) < 0:
            #     raise Exception, "_error {0}".format(meta["tp"])

            analy_meta(meta, response, citys["SN"])
            meta["vendorid"] = analy_re_search(response.body, ['"vendorCode":"(.*?)"'], israise=False)
            if meta["vendorid"] == "":
                meta["vendorid"] = "0000000000"

            # 145278423 000000000
            if len(str(meta["pid"][1])) == 0:
                price_url1 = "http://icps.suning.com/icps-web/getAllPriceFourPage/000000000{0}_{1}_".format(
                    meta["pid"][0], meta["pid"][1])
                price_url2 = "_1_pc_showSaleStatus.vhtm?callback=showSaleStatus"
            else:
                price_url1 = "http://pas.suning.com/nspcsale_0_000000000{0}_000000000{0}_{1}_10_".format(
                    meta["pid"][1], meta["vendorid"])
                price_url2 = "_20358_1000000_9017_10107_Z001.html?callback=pcData"

            if len(str(meta["pid"][1])) == 0:
                meta["m_url"] = "http://pas.suning.com/nsitemsale_000000000{0}_{1}_10_010_0100101_0_5__1.html?callback=wapData".format(
                    meta["pid"][0], meta["pid"][1])
            else:
                meta["m_url"] = "http://pas.suning.com/nsitemsale_000000000{0}_{1}_10_010_0100101_0_5__1.html?callback=wapData".format(
                    meta["pid"][1], meta["pid"][0])

            meta["price_url1"] = price_url1
            meta["price_url2"] = price_url2

            if len(str(meta["pid"][1])) == 0:
                pro_sale_url = "http://pas.suning.com/nspcsale_0_000000000{0}_000000000{0}_0000000000_10_010_0100101_20358_1000000_9017_10106_Z001.html".format(
                    meta["pid"][0])
            else:
                pro_sale_url = "http://pas.suning.com/nspcsale_0_000000000{0}_000000000{0}_0000000000_10_010_0100101_20358_1000000_9017_10106_Z001.html".format(
                    meta["pid"][1])

            # print pro_sale_url
            yield Request(pro_sale_url, meta=meta, dont_filter=True, callback=self.parseSN_promotion_sale)
            # yield Request(meta["price_url1"] + "010_0100101" + meta["price_url2"],
            #               meta=meta, dont_filter=True, callback=self.parseSN_promotion_type)
        except Exception, e:
            self.log('parseSN : {0}  {1}'.format(meta["url"], str(e)), level=logging.ERROR)
            for c in cs:
                meta["city"] = c
                item = self.update_item(meta)
                yield item

    def parseSN_promotion_sale(self, response):
        meta = response.meta
        try:
            print '"invStatus":"1"' in response.body
            if '"invStatus":"1"' in response.body or True:
                yield Request(
                    meta["price_url1"] + "010_0100101" + meta["price_url2"],
                    meta=meta,
                    dont_filter=True,
                    callback=self.parseSN_promotion_type,
                )
            else:
                yield Request(meta["m_url"], meta=meta, dont_filter=True, callback=self.parseSN_price_mb)
        except Exception, e:
            self.log('parseSN_promotion_sale : {0}  {1}'.format(meta["url"], str(e)), level=logging.ERROR)
            for c in cs:
                meta["city"] = c
                item = self.update_item(meta)
                yield item

    def parseSN_promotion_type(self, response):
        meta = response.meta
        try:
            pro_type = analy_re_search(response.body, ['"priceType":"(.+?)"'])

            if len(str(meta["pid"][1])) == 0:
                pro_url = "http://icps.suning.com/icps-web/getPromotionFourPage/000000000{0}_{1}_731_7310101_412.00_".format(
                    meta["pid"][0], meta["pid"][1])
            else:
                pro_url = "http://icps.suning.com/icps-web/getPromotionFourPage/000000000{0}_{1}_731_7310101_412.00_".format(
                    meta["pid"][1], meta["vendorid"])
            pro_url += pro_type + "_11_1_3_pds_FourPage.promInfoCallback.vhtm"
            print pro_url
            yield Request(pro_url, meta=meta, dont_filter=True, callback=self.parseSN_promotion)
        except Exception, e:
            self.log('parseSN_Promotion_type : {0}  {1}'.format(meta["url"], str(e)), level=logging.ERROR)
            for c in cs:
                meta["city"] = c
                item = self.update_item(meta)
                yield item

    def parseSN_promotion(self, response):
        meta = response.meta
        # with open("content.txt", "wb") as fp:
        #     fp.write(response.body)
        try:
            meta["promotions"] = analy_re_findall(response.body, ['"activityDesc":"(.*?)"'], israise=False)
            yield Request(meta["m_url"], meta=meta, dont_filter=True, callback=self.parseSN_price_mb)
        except Exception, e:
            self.log('parseSN_Promotion : {0}  {1}'.format(meta["url"], str(e)), level=logging.ERROR)
            for c in cs:
                meta["city"] = c
                item = self.update_item(meta)
                yield item

    def parseSN_price_mb(self, response):
        meta = response.meta
        try:
            meta["price_mobile"] = analy_re_search(response.body, ['"promotionPrice":"([\d\.]+)"', '"gbPrice":"([\d\.]+)"'], israise=False)

            for c in cs:
                yield Request(meta["price_url1"] + citys["SN"]["web"][c] + meta["price_url2"],
                              callback=self.parseALL_price_web,
                              dont_filter=True,
                              headers={"Host": "icps.suning.com",
                                       "If-Modified-Since": time.strftime("%Y-%m-%d %H:%M:%S"),
                                       "Referer": meta["url"],
                                       "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:46.0) Gecko/20100101 Firefox/46.0"
                                       },
                              meta={"meta": meta,
                                    "city": c,
                                    "website": "SN"
                                    }
                              )
        except Exception, e:
            self.log('parseSNpricemb : {0}  {1}'.format(meta["url"], str(e)), level=logging.ERROR)
            for c in cs:
                meta["city"] = c
                item = self.update_item(meta)
                yield item
        logger.info("end SN : {0}".format(meta["url"]))

    def parseTM(self, response):
        meta = response.meta["item_data"]
        logger.info("begin TM : {0}".format(meta["url"]))
        try:
            analy_error(meta, response, "TM")
            # if int(meta["tp"]) < 0:
            #     raise Exception, "_error {0}".format(meta["tp"])

            analy_meta(meta, response, citys["TM"])
            if meta["pid"] is None or meta["pid"] == "":
                meta["pid"] = analy_re_search(response.body, ['"skuId":"(\d+)"'])

            promotion_url = "https:" + analy_re_search(response.body, ['"initApi":"(.+?)"']) + "&callback=setMdskip"
            promotion_url += "&timestamp=1465714651296&isg=Av39jBQcYXf-vcF/Rjd-WfQuLVf3mjHs&areaId=110100&cat_id=2"
            meta1 = {"meta": meta,
                     "price_url": "https:" + analy_re_search(response.body, [
                         '"initApi":"(//mdskip.taobao.com/core/initItemDetail.htm.+?)"']),
                     "price_url1": "https://mdskip.taobao.com/core/changeLocation.htm?queryDelivery=true&queryProm=true&tmallBuySupport=true&ref=",
                     "price_url2": "&_ksTS=1464747454304_2005&callback=jsonp2006&" + analy_re_search(response.body,
                                                                                                'changeLocation.htm\?(.+?)","'),
                     "price_urlm": analy_re_search(response.url, ['(item.htm\?.+skuId=\d+)', '(item.htm\?.+)']),
                     }
            # print promotion_url
            yield Request(promotion_url, meta=meta1, dont_filter=True, callback=self.parseTM_promotion)
        except Exception, e:
            self.log('parseTM : {0}  {1}'.format(meta["url"], str(e)), level=logging.ERROR)
            for c in cs:
                meta["city"] = c
                item = self.update_item(meta)
                yield item

    def parseTM_promotion(self, response):
        meta = response.meta["meta"]
        meta1 = response.meta
        # with open("content.txt", "wb") as fp:
        #     fp.write(response.body)
        try:
            meta["promotions"] = analy_re_findall(response.body, ['"msg":"(.*?)"', '"txt":"(.*?)"'], isapp=True)
            meta["promotions"] = meta["promotions"].strip().decode("gbk")
            meta1["meta"] = meta

            yield Request("https://detail.m.tmall.com/" + meta1["price_urlm"],
                          meta=meta1, dont_filter=True, callback=self.parseTM_price_mb,
                          )
        except Exception, e:
            self.log('parseTM_Promotion : {0}  {1}'.format(meta["url"], str(e)), level=logging.ERROR)
            for c in cs:
                meta["city"] = c
                item = self.update_item(meta)
                yield item

    def parseTM_price_mb(self, response):
        meta = response.meta["meta"]
        try:
            content = analy_re_search(
                response.body.decode("gbk").encode("utf8"),
                ['var _DATA_Mdskip = \s*([\s\S]+?)</script>'],
                israise=False
            )
            result = self.analy_tm(content, meta)
            if "price_original" in result:
                meta["price_mobile"] = result["price_original"]
            if "price" in result:
                meta["price_mobile"] = result["price"]
        except Exception, e:
            self.log('parseTMpricemb : {0}  {1}'.format(meta["url"], str(e)), level=logging.ERROR)

        try:
            for c in cs:
                yield Request(response.meta["price_url1"] + citys["TM"]["web"][c] + response.meta["price_url2"],
                              headers={"Accept": "*/*",
                                       "Referer": response.url,
                                       "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko",
                                       "Cookie": "cookie2=1cd7c737de3918441ecb3b40d4612fe9; t=1889a58fa3474963fd27f13bd25361fb;"
                                       },
                              meta={"meta": meta,
                                    "city": c,
                                    "website": "TM",
                                    },
                              dont_filter=True,
                              callback=self.parseALL_price_web
                              )
        except Exception, e:
            self.log('parseTMpricemb2 : {0}  {1}'.format(meta["url"], str(e)), level=logging.ERROR)
            for c in cs:
                meta["city"] = c
                item = self.update_item(meta)
                yield item
        logger.info("end TM : {0}".format(meta["url"]))

    def parseDD(self, response):
        meta = response.meta["item_data"]
        logger.info("begin DD : {0}".format(meta["url"]))
        try:
            analy_error(meta, response, "DD")
            # if int(meta["tp"]) < 0:
            #     raise Exception, "_error {0}".format(meta["tp"])
            analy_meta(meta, response, citys["DD"])

            meta["shopid"] = analy_re_search(response.body, ['shopId":"(.+?)"'], israise=False)

            yield Request("http://product.m.dangdang.com/product.php?pid={0}".format(meta["pid"]),
                          meta = meta,
                          dont_filter=True,
                          callback=self.parseDD_price_mb
                          )
        except Exception, e:
            self.log('parseDD : {0}  {1}'.format(meta["url"], str(e)), level=logging.ERROR)
            for c in cs:
                meta["city"] = c
                item = self.update_item(meta)
                yield item

    def parseDD_price_mb(self, response):
        meta = response.meta
        try:
            meta["price_mobile"] = analy_xpath(response, ['//span[@id="main_price"]/text()'])
            if meta["price_mobile"] is None or meta["price_mobile"] == "":
                meta["price_mobile"] = analy_re_search(response.body, ['"rank_price":"(.+?)"'])

            if "product.dangdang" in meta["url"]:   price_url = "http://product.dangdang.com/"
            else:   price_url = "http://detail.dangdang.com/"
            price_url += "?r=callback%2Fproduct-info&productId={0}&isCatalog=0&shopId={1}".format(meta["pid"], meta["shopid"])
            print price_url
            yield Request(price_url, meta = meta, dont_filter=True, callback=self.parseDD_price_web)
        except Exception, e:
            self.log('parseDDpricemb : {0}  {1}'.format(meta["url"], str(e)), level=logging.ERROR)
            for c in cs:
                meta["city"] = c
                item = self.update_item(meta)
                yield item

    def parseDD_price_web(self, response):
        meta = response.meta
        # with open("content.txt", "wb") as fp:
        #     fp.write(response.body)
        try:
            result = json.loads(response.body)
            resultspu = result["data"]["spu"]

            price = None
            try:
                if str(resultspu["preSale"]["isPreSale"]) == "1":
                    for p in resultspu["preSale"]["rankList"]:
                        price = p["price"]
            except Exception, error:
                logger.info("ispresale is error : {0} {1}".format(meta["url"], result))

            try:
                price = analy_re_search(str(resultspu["promotion"]), ["'directPrice': u'(.+?)'"], israise=False)
                if price == "":
                    price = None
            except:
                logger.info("spupromotion is error : {0} {1}".format(meta["url"], result))

            try:
                if price is None:
                    price = resultspu["price"]["salePrice"]
            except:
                logger.error("saleprice is error : {0} {1}".format(meta["url"], result))

            if price:
                meta["price_web"] = price
        except Exception, e:
            self.log('parseDDpriceweb : {0}  {1}'.format(meta["url"], str(e)), level=logging.ERROR)

        for c in cs:
            meta["city"] = c
            item = self.update_item(meta)
            yield item
        logger.info("end DD : {0}".format(meta["url"]))

    def parseGM(self, response):
        meta = response.meta["item_data"]
        logger.info("begin GM : {0}".format(meta["url"]))
        try:
            analy_error(meta, response, "GM")
            # if int(meta["tp"]) < 0:
            #     raise Exception, "_error {0}".format(meta["tp"])

            analy_meta(meta, response, citys["GM"])
            promotion_type_url = "http://ss.gome.com.cn/item/v1/d/reserve/p/detail/{0}/{1}/null/11010000/flag/item/userStores".format(
                meta["pid"][0], meta["pid"][1])
            # print promotion_type_url
            yield Request(promotion_type_url, meta=meta, dont_filter=True, callback=self.parseGM_promotions_type)

        except Exception, e:
            self.log('parseGM : {0}  {1}'.format(meta["url"], str(e)), level=logging.ERROR)
            for c in cs:
                meta["city"] = c
                item = self.update_item(meta)
                yield item

    def parseGM_promotions_type(self, response):
        meta = response.meta
        meta["visible_pro"] = True
        try:
            if '没有找到预约抢购信息' not in response.body:#'"visible":false' in response.body:
                meta["visible_pro"] = False
        except Exception, e:
            self.log('parseGM_promotions_type : {0} {1}'.format(meta["url"], e), level=logging.ERROR)

        promotion_url = "http://ss.gome.com.cn/item/v1/d/m/store/{0}/{1}/N/12010200/120102002/null/flag/item/allStore".format(
            meta["pid"][0], meta["pid"][1])
        # print promotion_url
        # print meta["visible_pro"]
        yield Request(promotion_url, meta=meta, dont_filter=True, callback=self.parseGM_promotions)

    def parseGM_promotions(self, response):
        meta = response.meta
        # with open("content.txt", "wb") as fp:
        #     fp.write(response.body)
        try:
            try:
                if meta["visible_pro"]:
                    content = analy_re_search(response.body, '"promArray":([\s\S]+)')
                    if content:
                        meta["promotions"] = re.sub('<.+?>', '', analy_re_findall(content, ['"desc":"(.*?)"', '"description":"(.*?)"'])).strip()
                else:
                    meta["promotions"] = ""
                meta["promotions_title"] = re.sub('<.+?>', '', analy_re_search(response.body, '"desc":"(.+?)","')).strip()
                price_web = analy_re_search(response.body, '"salePrice":"(.*?)"')
                meta["price_web"] = price_web if price_web != "" else meta["price_web"]
            except:
                pass

            # "http://m.gome.com.cn/pro-inventorynnquiry-{0}-{1}-11000000-11010000-11010200-110102001.html?level=5".format(
            #     meta["pid"][0], meta["pid"][1]),
            # murl = "http://item.m.gome.com.cn/product-{0}-{1}.html".format(meta["pid"][0], meta["pid"][1])
            murl = "http://item.m.gome.com.cn/product/stock?goodsNo={0}&skuID={1}&shopId=&shopType=0".format(meta["pid"][0], meta["pid"][1])
            murl += "&provinceId=11000000&cityId=11010000&districtId=11010200&townId=110102002&modelId=&fei=0&ajax=1"
            # print murl
            yield Request(murl, meta=meta, dont_filter=True, callback=self.parseGM_price_mb)
        except Exception, e:
            self.log('parseGMpromotions : {0}  {1}'.format(meta["url"], str(e)), level=logging.ERROR)
            for c in cs:
                meta["city"] = c
                item = self.update_item(meta)
                yield item

    def parseGM_price_mb(self, response):
        meta = response.meta
        # with open("content.txt", "wb") as fp:
        #     fp.write(response.body)
        try:
            meta["price_mobile"] = analy_re_search(response.body, '"skuPrice":"([\d\.]+)"')
        except Exception, e:
            self.log('parseGMpricemb : {0}  {1}'.format(meta["url"], str(e)), level=logging.ERROR)
        for c in cs:
            meta["city"] = c
            item = self.update_item(meta)
            yield item
        logger.info("end GM : {0}".format(meta["url"]))

    def parseTB(self, response):
        meta = response.meta["item_data"]
        logger.info("begin TB : {0}".format(meta["url"]))
        try:
            analy_error(meta, response, "TB")
            if int(meta["tp"]) < 0:
                raise Exception, "_error {0}".format(meta["tp"])

            analy_meta(meta, response, citys["TB"])

            count_url = analy_re_search(response.body, ["counterApi\s+:\s*'(.+?)'"])
            price_url = analy_re_search(response.body, ['sibUrl\s*:\s*\'(.+?)\''], israise=True)
            price_url = "https:" + price_url + "&callback=onSibRequestSuccess"

            if count_url and count_url != "":
                count_url = "https:" + count_url
                yield Request(count_url, meta = {"meta": meta,
                                                 "price_url": price_url
                                                 },
                              dont_filter=True, callback=self.parseTB_count
                              )

        except Exception, e:
            self.log('parseTB : {0}  {1}'.format(meta["url"], str(e)), level=logging.ERROR)
            for c in cs:
                meta["city"] = c
                item = self.update_item(meta)
                yield item

    def parseTB_count(self, response):
        meta = response.meta["meta"]
        try:


            price_url = response.meta["price_url"]
            if price_url == "":
                raise Exception, "price_url is "" !"

            yield Request(price_url, meta=meta, dont_filter=True, callback=self.parseTB_price_web)
        except Exception, e:
            self.log('parseTB_count : {0} {1}'.format(meta["url"], str(e), level=logging.ERROR))
            for c in cs:
                meta["city"] = c
                item = self.update_item(meta)
                yield item

    def parseTB_price_web(self, response):
        meta = response.meta
        # with open("content.txt", "wb") as fp:
        #     fp.write(response.body)
        try:
            content = analy_re_search(response.body, ['onSibRequestSuccess\(([\s\S]+)\);'], israise=True)
            result = json.loads(content)
            price = None
            price_original = result["data"]["price"]
            result_p = result["data"]["promotion"]["promoData"]

            for key in result_p.keys():
                try:
                    pid = str(int(key))
                    result_p = result_p[pid]
                    break
                except:
                    continue
            else:
                result_p = result_p["def"]

            for res in result_p:
                try:
                    price = res["prices"]
                    break
                except:
                    pass
            else:
                price = price_original

            if meta["price_web"] is None or meta["price_web"] == "":
                meta["price_web"] = price
            meta["price_original"] = price_original

        except Exception, e:
            self.log('parseTB_prices_web : {0}  {1}'.format(meta["url"], str(e)), level=logging.ERROR)

        meta["price_mobile"] = meta["price_web"]

        for c in cs:
            meta["city"] = c
            item = self.update_item(meta)
            yield item
        logger.info("end TB : {0}".format(meta["url"]))