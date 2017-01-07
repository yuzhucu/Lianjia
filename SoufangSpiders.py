# -*- coding:utf-8 -*-
############################################################################
# 程序：上海搜房网爬虫
# 功能：抓取上海搜房网二手房在售、成交数据
# 创建时间：2017/01/03
# 更新历史：2017/01/07 增加多城市处理、随机Header
# 使用库：requests、BeautifulSoup4、MySQLdb
# 作者：yuzhucu
#############################################################################
import requests
from bs4 import BeautifulSoup
import lxml
import time
import random
import MySQLdb

def getCurrentTime():
    # 获取当前时间
    return time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time()))

def getURL(url, tries_num=50, sleep_time=0, time_out=10):
    # header = {'content-type': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    #            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.75 Safari/537.36',
    #            "Host": "esf.sh.fang.com"}
    # proxy ={ "http": "110.11.12.13:8080", "https": "http://110.11.12.13:8080" }
    sleep_time_p = sleep_time
    time_out_p = time_out
    tries_num_p = tries_num
    try:
        res = requests.Session()
        if isproxy == 1:
            res = requests.get(url, headers=header, timeout=time_out, proxies=proxy)
        else:
            res = requests.get(url, headers=header, timeout=time_out)
        res.raise_for_status()  # 如果响应状态码不是 200，就主动抛出异常
    except requests.RequestException as e:
        sleep_time_p = sleep_time_p + 10
        time_out_p = time_out_p + 10
        tries_num_p = tries_num_p - 1
        # 设置重试次数，最大timeout 时间和 最长休眠时间
        # print tries_num_p
        if tries_num_p > 0:
            time.sleep(sleep_time_p)
            print getCurrentTime(), url, 'URL Connection Error: 第', max_retry - tries_num_p, u'次 Retry Connection', e
            return getURL(url, tries_num_p, sleep_time_p, time_out_p)
    # if res.status_code == 200:
    #             print getCurrentTime(), url, 'URL Connection Success: 共尝试', max_retry - tries_num_p, u'次', ',sleep_time:', sleep_time_p, ',time_out:', time_out_p
    #         else:
    #             print getCurrentTime(), url, 'URL Connection Error: 共尝试', max_retry - tries_num_p, u'次', ',sleep_time:', sleep_time_p, ',time_out:', time_out_p
    return res

def getSoufangList(fang_url, args):
    base_url = args['base_url']
    result = {}
    res = getURL(fang_url)
    res.encoding = 'gbk'
    soup = BeautifulSoup(res.text, 'html.parser')
    for fang in soup.find_all("dd", class_="info rel floatr"):
        try:
            result['fang_key'] = fang.select('p')[0].a['href'].strip()
            result['city'] = args['city']
            result['quyu'] = args['region']
            result['bankuai'] = args['subRegion']
            result['fang_desc'] = fang.select('p')[0].text.strip()
            result['fang_url'] = base_url + fang.select('p')[0].a['href'].strip()
            result['huxing'] = fang.select('p')[1].contents[0].strip()
            result['louceng'] = fang.select('p')[1].contents[2].strip()
            result['chaoxiang'] = fang.select('p')[1].contents[4].strip()
            try:
                result['age'] = fang.select('p')[1].contents[6].strip()
            except Exception as e:
                # 建筑年代和朝向可能缺失，如缺失先放一样
                result['age'] = result['chaoxiang']
                print getCurrentTime(), u"Exception:%s" % (e.message), result['fang_url'], result[
                    'fang_desc'], 'chaoxiang:', result['chaoxiang'], 'age:', result['age']
            result['xiaoqu'] = fang.select('p')[2].span.text.strip()
            result['address'] = fang.find('span', 'iconAdress ml10 gray9').get_text()
            # result['subway']=fang.find('span','train note').text.strip()
            # result['subway']=fang.select('div.mt8.clearfix > div.pt4.floatl > span.train.note')[0].text
            # result['subway']=fang.select('div')[0].text.strip()
            result['mianji'] = fang.find('div', 'area alignR').get_text().strip()
            # result['mianji'] = fang.find('div', 'area alignR').get_text().encode("utf-8").strip().strip('建筑面积').strip()
            mianji = fang.find('div', 'area alignR').get_text().encode("utf-8").strip().strip('建筑面积').strip()
            # result['mianji']=mianji
            # print  type( result['mianji']),type(mianji),type(fang.find('div', 'area alignR').get_text().encode("utf-8").strip().strip('建筑面积').strip())
            result['price'] = fang.find('span', 'price').get_text().strip()
            result['price_pre'] = fang.select('div.moreInfo > p.danjia.alignR.mt5')[0].get_text().strip()
            # print fang.select('div.moreInfo > p.danjia.alignR.mt5')[0].get_text().strip()
            result['data_source'] = 'Soufang'
            result['updated_date'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            mySQL.insertData('soufang_fang_list', result)
            # print result
            print getCurrentTime(), u'在售：', result['quyu'], result['bankuai'], result['xiaoqu'], result['address'], \
                result['huxing'], result['louceng'], result['chaoxiang'], result['price'], u'万', result[
                'price_pre'], mianji, result['fang_desc']
        except Exception as e:
            print  getCurrentTime(), u"Exception:%s" % (e.message), result['fang_url'], result['fang_desc']
    return result

def getRegions(fang_url):
    res = getURL(fang_url)
    res.encoding = 'gbk'
    soup = BeautifulSoup(res.text, 'html.parser')
    result = []
    gio_district = soup.find('div', class_="qxName")
    try:
        for link in gio_district.find_all('a'):
            district = {}
            district['code'] = link.get('href')
            district['name'] = link.get_text()
            # print  district['code'],district['name']
            if district['name'] <> u'\u4e0d\u9650':
                result.append(district)
    except  Exception, e:
        print  getCurrentTime(), 'getRegions', fang_url, u"Exception:%s" % (e.message)
        return result
    return result

def getSubRegions(fang_url):
    res = getURL(fang_url)
    res.encoding = 'gbk'
    soup = BeautifulSoup(res.text, 'html.parser')
    result = []
    gio_plate = soup.find('p', id="shangQuancontain")
    try:
        for link in gio_plate.find_all('a'):
            district = {}
            district['code'] = link.get('href')
            district['name'] = link.get_text()
            # print  district['code'],district['name']
            if district['name'] <> u'\u4e0d\u9650':
                result.append(district)
    except Exception, e:
        print  getCurrentTime(), 'getSubRegions', fang_url, u"Exception:%s" % (e.message)
        return result
    return result

def randHeader():
    head_connection = ['Keep-Alive', 'close']
    head_accept = ['text/html, application/xhtml+xml, */*']
    head_accept_language = ['zh-CN,fr-FR;q=0.5', 'en-US,en;q=0.8,zh-Hans-CN;q=0.5,zh-Hans;q=0.3']
    head_user_agent = ['Opera/8.0 (Macintosh; PPC Mac OS X; U; en)',
                       'Opera/9.27 (Windows NT 5.2; U; zh-cn)',
                       'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Win64; x64; Trident/4.0)',
                       'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)',
                       'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.2; .NET4.0C; .NET4.0E)',
                       'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.2; .NET4.0C; .NET4.0E; QQBrowser/7.3.9825.400)',
                       'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0; BIDUBrowser 2.x)',
                       'Mozilla/5.0 (Windows; U; Windows NT 5.1) Gecko/20070309 Firefox/2.0.0.3',
                       'Mozilla/5.0 (Windows; U; Windows NT 5.1) Gecko/20070803 Firefox/1.5.0.12',
                       'Mozilla/5.0 (Windows; U; Windows NT 5.2) Gecko/2008070208 Firefox/3.0.1',
                       'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.12) Gecko/20080219 Firefox/2.0.0.12 Navigator/9.0.0.6',
                       'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.95 Safari/537.36',
                       'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; rv:11.0) like Gecko)',
                       'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:21.0) Gecko/20100101 Firefox/21.0 ',
                       'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Maxthon/4.0.6.2000 Chrome/26.0.1410.43 Safari/537.1 ',
                       'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.92 Safari/537.1 LBBROWSER',
                       'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.75 Safari/537.36',
                       'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/3.0 Safari/536.11',
                       'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko',
                       'Mozilla/5.0 (Macintosh; PPC Mac OS X; U; en) Opera 8.0'
                       ]
    result = {
        'Connection': head_connection[0],
        'Accept': head_accept[0],
        'Accept-Language': head_accept_language[1],
        'User-Agent': head_user_agent[random.randrange(0, len(head_user_agent))]
    }
    return result

class MySQL:
    # 获取当前时间
    def getCurrentTime(self):
        return time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time()))
    # 数据库初始化
    def _init_(self, ip, username, pwd, schema):
        try:
            self.db = MySQLdb.connect(ip, username, pwd, schema)
            print self.getCurrentTime(), u"MySQL DB Connect Success"
            self.cur = self.db.cursor()
        except MySQLdb.Error, e:
            print self.getCurrentTime(), u"MySQL DB Connect Error :%d: %s" % (e.args[0], e.args[1])
    # 插入数据
    def insertData(self, table, my_dict):
        try:
            self.db.set_character_set('utf8')
            cols = ', '.join(my_dict.keys())
            values = '"," '.join(my_dict.values())
            sql = "replace INTO %s (%s) VALUES (%s)" % (table, cols, '"' + values + '"')
            try:
                result = self.cur.execute(sql)
                insert_id = self.db.insert_id()
                self.db.commit()
                # 判断是否执行成功
                if result:
                    return insert_id
                else:
                    return 0
            except MySQLdb.Error, e:
                # 发生错误时回滚
                self.db.rollback()
                print self.getCurrentTime(), u"Data Insert Failed: %d: %s" % (e.args[0], e.args[1])
        except MySQLdb.Error, e:
            print self.getCurrentTime(), u"MySQLdb Error:%d: %s" % (e.args[0], e.args[1])

def getSoufangMain(url):
    regions = getRegions(url)
    regions.reverse()
    while regions:
        region = regions.pop()
        print getCurrentTime(), 'Region:', region['name'], ':', 'Scrapy Starting.....'
        time.sleep(sleep_time)
        subRegions = getSubRegions('http://esf.sh.fang.com' + str(region['code']))
        subRegions.reverse()
        while subRegions:
            try:
                subRegion = subRegions.pop()
                print getCurrentTime(), region['name'], ':', subRegion['name'], 'Scrapy Starting.....'
                time.sleep(sleep_time)
                for i in range(start_page, end_page):
                    args = {'region': region['name'], 'subRegion': subRegion['name'], 'city': 'Shanghai'}
                    fang_url = 'http://esf.sh.fang.com' + subRegion['code'] + 'i3' + str(i)
                    print getCurrentTime(), region['name'], ':', subRegion['name'], fang_url
                    time.sleep(sleep_time)
                    fang = getSoufangList(fang_url, args)
                    if len(fang) < 1:
                        print getCurrentTime(), region['name'], ':', subRegion[
                            'name'], u': getSoufangList Scrapy Finished'
                        break
                    print getCurrentTime(), region['name'], ':', subRegion['name'], 'Scrapy Finished'
            except Exception, e:
                print  getCurrentTime(), u"Exception:%s" % (e.message)
        print getCurrentTime(), region['name'], ':', 'Scrapy Finished'
    print getCurrentTime(), 'getSoufangMain Scrapy Success'

def getSoufangMutiCityMain(city):
    regions = getRegions(city['base_url'])
    regions.reverse()
    while regions:
        region = regions.pop()
        print getCurrentTime(), 'Region:', region['name'], ':', 'Scrapy Starting.....'
        time.sleep(sleep_time)
        subRegions = getSubRegions(city['base_url'] + str(region['code']))
        subRegions.reverse()
        while subRegions:
            try:
                subRegion = subRegions.pop()
                print getCurrentTime(), region['name'], ':', subRegion['name'], 'Scrapy Starting.....'
                # time.sleep(sleep_time)
                for i in range(start_page, end_page):
                    args = {'region': region['name'], 'subRegion': subRegion['name'], 'city': city['city'],
                            'base_url': city['base_url']}
                    fang_url = city['base_url'] + subRegion['code'] + 'i3' + str(i)
                    print getCurrentTime(), args['city'], args['region'], args['subRegion'], fang_url
                    #time.sleep(sleep_time)
                    fang = getSoufangList(fang_url, args)
                    if len(fang) < 1:
                        print getCurrentTime(), region['name'], ':', subRegion[
                            'name'], u': getSoufangList Scrapy Finished'
                        break
                    print getCurrentTime(), region['name'], ':', subRegion['name'], 'Scrapy Finished'
            except Exception, e:
                print  getCurrentTime(), u'Exception:%s' % (e.message)
        print getCurrentTime(), region['name'], ':', 'Scrapy Finished'
    print getCurrentTime(), 'getSoufangMain Scrapy Success'

def main():
    print getCurrentTime(), 'Main Scrapy Starting'
    global mySQL, start_page, end_page, sleep_time, isproxy, max_retry, proxy, header
    mySQL = MySQL()
    mySQL._init_('localhost', 'root', 'root', 'fang')
    # mySQL._init_('110.20.15.79', 'cyz', 'cyz', 'cyzdb')
    proxy = {"http": "http://110.37.84.147:8080", "https": "http://110.37.84.147:8080"}
    # header = {'content-type': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    #           'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.75 Safari/537.36',
    #           "Host": "esf.sh.fang.com"}
    header = randHeader()
    start_page = 1
    end_page = 101
    sleep_time = 0.1
    isproxy = 0  # 如需要使用代理，改为1，并设置代理IP参数 proxy
    max_retry = 50
    url = 'http://esf.sh.fang.com'
    cities = [{'base_url': 'http://esf.zz.fang.com', 'city': 'ZhengZhou'},
              {'base_url': 'http://esf.sh.fang.com', 'city': 'ShangHai'},
              {'base_url': 'http://esf.sz.fang.com', 'city': 'ShenZhen'},
              {'base_url': 'http://esf.gz.fang.com', 'city': 'GuangZhou'},
              {'base_url': 'http://esf.fang.com', 'city': 'BeiJing'}
              ]
    # getRegions(url)
    # getSubRegions(url)
    # getSoufangList(url)
    # getSoufangMain(url)
    for city in cities:
        getSoufangMutiCityMain(city)
if __name__ == "__main__":
    main()
