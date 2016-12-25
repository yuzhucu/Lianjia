# -*- coding:utf-8 -*-
############################################################################
# 程序：上海链家网爬虫
# 功能：抓取上海链家二手房在售、成交数据 ，大约各5万记录；小区2万多个
# 创建时间：2016/11/10
# 更新历史：2016/11/26
#          2016.11.27:增加地铁找房；更新区域参数；拆分模块 ，以便于单独调用
#          2016.12.06 加入多线程处理
# 使用库：requests、BeautifulSoup4、MySQLdb
# 作者：yuzhucu
#############################################################################
import requests
from bs4 import BeautifulSoup
import time
import MySQLdb
import urllib
import urllib2
import json
import cookielib
import re
import zlib
from threading import Thread
from Queue import Queue
from time import sleep
#登录，不登录不能爬取三个月之内的数据
#import LianJiaLogIn
def LianjiaLogin():
    #获取Cookiejar对象（存在本机的cookie消息）
    cookie = cookielib.CookieJar()
    #自定义opener,并将opener跟CookieJar对象绑定
    opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cookie))
    #安装opener,此后调用urlopen()时都会使用安装过的opener对象
    urllib2.install_opener(opener)


    home_url = 'http://sh.lianjia.com/'
    auth_url = 'https://passport.lianjia.com/cas/login?service=http%3A%2F%2Fsh.lianjia.com%2F'
    #auth_url='https://passport.lianjia.com/cas/login?service=http%3A%2F%2Fuser.sh.lianjia.com%2Findex'
    chengjiao_url = 'http://sh.lianjia.com/chengjiao/'


    headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, sdch',
    'Accept-Language': 'zh-CN,zh;q=0.8',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Host': 'passport.lianjia.com',
    'Pragma': 'no-cache',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.111 Safari/537.36'
}

    # 获取lianjia_uuid
    req = urllib2.Request('http://sh.lianjia.com/')
    opener.open(req)
    # 初始化表单
    req = urllib2.Request(auth_url, headers=headers)
    result = opener.open(req)


    # 获取cookie和lt值
    pattern = re.compile(r'JSESSIONID=(.*)')
    jsessionid = pattern.findall(result.info().getheader('Set-Cookie').split(';')[0])[0]

    html_content = result.read()
    gzipped = result.info().getheader('Content-Encoding')
    if gzipped:
        html_content = zlib.decompress(html_content, 16+zlib.MAX_WBITS)
    pattern = re.compile(r'value=\"(LT-.*)\"')
    lt = pattern.findall(html_content)[0]
    pattern = re.compile(r'name="execution" value="(.*)"')
    execution = pattern.findall(html_content)[0]


    # data
    data = {
    'username': '15900445100', #替换为自己账户的用户名
    'password': 'Pa82839473', #替换为自己账户的密码
    'execution': execution,
    '_eventId': 'submit',
    'lt': lt,
    'verifyCode': '',
    'redirect': '',
    }

    # urllib进行编码
    post_data=urllib.urlencode(data)


    headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.8',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Host': 'passport.lianjia.com',
    'Origin': 'https://passport.lianjia.com',
    'Pragma': 'no-cache',
    'Referer': 'https://passport.lianjia.com/cas/login?service=http%3A%2F%2Fsh.lianjia.com%2F',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.111 Safari/537.36',
    'Upgrade-Insecure-Requests': '1',
    'X-Requested-With': 'XMLHttpRequest',
    }
    req = urllib2.Request(auth_url, post_data, headers)
    try:
        result = opener.open(req)
    except urllib2.HTTPError, e:
        print e.getcode()
        print e.reason
        print e.geturl()
        print e.info()
        req = urllib2.Request(e.geturl())
        result = opener.open(req)
        req = urllib2.Request(chengjiao_url)
        result = opener.open(req).read()

def getURL(url, tries_num=5, sleep_time=1, time_out=10):
    headers = {'content-type': 'application/json',
               'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:22.0) Gecko/20100101 Firefox/22.0'}
    sleep_time_p = sleep_time
    time_out_p = time_out
    tries_num_p = tries_num
    auth_url = 'https://passport.lianjia.com/cas/login?service=http%3A%2F%2Fsh.lianjia.com%2F'
    try:
        #s = requests.Session()
        #s.post(auth_url, {'username':'15900445100', 'password': 'Pa82839473',}, headers=headers)
        #res = s.get(url, headers=headers, timeout=time_out)
        res = requests.get(url, headers=headers, timeout=time_out)
        res.raise_for_status()  # 如果响应状态码不是 200，就主动抛出异常
    except requests.RequestException as e:
        sleep_time_p = sleep_time_p + 1
        time_out_p = time_out_p + 5
        tries_num_p = tries_num_p - 1
        print getCurrentTime(), url, 'URL Connection Error: 第', tries_num - tries_num_p, u'次 Retry Connection', e
        # 设置重试次数，最大timeout 时间和 最长休眠时间
        if tries_num_p > 0 and sleep_time_p < 300 and time_out_p < 600:
            time.sleep(sleep_time_p)
            res = getURL(url, tries_num_p, sleep_time_p, time_out_p)
            #return res
            print getCurrentTime(), url, 'URL Connection Success: 共尝试', tries_num - tries_num_p, u'次', ',sleep_time:', sleep_time_p, ',time_out:', time_out_p
    return res

def getURL2(url, tries_num=5, sleep_time=1, time_out=10):

    sleep_time_p = sleep_time
    time_out_p = time_out
    tries_num_p = tries_num
    #auth_url = 'https://passport.lianjia.com/cas/login?service=http%3A%2F%2Fsh.lianjia.com%2F'
    try:
        res= urllib2.urlopen(url)
        #res = requests.get(url, timeout=time_out)
        #res.raise_for_status()  # 如果响应状态码不是 200，就主动抛出异常
    except requests.RequestException as e:
        sleep_time_p = sleep_time_p + 1
        time_out_p = time_out_p + 5
        tries_num_p = tries_num_p - 1
        print getCurrentTime(), url, 'URL Connection Error: 第', tries_num - tries_num_p, u'次 Retry Connection', e
        # 设置重试次数，最大timeout 时间和 最长休眠时间
        if tries_num_p > 0 and sleep_time_p < 300 and time_out_p < 600:
            time.sleep(sleep_time_p)
            res = getURL(url, tries_num_p, sleep_time_p, time_out_p)
            #return res
            print getCurrentTime(), url, 'URL Connection Success: 共尝试', tries_num - tries_num_p, u'次', ',sleep_time:', sleep_time_p, ',time_out:', time_out_p
    return res

def getXiaoquList(fang_url):
    result = {}
    base_url = 'http://sh.lianjia.com'
    # res=requests.get(fang_url)
    res = getURL(fang_url)
    res.encoding = 'utf-8'
    soup = BeautifulSoup(res.text, 'html.parser')
    for fang in soup.select('.info-panel'):
        if (len(fang) > 0):
            try:
                result['xiaoqu_key'] = fang.select('h2')[0].a['key'].strip().lstrip().strip(" ")
                result['xiaoqu_name'] = fang.select('h2')[0].text.strip()
                result['xiaoqu_url'] = base_url + fang.select('h2')[0].a['href'].strip()
                result['quyu'] = fang.select('.con')[0].contents[1].text.strip()
                result['bankuai'] = fang.select('.con')[0].contents[3].text.strip()
                result['price'] = fang.select('.price')[0].span.text.strip() + fang.select('.price')[0].contents[2].strip()
                result['age'] = ''
                result['subway'] = ''
                result['onsale_num'] = ''
                result['fang_url'] = ''
                if len(fang.select('.con')[0].contents) >= 5:
                    result['age'] = fang.select('.con')[0].contents[-1].string.strip()
                if len(fang.select('.fang-subway-ex')) > 0:
                    result['subway'] = fang.select('.fang-subway-ex')[0].text.strip()
                if len(fang.select('.square')) > 0:
                    result['onsale_num'] = fang.select('.square')[0].a.text.strip()
                if len(fang.select('.square')) > 0:
                    result['fang_url'] = base_url + fang.select('.square')[0].a['href'].strip()
                    getLianjiaList(result['fang_url'])
                mySQL.insertData('lianjia_fang_xiaoqu', result)
                print getCurrentTime(), u'小区：', result['xiaoqu_key'], result['xiaoqu_name'], result['age'], result[ 'quyu'],result['bankuai'], \
                                                     result['subway'], result['xiaoqu_url'], result['price'], result['onsale_num'], result['fang_url']
                getLianjiaList(result['fang_url'])
            except Exception, e:
                print  getCurrentTime(), u"Exception:%d: %s" % (e.args[0], e.args[1])
    return result

def getLianjiaList(fang_url):
    result = {}
    base_url = 'http://sh.lianjia.com'
    # res=requests.get(fang_url)
    res = getURL(fang_url)
    res.encoding = 'utf-8'
    soup = BeautifulSoup(res.text, 'html.parser')
    for fang in soup.select('.info-panel'):
        if (len(fang) > 0):
            result['fang_key'] = fang.select('h2')[0].a['key'].strip()
            result['fang_desc'] = fang.select('h2')[0].text.strip()
            result['fang_url'] = base_url + fang.select('h2')[0].a['href'].strip()
            result['price'] = fang.select('.price')[0].text.strip()
            result['price_pre'] = fang.select('.price-pre')[0].text.strip()
            result['xiaoqu'] = fang.select('.where')[0].a.text.strip()
            result['huxing'] = fang.select('.where')[0].contents[3].text.strip()
            result['mianji'] = fang.select('.where')[0].contents[5].text.strip()
            result['quyu'] = fang.select('.con')[0].contents[1].text.strip()
            result['bankuai'] = fang.select('.con')[0].contents[3].text.strip()
            if len(result['bankuai'])<2 :
                 result['bankuai']=""
            result['louceng'] = fang.select('.con')[0].contents[6].string.strip()
            result['chaoxiang'] = ''
            result['age'] = ''
            result['subway'] = ''
            result['taxfree'] = ''
            result['haskey'] = ''
            result['col_look'] = ''
            if len(fang.select('.con')[0].contents) >= 8:
                result['chaoxiang'] = fang.select('.con')[0].contents[8].string.strip()
            if len(fang.select('.con')[0].contents) > 9:
                result['age'] = fang.select('.con')[0].contents[-1].string.strip()
            if len(fang.select('.fang-subway-ex')) > 0:
                result['subway'] = fang.select('.fang-subway-ex')[0].text.strip()
            if len(fang.select('.taxfree-ex')) > 0:
                result['taxfree'] = fang.select('.taxfree-ex')[0].text.strip()
            if len(fang.select('.haskey-ex')) > 0:
                result['haskey'] = fang.select('.haskey-ex')[0].text.strip()
            if len(fang.select('.square')) > 0:
                result['col_look'] = fang.select('.square')[0].span.text.strip()
            mySQL.insertData('lianjia_fang_list', result)
            print getCurrentTime(), u'在售：', result['fang_key'], result['quyu'], result['bankuai'], result['xiaoqu'], \
                                                 result['huxing'], result['price'], result['price_pre'], result['mianji']
            # fangList.append(result)
    return result

def getLianjiaTransList(fang_url):
    result = {}
    base_url = 'http://sh.lianjia.com'
    # res=requests.get(fang_url)
    res = getURL(fang_url)
    res.encoding = 'utf-8'
    soup = BeautifulSoup(res.text, 'html.parser')
    for fang in soup.select('.info-panel'):
        if (len(fang) > 0):
            result['fang_key'] = fang.select('h2')[0].a['key'].strip()
            result['fang_desc'] = fang.select('h2')[0].text.strip()
            result['fang_url'] = base_url + fang.select('h2')[0].a['href'].strip()
            result['taxfree'] = ''
            result['subway'] = ''
            result['chaoxiang'] = ''
            result['zhuangxiu'] = ''
            result['transaction_date'] = fang.select('.dealType')[0].contents[1].text.strip().strip(u'链家网签约').strip()
            result['price_pre'] = fang.select('.dealType')[0].contents[3].text.strip().strip(u'挂牌单价').strip()
            result['price'] = fang.select('.dealType')[0].contents[5].text.strip().strip(u'挂牌总价').strip()
            result['quyu'] = fang.select('.con')[0].contents[1].text.strip()
            result['bankuai'] = fang.select('.con')[0].contents[3].text.strip()
            result['louceng'] = fang.select('.con')[0].contents[6].string.strip()
            if len(fang.select('.con')[0].contents) >= 8:
                result['chaoxiang'] = fang.select('.con')[0].contents[8].string.strip()
            if len(fang.select('.con')[0].contents) >= 10:
                result['zhuangxiu'] = fang.select('.con')[0].contents[10].string.strip()
            mySQL.insertData('lianjia_fang_transaction', result)
            print getCurrentTime(), u'成交：', result['fang_key'], result['transaction_date'], result['quyu'], result[
                'bankuai'], result['fang_desc'], result['chaoxiang'], result['louceng'], result['zhuangxiu'], result[
                'price_pre'], result['price']  # ,result['fang_url']
    return result

def getSubRegions(fang_url, region):
    base_url = 'http://sh.lianjia.com'
    res = getURL(fang_url + region['code'])
    res.encoding = 'utf-8'
    soup = BeautifulSoup(res.text, 'html.parser')
    result = []
    gio_plate = soup.find('div', class_="option-list sub-option-list gio_plate")
    try:
        for link in gio_plate.find_all('a'):
            district = {}
            district['link']=link.get('href')
            district['code'] = link.get('gahref')
            district['name']=link.get_text()
            if district['code'] not in ['plate-nolimit']:
                result.append(district)
    except AttributeError:
        return result
    #print getCurrentTime(),'getSubRegions:',result
    return result

def getRegions(fang_url, region):
    base_url = 'http://sh.lianjia.com'
    url_fang = fang_url + region;
    res = getURL(fang_url + region)
    res.encoding = 'utf-8'
    soup = BeautifulSoup(res.text, 'html.parser')
    result = []
    gio_district = soup.find('div', class_="option-list gio_district")
    for link in gio_district.find_all('a'):
        district = {}
        district['link']=link.get('href')
        district['code'] = link.get('gahref')
        district['name']=link.get_text()
        if district['code'] not in ['district-nolimit']:
            result.append(district)
    #print getCurrentTime(),'getRegions:',result
    return result

def getLines(fang_url, region):
    base_url = 'http://sh.lianjia.com'
    res = getURL(fang_url + region)
    res.encoding = 'utf-8'
    soup = BeautifulSoup(res.text, 'html.parser')
    result = []
    gio_line = soup.find('div', class_="option-list gio_line")
    for link in gio_line.find_all('a'):
        district = {}
        if link.get('gahref') not in ['line-nolimit']:
            district['link'] = link.get('href')
            district['code'] = link.get('gahref')
            district['name'] = link.get_text()
            result.append(district)
    #print getCurrentTime(),'getLines:',result
    return result

def getLinesStations(fang_url, region):
    base_url = 'http://sh.lianjia.com'
    res=getURL(fang_url+region['code'])
    res.encoding = 'utf-8'
    soup = BeautifulSoup(res.text, 'html.parser')
    result = []
    gio_stop = soup.find('div', class_="option-list sub-option-list gio_stop")
    for link in gio_stop.find_all('a'):
        district = {}
        if link.get('gahref') not in ['stop-nolimit']:
            district['link'] = link.get('href')
            district['code'] = link.get('gahref')
            district['name'] = link.get_text()
            result.append(district)
    #print getCurrentTime(),'getLinesStations:',result
    return result

# 获取当前时间
def getCurrentTime():
    return time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time()))

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
            sql = "INSERT INTO %s (%s) VALUES (%s)" % (table, cols, '"' + values + '"')
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
                # 主键唯一，无法插入
                if "key 'PRIMARY'" in e.args[1]:
                    print self.getCurrentTime(), u"Primary Key Constraint，No Data Insert:", e.args[0], e.args[1]
                    # return 0
                elif "MySQL server has gone away" in e.args :
                    self._init_('localhost', 'root', 'root', 'fang')
                else:
                    print self.getCurrentTime(), u"Data Insert Failed: %d: %s" % (e.args[0], e.args[1])
        except MySQLdb.Error, e:
            print self.getCurrentTime(), u"MySQLdb Error:%d: %s" % (e.args[0], e.args[1])

def getLineMain():
    lines = getLines('http://sh.lianjia.com/ditiefang/', '')
    lines.reverse()
    #start_page = 1
    #end_page =10
    #sleep_time = 0.5
    #按地铁爬取数据
    while lines:
        #break  #Test
        line = lines.pop()
        print getCurrentTime(),line['name'],':','getLineMain :Scrapy Starting.....'
        time.sleep(sleep_time)
        linesStations = getLinesStations('http://sh.lianjia.com/ditiefang/', line)
        while linesStations:
            try:
                linesStation = linesStations.pop()
                print getCurrentTime(), line['name'],':',linesStation['name'],'Scrapy Starting.....'
                # time.sleep(sleep_time)
                for i in range(start_page, end_page):
                    fang_url = 'http://sh.lianjia.com/ditiefang/' + linesStation['code'] + '/d' + str(i)
                    print getCurrentTime(), line['name'], ':', linesStation['name'], fang_url
                    time.sleep(sleep_time)
                    fang = getLianjiaList(fang_url)
                    if len(fang) < 1:
                        print getCurrentTime(),line['name'],':',linesStation['name'],u' : getLianjiaList Scrapy Finished'
                        break
                print getCurrentTime(),line['name'],':',linesStation['name'],'Scrapy Finished'
            except Exception, e:
                print  getCurrentTime(), u"Exception:%s" % (e.message)
                #if "MySQL server has gone away" in e.args:
                #    mySQL._init_('localhost', 'root', 'root', 'fang')
        print getCurrentTime(),linesStation['name'],':','linesStations:Scrapy Finished'
    print getCurrentTime(),line['name'],':','getLineMain:Scrapy Finished'

def getTransMain():
    regions = getRegions('http://sh.lianjia.com/ershoufang/', 'pudongxinqu')
    regions.reverse()
    #start_page = 1
    #end_page =10
    #sleep_time = 0.5
    #按行政区域爬取数据
    while regions:
        #break  #Test
        region = regions.pop()
        print getCurrentTime(), 'Region:',region['name'], ':', 'Scrapy Starting.....'
        time.sleep(sleep_time)
        subRegions = getSubRegions('http://sh.lianjia.com/ershoufang/', region)
        subRegions.reverse()
        while subRegions:  # and  region in  ['pudongxinqu','minhang','baoshan','xuhui','putuo','yangpu','changning','songjiang','jiading','huangpu','jingan','zhabei','hongkou','qingpu','fengxian','jinshan','chongming','shanghaizhoubian']:
            try:
                subRegion = subRegions.pop()
                print getCurrentTime(), region['name'], ':', subRegion['name'], 'Scrapy Starting.....'
                # time.sleep(sleep_time)
                for i in range(start_page, end_page):
                    chengjiao_url = 'http://sh.lianjia.com/chengjiao/' + subRegion['code'] + '/d' + str(i)
                    print getCurrentTime(), subRegion['name'], chengjiao_url
                    time.sleep(sleep_time)
                    fang = getLianjiaTransList(chengjiao_url)
                    if len(fang) < 1:
                        print getCurrentTime(), region['name'], ':', subRegion['name'], u' : getLianjiaTransList Scrapy Finished'
                        break

                print getCurrentTime(), region['name'], ':', subRegion['name'], 'Scrapy Finished'
            except Exception, e:
                print  getCurrentTime(), u"Exception:%s" % (e.message)
                #if "MySQL server has gone away" in e.args:
                #    mySQL._init_('localhost', 'root', 'root', 'fang')
        print getCurrentTime(), region['name'], ':', 'Scrapy Finished'
    print getCurrentTime(), 'getTransMain Scrapy Success'

def getXiaoquMain():
    regions = getRegions('http://sh.lianjia.com/ershoufang/', 'pudongxinqu')
    regions.reverse()
    #start_page = 1
    #end_page =10
    #sleep_time = 0.5
    #按行政区域爬取数据
    while regions:
        #break  #Test
        region = regions.pop()
        print getCurrentTime(), 'Region:',region['name'], ':', 'Scrapy Starting.....'
        time.sleep(sleep_time)
        subRegions = getSubRegions('http://sh.lianjia.com/ershoufang/', region)
        subRegions.reverse()
        while subRegions:  # and  region in  ['pudongxinqu','minhang','baoshan','xuhui','putuo','yangpu','changning','songjiang','jiading','huangpu','jingan','zhabei','hongkou','qingpu','fengxian','jinshan','chongming','shanghaizhoubian']:
            try:
                subRegion = subRegions.pop()
                print getCurrentTime(), region['name'], ':', subRegion['name'], 'Scrapy Starting.....'
                # time.sleep(sleep_time)
                for i in range(start_page, end_page):
                    fang_url = 'http://sh.lianjia.com/xiaoqu/' + subRegion['code'] + '/d' + str(i)
                    print getCurrentTime(), region['name'], ':', subRegion['name'], fang_url
                    time.sleep(sleep_time)
                    fang = getXiaoquList(fang_url)
                    if len(fang) < 1:
                        print getCurrentTime(), region['name'], ':', subRegion['name'], u' : getXiaoquList Scrapy Finished'
                        break
                print getCurrentTime(), region['name'], ':', subRegion['name'], 'Scrapy Finished'
            except Exception, e:
                print  getCurrentTime(), u"Exception:%s" % (e.message)
                #if "MySQL server has gone away" in e.args:
                #    mySQL._init_('localhost', 'root', 'root', 'fang')
        print getCurrentTime(), region['name'], ':', 'Scrapy Finished'
    print getCurrentTime(), 'getXiaoquMain Scrapy Success'

def getFangMain():
    regions = getRegions('http://sh.lianjia.com/ershoufang/', 'pudongxinqu')
    regions.reverse()
    #start_page = 1
    #end_page =10
    #sleep_time = 0.5
    #按行政区域爬取数据
    while regions:
        #break  #Test
        region = regions.pop()
        print getCurrentTime(), 'Region:',region['name'], ':', 'Scrapy Starting.....'
        time.sleep(sleep_time)
        subRegions = getSubRegions('http://sh.lianjia.com/ershoufang/', region)
        subRegions.reverse()
        while subRegions:  # and  region in  ['pudongxinqu','minhang','baoshan','xuhui','putuo','yangpu','changning','songjiang','jiading','huangpu','jingan','zhabei','hongkou','qingpu','fengxian','jinshan','chongming','shanghaizhoubian']:
            try:
                subRegion = subRegions.pop()
                print getCurrentTime(), region['name'], ':', subRegion['name'], 'Scrapy Starting.....'
                # time.sleep(sleep_time)
                for i in range(start_page, end_page):
                    fang_url = 'http://sh.lianjia.com/ershoufang/' + subRegion['code']+ '/d' + str(i)
                    print getCurrentTime(), region['name'], ':', subRegion['name'], fang_url
                    time.sleep(sleep_time)
                    fang = getLianjiaList(fang_url)
                    if len(fang) < 1:
                        print getCurrentTime(), region['name'], ':', subRegion['name'], u' : getLianjiaList Scrapy Finished'
                        break
                print getCurrentTime(), region['name'], ':', subRegion['name'], 'Scrapy Finished'
            except Exception, e:
                print  getCurrentTime(), u"Exception:%s" % (e.message)
                #if "MySQL server has gone away" in e.args:
                #    mySQL._init_('localhost', 'root', 'root', 'fang')
        print getCurrentTime(), region['name'], ':', 'Scrapy Finished'
    print getCurrentTime(), 'getFangMain Scrapy Success'

def mainAll():
    regions = getRegions('http://sh.lianjia.com/ershoufang/', 'pudongxinqu')
    regions.reverse()
    #start_page = 1
    #end_page =10
    #sleep_time = 0.5
    #按行政区域爬取数据
    while regions:
        #break  #Test
        region = regions.pop()
        print getCurrentTime(), 'Region:',region['name'], ':', 'Scrapy Starting.....'
        time.sleep(sleep_time)
        subRegions = getSubRegions('http://sh.lianjia.com/ershoufang/', region)
        subRegions.reverse()
        while subRegions:  # and  region in  ['pudongxinqu','minhang','baoshan','xuhui','putuo','yangpu','changning','songjiang','jiading','huangpu','jingan','zhabei','hongkou','qingpu','fengxian','jinshan','chongming','shanghaizhoubian']:
            try:
                subRegion = subRegions.pop()
                print getCurrentTime(), region['name'], ':', subRegion['name'], 'Scrapy Starting.....'
                # time.sleep(sleep_time)
                for i in range(start_page, end_page):
                    fang_url = 'http://sh.lianjia.com/xiaoqu/' + subRegion['code'] + '/d' + str(i)
                    print getCurrentTime(), region['name'], ':', subRegion['name'], fang_url
                    time.sleep(sleep_time)
                    fang = getXiaoquList(fang_url)
                    if len(fang) < 1:
                        print getCurrentTime(), region['name'], ':', subRegion['name'], u' : getXiaoquList Scrapy Finished'
                        break

                for i in range(start_page, end_page):
                    fang_url = 'http://sh.lianjia.com/ershoufang/' + subRegion['code']+ '/d' + str(i)
                    print getCurrentTime(), region['name'], ':', subRegion['name'], fang_url
                    time.sleep(sleep_time)
                    fang = getLianjiaList(fang_url)
                    if len(fang) < 1:
                        print getCurrentTime(), region['name'], ':', subRegion['name'], u' : getLianjiaList Scrapy Finished'
                        break

                for i in range(start_page, end_page):
                    chengjiao_url = 'http://sh.lianjia.com/chengjiao/' + subRegion['code'] + '/d' + str(i)
                    print getCurrentTime(), subRegion['name'], chengjiao_url
                    time.sleep(sleep_time)
                    fang2 = getLianjiaTransList(chengjiao_url)
                    if len(fang2) < 1:
                        print getCurrentTime(), region['name'], ':', subRegion['name'], u' : getLianjiaTransList Scrapy Finished'
                        break

                print getCurrentTime(), region['name'], ':', subRegion['name'], 'Scrapy Finished'
            except Exception, e:
                print  getCurrentTime(), u"Exception:%s" % (e.message)
                #if "MySQL server has gone away" in e.args:
                #    mySQL._init_('localhost', 'root', 'root', 'fang')
        print getCurrentTime(), region['name'], ':', 'Scrapy Finished'
    print getCurrentTime(), 'Lianjia Shanghai All Scrapy Success'

def getSubregionsThread():
    while True:
        region = regionsQueue.get()
        subRegions=getSubRegions('http://sh.lianjia.com/ershoufang/', region)
        while subRegions:
             try:
                subRegion = subRegions.pop()
                print getCurrentTime(), region['name'], ':', subRegion['name'], 'Scrapy Starting.....'
                time.sleep(sleep_time)
                for i in range(start_page, end_page):
                    chengjiao_url = 'http://sh.lianjia.com/chengjiao/' + subRegion['code'] + '/d' + str(i)
                    print getCurrentTime(), subRegion['name'], chengjiao_url
                    time.sleep(sleep_time)
                    fang = getLianjiaTransList(chengjiao_url)
                    if len(fang) < 1:
                        print getCurrentTime(), region['name'], ':', subRegion['name'], u' : getLianjiaTransList Scrapy Finished'
                        break

                print getCurrentTime(), region['name'], ':', subRegion['name'], 'Scrapy Finished'
             except Exception, e:
                print  getCurrentTime(), u"Exception:%s" % (e.message)
        print getCurrentTime(), region['name'], ':', 'Scrapy Finished'
        regionsQueue.task_done()

def getTransThread():
    regions = getRegions('http://sh.lianjia.com/ershoufang/', 'pudongxinqu')
    regions.reverse()
    while regions:
       regionsQueue.put(regions.pop())
    #fork NUM个线程等待队列
    for i in range(NUM):
      t = Thread(target= getSubregionsThread)
      print i ,u'启动'
      t.setDaemon(True)
      t.start()
    #等待所有JOBS完成
    regionsQueue.join()
    print getCurrentTime(), 'getTransMain Scrapy Success'

def main():
    #LianjiaLogin()
    print getCurrentTime(), 'Main Scrapy Starting'
    global mySQL, start_page, end_page, sleep_time,regionsQueue,NUM,taskQueue
    mySQL = MySQL()
    mySQL._init_('localhost', 'root', 'root', 'fang')
    start_page=1
    end_page=101
    sleep_time=0.1
    regionsQueue = Queue()  # q是任务队列
    taskQueue= Queue()
    NUM = 1  #NUM是并发线程总数
    #JOBS = 100 #JOBS是有多少任务
    #getTransMain()
    #getTransThread()
    #getFangMain()
    mainAll()
    #getXiaoquMain()
    #getLineMain()

if __name__ == "__main__":
    main()