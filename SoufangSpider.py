# -*- coding:utf-8 -*-
############################################################################
'''
# 程序：上海搜房网爬虫
# 功能：抓取上海搜房网二手房在售、成交数据
# 创建时间：2017/01/03
# 更新历史：2017/01/07 增加多城市处理、随机Header;
#                      增加爬取城市URL信息;封装为类，补充注释和日志
#
# 使用库：requests、BeautifulSoup4、MySQLdb
# 作者：yuzhucu
'''
#############################################################################
import requests
from bs4 import BeautifulSoup
import lxml
import time
import random
import MySQLdb

def randHeader():
    '''
    随机生成User-Agent
    :return:
    '''
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

def getCurrentTime():
        # 获取当前时间
        return time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time()))

def getURL(url, tries_num=50, sleep_time=0, time_out=10,max_retry = 50):
        '''
           这里重写get函数，主要是为了实现网络中断后自动重连，同时为了兼容各种网站不同的反爬策略及，通过sleep时间和timeout动态调整来测试合适的网络连接参数；
           通过isproxy 来控制是否使用代理，以支持一些在内网办公的同学
        :param url:
        :param tries_num:  重试次数
        :param sleep_time: 休眠时间
        :param time_out: 连接超时参数
        :param max_retry: 最大重试次数，仅仅是为了递归使用
        :return: response
        '''
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
            if tries_num_p > 0:
                time.sleep(sleep_time_p)
                print getCurrentTime(), url, 'URL Connection Error: 第', max_retry - tries_num_p, u'次 Retry Connection', e
                return getURL(url, tries_num_p, sleep_time_p, time_out_p,max_retry)
        return res

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

class SoufangSpider():
   '''
     这里封装为类主要是便于管理，函数太多了修改代码切换麻烦
   '''
   def getCurrentTime(self):
        # 获取当前时间，主要用于记录日志
        return time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time()))

   def getCityURL(self,fang_url):
        '''
           从给定的主页登录，获取一级城市URL链接信息；剔除香港和更多城市链接
           :param fang_url:给定的主页登录，也可以从任一城市或者区域入口进入
           :return:返回城市名称及URL信息
        '''
        res = getURL(fang_url)
        res.encoding = 'gbk'
        soup = BeautifulSoup(res.text, 'html.parser')
        result = []
        gio_district = soup.find('div', class_="city20141104nr")
        try:
            for link in gio_district.find_all('a'):
                district = {}
                district['base_url'] = link.get('href')
                district['city'] = link.get_text()
                #print  district['code'],district['name']
                #剔除香港和更多城市链接
                if district['base_url'] not in [u'http://www.hkproperty.com/',u'/newsecond/esfcities.aspx']:
                    result.append(district)
        except  Exception, e:
            print  self.getCurrentTime(), 'getCityURL', fang_url, u"Exception:%s" % (e.message)
            return result
        #print result
        return result

   def getAllCityURL(self,fang_url):
        '''
           从给定的主页登录，获取一级城市URL链接信息；剔除香港和更多城市链接
           :param fang_url:给定的主页登录，也可以从任一城市或者区域入口进入
           :return:返回城市名称及URL信息
        '''
        res = getURL(fang_url)
        res.encoding = 'gbk'
        soup = BeautifulSoup(res.text, 'html.parser')
        result = []
        gio_district = soup.find_all('a', class_="red")
        #gio_district = soup.select('#c02 > ul')
        #patt=re.compile(r'(<a class=(""|"red") href="http://esf*</a>)')
        #print type(gio_district),len(gio_district),type(gio_district[0])
        print type(gio_district),len(gio_district),(gio_district)
        #links=re.findall(patt,gio_district[0])
        try:
            for i in range(0,170):
              #for link in links.find('a'):
                link=gio_district[i]
                district = {}
                district['base_url'] = link.get('href')
                district['city'] = link.get_text()
                print  district['code'],district['name']
                #剔除香港和更多城市链接
                if district['base_url'] not in [u'http://www.hkproperty.com/',u'/newsecond/esfcities.aspx']:
                    result.append(district)
        except  Exception, e:
            print  self.getCurrentTime(), 'getAllCityURL', fang_url, u"Exception:%s" % (e.message)
            return result
        #print result
        return result

   def getRegions(self,fang_url):
        '''
          根据城市链接入口，获取每个城市的一级行政区域，因网站默认只显示100页，
          在数量量大的情况下，可以细化条件以扩大爬取数据量；
          搜房网字符集编码为GBK，否则中文乱码
        :param fang_url:城市链接入口
        :return:返回一级区域名称及URL信息
        '''
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
                #剔除不限，减少重复数据爬取，因中文乱码，这里用unicode代替
                if district['name'] <> u'\u4e0d\u9650':
                    result.append(district)
        except  Exception, e:
            print  self.getCurrentTime(), 'getRegions', fang_url, u"Exception:%s" % (e.message)
            return result
        return result

   def getSubRegions(self,fang_url):
        '''
         从一级行政区域出发，获取二级区域链接，处理方式同一级行政区域
        :param fang_url:二级行政区域URL（全拼，需加上城市URL前缀，二级URL已包含一级URL，这个与链家不同）
        :return:返回二级区域的名称及URL信息
        '''
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
            print  self.getCurrentTime(), 'getSubRegions', fang_url, u"Exception:%s" % (e.message)
            return result
        return result

   def getSoufangList(self,fang_url, args):
        '''
        根据传入的网页入口，逐页爬取房源清单信息。并存入MySQL数据库。
        这里主要通过BeautifulSoup4 的find、select 抓取网页信息，有个别字段不是标准网页元素，通过select、xpath无法获取所有信息,故多种方式并用。
        后续再研究下xpath、css方法抓取网页信息的方法；另个别字段还没有做保准化清洗（如数字和文字没有剥离，可以在数据库中处理更简单些，刚学网页，还不是很熟悉）
        :param fang_url:根据传入的URL（可以在城市、一级行政区域、二级行政区域链接入口），搜房网比链家规整，各城市、层级 房源信息格式基本一致，可以公用。
        :param args:这里的参数是一个数据，因为房源明细信息不全（如没有包含行政区域等），通过数组封装起来，可以把房源的城市、行政区域、URL入口等一并传入保存至数据库，参数数量又不至于太多
        :return: 返回房源详细信息
        '''
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
                    # 建筑年代和朝向可能不全，如缺失先放一样，先确保房源信息完没有丢失
                    result['age'] = result['chaoxiang']
                    print self.getCurrentTime(), u"Exception:%s" % (e.message), result['fang_url'], result[
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
                print self.getCurrentTime(), u'在售：',result['city'], result['quyu'], result['bankuai'], result['xiaoqu'], result['address'], \
                    result['huxing'], result['louceng'], result['chaoxiang'], result['price'], u'万', result[
                    'price_pre'], mianji, result['fang_desc']
            except Exception as e:
                print  self.getCurrentTime(),args['city'],args['region'],args['subRegion'],':', u"Exception:%s" % (e.message), result['fang_url'], result['fang_desc']
        return result

   def getSoufangMutiCityMain(self,city):
        '''
        爬取数据入口，刚开始只考虑了单一城市爬取，后来发现搜房网各城市网页结构类似，股修改为按城市爬取。
        对每个城市，分别根据一级、二级行政区域逐层循环爬取；每个链接最多只显示100页，通过城市链接+二级区域链接（相对路径）+页码 拼接完整链接；
        每页30默认30条记录，每个区域最多显示100页，也就是3000条数据；如果不足100页，直接中断返回下一个区域； 因搜房网数据客户经理在不断刷新，动态链接信息不同时间爬取结果可能不一样，也暂时没有考虑断点续传问题，只能重跑；
        一种简单粗暴的方式是可以通过全局参数end_page 来控制最大爬取的页码数（<=100）来避免重复数据； 数据入库也是repalce模式直接更新或者插入
        :param city:这里是一个数组，除了传入城市的基本URL外，也可以把其他想要的信息一并传入以做调试和保存数据
        :return:NONE
        '''
        regions = self.getRegions(city['base_url'])
        #regions.reverse()
        while regions:
            region = regions.pop()
            print self.getCurrentTime(),city['city'],region['name'], ':', 'Scrapy Starting.....'
            time.sleep(sleep_time)
            subRegions = self.getSubRegions(city['base_url'] + str(region['code']))
            subRegions.reverse()
            while subRegions:
                try:
                    subRegion = subRegions.pop()
                    print self.getCurrentTime(), city['city'],region['name'],subRegion['name'], ':', 'Scrapy Starting.....'
                    # time.sleep(sleep_time)
                    for i in range(start_page, end_page):
                        args = {'region': region['name'], 'subRegion': subRegion['name'], 'city': city['city'],
                                'base_url': city['base_url']}
                        fang_url = city['base_url'] + subRegion['code'] + 'i3' + str(i)
                        print self.getCurrentTime(), args['city'], args['region'], args['subRegion'],':', fang_url,':', 'Scrapy Starting...'
                        #time.sleep(sleep_time)
                        fang = self.getSoufangList(fang_url, args)
                        if len(fang) < 1:
                            print self.getCurrentTime(), city['city'], region['name'],  subRegion[ 'name'],':', ' Scrapy Finished'
                            break
                        print self.getCurrentTime(),city['city'], region['name'], subRegion['name'],':',fang_url,':', 'Scrapy Finished'
                except Exception, e:
                    print  self.getCurrentTime(),city['city'], region['name'], subRegion['name'],':', u'Exception:%s' % (e.message)
            print self.getCurrentTime(),city['city'], region['name'], ':', 'Scrapy Finished'
        print self.getCurrentTime(),city['city'],':' ,' Scrapy Success'

def main():
    global mySQL, start_page, end_page, sleep_time, isproxy, proxy, header
    mySQL = MySQL()
    soufang=SoufangSpider()
    #mySQL._init_('localhost', 'root', 'root', 'fang')
    mySQL._init_('115.159.209.101', 'root', 'root', 'fang')
    isproxy = 0  # 如需要使用代理，改为1，并设置代理IP参数 proxy
    proxy = {"http": "http://110.37.84.147:8080", "https": "http://110.37.84.147:8080"}#这里需要替换成可用的代理IP
    header = randHeader()
    start_page = 1
    end_page = 101
    sleep_time = 0.1
    url = 'http://esf.sz.fang.com/'
    '''
    获取城市列表，并逐个城市爬取；后面有时间研究下多线程并发处理
    整个测试下来，发现就上海的爬取比较慢，网络经常中断，其他城市都还好。本地区网络不应该更快么，还是上海的访问量远大于其他城市？
    '''
    cities = [{'base_url': 'http://esf.sh.fang.com/', 'city': u'上海'},
              {'base_url': 'http://esf.zz.fang.com/', 'city': u'郑州'},
              {'base_url': 'http://esf.sz.fang.com/', 'city': u'深圳'},
              {'base_url': 'http://esf1.fang.com/', 'city': u'北京'},
              {'base_url': 'http://esf.gz.fang.com/', 'city': u'广州'}
              ]
    cities = soufang.getCityURL(url)
    citylist='http://esf.sh.fang.com/newsecond/esfcities.aspx'
    #cities = soufang.getAllCityURL(citylist)
    #cities.reverse()
    for city in cities:
          print city['city'],city['base_url']
          if city['base_url'] in ['http://esf.sh.fang.com/','http://esf.zz.fang.com/','http://esf.sz.fang.com/','http://esf1.fang.com/','http://esf.gz.fang.com/']:
              print 'continue'
          else:
              soufang.getSoufangMutiCityMain(city)

if __name__ == "__main__":
    main()
