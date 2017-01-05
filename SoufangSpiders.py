# -*- coding:utf-8 -*-
############################################################################
# 程序：上海搜房网爬虫
# 功能：抓取上海搜房网二手房在售、成交数据
# 创建时间：2017/01/03
# 更新历史：
#
# 使用库：requests、BeautifulSoup4、MySQLdb
# 作者：yuzhucu
#############################################################################
import requests
from bs4 import BeautifulSoup
import lxml
import time
import MySQLdb

def getCurrentTime():
    # 获取当前时间
    return time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time()))

def getURL(url, tries_num=1000, sleep_time=5, time_out=100):
    headers = {'content-type': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
               'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.75 Safari/537.36',
               "Host": "esf.sh.fang.com"}
    sleep_time_p = sleep_time
    time_out_p = time_out
    tries_num_p = 0
    try:
        res = requests.get(url, headers=headers, timeout=time_out)
        res.raise_for_status()  # 如果响应状态码不是 200，就主动抛出异常
    except requests.RequestException as e:
        sleep_time_p = sleep_time_p + 1
        time_out_p = time_out_p + 10
        tries_num_p = tries_num_p +1
        print getCurrentTime(), url, 'URL Connection Error: 第', tries_num_p, u'次 Retry Connection', e
        # 设置重试次数，最大timeout 时间和 最长休眠时间
        if tries_num_p<=tries_num:
            time.sleep(sleep_time_p)
            #return res
            return getURL(url, tries_num-time_out_p+1, sleep_time_p, time_out_p)
            print getCurrentTime(), url, 'URL Connection Success: 共尝试', tries_num_p, u'次', ',sleep_time:', sleep_time_p, ',time_out:', time_out_p

    return res

def getSoufangList(fang_url):
    base_url=''
    result = {}
    res=getURL(fang_url)
    res.encoding = 'gbk'
    soup = BeautifulSoup(res.text, 'lxml')
    for fang in soup.find_all("dd",class_="info rel floatr"):
        try:
            result['fang_key']=fang.select('p')[0].a['href'].strip()
            result['fang_desc'] = fang.select('p')[0].text.strip()
            result['fang_url'] = base_url + fang.select('p')[0].a['href'].strip()
            result['huxing']= fang.select('p')[1].contents[0].strip()
            result['louceng']= fang.select('p')[1].contents[2].strip()
            result['chaoxiang']= fang.select('p')[1].contents[4].strip()
            result['age']= fang.select('p')[1].contents[6].strip()
            result['xiaoqu']= fang.select('p')[2].span.text.strip()
            result['address']= fang.find('span','iconAdress ml10 gray9').get_text()
            #result['subway']=fang.find('span','train note').text.strip()
            #result['subway']=fang.select('div.mt8.clearfix > div.pt4.floatl > span.train.note')[0].text
            #result['subway']=fang.select('div')[0].text.strip()
            result['mianji']=fang.select('div')[2].text.strip()
            result['mianji']=fang.find('div','area alignR').get_text().strip()
            result['price']=fang.find('span','price').get_text().strip()
            result['price_pre']=fang.select('div.moreInfo > p.danjia.alignR.mt5')[0].get_text().strip()
            #print fang.select('div.moreInfo > p.danjia.alignR.mt5')[0].get_text().strip()
            result['data_source']='Soufang'
            result['updated_date']=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            mySQL.insertData('soufang_fang_list', result)
            print getCurrentTime(), u'在售：', result['xiaoqu'], result['address'], result['fang_desc'] ,\
                result['huxing'], result['louceng'],result['chaoxiang'],result['price'],u'万',result['price_pre']
            #fangList.append(result)
            #break
        except Exception, e:
             print  getCurrentTime(), u"Exception:%s" % (e.message)
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
            district['name']=link.get_text()
            #print  district['code'],district['name']
            if  district['name']<>u'\u4e0d\u9650':
                result.append(district)
    except  Exception, e:
            print  getCurrentTime(),'getRegions',fang_url,  u"Exception:%s" % (e.message)
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
            district['name']=link.get_text()
            print  district['code'],district['name']
            if district['code'] not in [u'不限']:
                result.append(district)
    except Exception, e:
            print  getCurrentTime(),'getSubRegions',fang_url, u"Exception:%s" % (e.message)
            return result
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
            #sql = "INSERT INTO %s (%s) VALUES (%s)" % (table, cols, '"' + values + '"')
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

def getSoufangMain(url):
    regions = getRegions(url)
    regions.reverse()
    while regions:
        region = regions.pop()
        print getCurrentTime(), 'Region:',region['name'], ':', 'Scrapy Starting.....'
        time.sleep(sleep_time)
        subRegions = getSubRegions('http://esf.sh.fang.com'+str(region['code']))
        subRegions.reverse()
        while subRegions:
            try:
                subRegion = subRegions.pop()
                print getCurrentTime(), region['name'], ':', subRegion['name'], 'Scrapy Starting.....'
                time.sleep(sleep_time)
                for i in range(start_page, end_page):
                        fang_url = 'http://esf.sh.fang.com' + subRegion['code']+ 'i3' + str(i)
                        print getCurrentTime(), region['name'], ':', subRegion['name'], fang_url
                        time.sleep(sleep_time)
                        #if region['code']<> subRegion['code']:
                        fang = getSoufangList(fang_url)
                        if len(fang) < 1:
                            print getCurrentTime(), region['name'], ':', subRegion['name'], u' : getSoufangList Scrapy Finished'
                            break
                        print getCurrentTime(), region['name'], ':', subRegion['name'], 'Scrapy Finished'
            except Exception, e:
                   print  getCurrentTime(), u"Exception:%s" % (e.message)
        print getCurrentTime(), region['name'], ':', 'Scrapy Finished'
    print getCurrentTime(), 'getSoufangMain Scrapy Success'

def main():
    print getCurrentTime(), 'Main Scrapy Starting'
    global mySQL, start_page, end_page, sleep_time
    mySQL = MySQL()
    mySQL._init_('localhost', 'root', 'root', 'fang')
    start_page=1
    end_page=101
    sleep_time=0.1
    url='http://esf.sh.fang.com'
    #url2='http://esf.sh.fang.com/house-a025/'
    aa=getRegions(url)
    #getSubRegions(url2)
    getSoufangMain(url)

if __name__ == "__main__":
    main()