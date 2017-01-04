# -*- coding:utf-8 -*-
############################################################################
# 程序：上海搜房网爬虫
# 功能：抓取上海链家二手房在售、成交数据 ，大约各5万记录；小区2万多个
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

def getURL(url, tries_num=5, sleep_time=3, time_out=50):
    headers = {'content-type': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
               'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.75 Safari/537.36',
               "Host": "esf.sh.fang.com"}
    sleep_time_p = sleep_time
    time_out_p = time_out
    tries_num_p = tries_num
    try:
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

def getSoufangList(fang_url):
    base_url=''
    result = {}
    #res = requests.get(fang_url)
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
            result['unit_price']=fang.select('div.moreInfo > p.danjia.alignR.mt5')[0].get_text().strip()
            #print fang.select('div.moreInfo > p.danjia.alignR.mt5')[0].get_text().strip()

            result['updated_date']=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            #mySQL.insertData('lianjia_fang_list', result)
            print getCurrentTime(), u'在售：', result['xiaoqu'], result['address'], result['fang_desc'] ,\
                result['huxing'], result['louceng'],result['chaoxiang'],result['price'],u'万',result['unit_price']
            # fangList.append(result)
            #break
        except:
            pass

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

def getMaxPage(fang_url):
    res = getURL(fang_url)
    res.encoding = 'utf-8'
    soup = BeautifulSoup(res.text, 'html.parser')
    result =0
    a=[]
    pageBox = soup.find('div', class_="page-box house-lst-page-box")
    try:
        for link in pageBox.find_all('a'):
            a=link.get('gahref')
            if link.get('gahref')  in [ 'results_next_page']:
               return result
            if link.get('gahref') <> 'results_next_page':
                result=link.get_text()
                #print getCurrentTime(),'getPageBox: MaxPage:',link.get_text()
        #print  result
        return result
    except Exception, e:
               #print getCurrentTime(),'getPageBox: MaxPage:',a,result,e.message,fang_url
               return 0
    #print getCurrentTime(),'getPageBox: MaxPage:',a,result
    return result

def getFangCond():
    result=[]
    for a in range(1,9):#面积
        for l in  range(1,7):#户型
            for p in  range(1,9):#总价
                cond={}
                cond['link']='a'+str(a)+'l'+str(l)+'p'+str(p)
                cond['a']='a'+str(a)
                cond['l']='l'+str(l)
                cond['p']='p'+str(p)
                print cond['link']
                result.append(cond)
    return result

def getFangTransCond():
    result=[]
    for a in range(1,9):#面积
        for l in  range(1,7):#户型
                cond={}
                cond['link']='a'+str(a)+'l'+str(l)
                cond['a']='a'+str(a)
                cond['l']='l'+str(l)
                print cond['link']
                result.append(cond)
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

def main():
    print getCurrentTime(), 'Main Scrapy Starting'
    global mySQL, start_page, end_page, sleep_time
    mySQL = MySQL()
    mySQL._init_('localhost', 'root', 'root', 'fang')
    start_page=1
    end_page=101
    sleep_time=0.1
    url='http://esf.sh.fang.com/'
    base_url = 'http://esf.sh.fang.com/'
    getSoufangList(url)

if __name__ == "__main__":
    main()