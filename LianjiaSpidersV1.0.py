# -*- coding:utf-8 -*-
'''
############################################################################
# 程序：上海链家网爬虫
# 功能：抓取上海链家二手房在售、成交数据 ，大约各5万记录；小区2万多个
# 创建时间：2016/11/10
# 更新历史：2016/11/26
#          2016.11.27:增加地铁找房；更新区域参数；拆分模块 ，以便于单独调用
#          2016.12.06 加入多线程处理
#          2016.12.28 增加按照面积、户型、总价等明细条件，以便扩大爬数范围
#          2017.01.12 梳理代码，清理无效代码
# 使用库：requests、BeautifulSoup4、MySQLdb
# 作者：yuzhucu
#############################################################################
'''
import requests
from bs4 import BeautifulSoup
import time
import MySQLdb
import random

def getCurrentTime():
    '''
    获取当前时间
    :return:
    '''
    return time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time()))

def getMaxPage(fang_url):
    '''
     获取某个区域的最大页码数，如果超过00，可以考虑进一步细化查询条件，以爬取更多数据
     :return:
     '''
    res = getURL(fang_url)
    res.encoding = 'utf-8'
    soup = BeautifulSoup(res.text, 'html.parser')
    result =0
    pageBox = soup.find('div', class_="page-box house-lst-page-box")
    try:
        for link in pageBox.find_all('a'):
            #能直接找到最大页，返回（此步可省略）
            # if link.get('gahref') == 'results_totalpage':
            #    result=link.get_text()
            #    print  getCurrentTime(),'getPageBox: MaxPage:',result
            #    return result
            #有些页数比较少，可能找不到总页数，根据results_next_page前一个页码作为最大页码
            if link.get('gahref') <> 'results_next_page':
                result=link.get_text()
            #使用next_page前一个页码作为最大页码
            if link.get('gahref') in [ 'results_next_page']:
                #print   getCurrentTime(),'getPageBox: MaxPage:',result
                return result
        #都找不到，页码为0
        return result
    except Exception, e:
               #print getCurrentTime(),'getPageBox: MaxPage:',a,result,e.message,fang_url
               return 0
    #print getCurrentTime(),'getPageBox: MaxPage:',a,result
    return result

def getFangMaxPagesMain():
     '''
     获取每个区域的最大页码数，如果超过00，可以考虑进一步细化查询条件，以爬取更多数据
     :return:
     '''
     regions = getRegions('http://sh.lianjia.com/ershoufang/', 'pudongxinqu')
     regions.reverse()
     maxpage=0
     while regions:
         #time.sleep(sleep_time)
         region = regions.pop()
         maxpage=getMaxPage('http://sh.lianjia.com/ershoufang/'+ region['code'])
         print  getCurrentTime(),region['name'],': '+str(maxpage)
         subRegions = getSubRegions('http://sh.lianjia.com/ershoufang/', region)
         subRegions.reverse()
         while subRegions:
            #time.sleep(sleep_time)
            subRegion=subRegions.pop()
            maxpage=getMaxPage('http://sh.lianjia.com/ershoufang/'+ subRegion['code'])
            if int(str(maxpage))>=100:
                print getCurrentTime(),region['name'],', '+subRegion['name'],': '+str(maxpage),'Over 100 pages'
            else:
                print getCurrentTime(), region['name'],', '+subRegion['name'],': '+str(maxpage)
     print getCurrentTime(),'getFangMaxPagesMain Sucess'

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
                #print cond['link']
                print 'a,d,l,p'
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
                print cond['link'],'a,d,l'
                result.append(cond)
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

def getRegions(fang_url, region):
    '''
    获取一级区域
    :param fang_url:
    :param region:
    :return:
    '''
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
    '''
    获取二级区域
    :param fang_url:
    :param region:
    :return:
    '''
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

def getLines(fang_url, region):
    '''
    获取地铁信息
    :param fang_url:
    :param region:
    :return:
    '''
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
    '''
    获取地铁站点信息
    :param fang_url:
    :param region:
    :return:
    '''
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

def getLianjiaList(fang_url):
    '''
    获取在售房源信息
    :param fang_url:
    :return:
    '''
    result = {}
    base_url = 'http://sh.lianjia.com'
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
            result['updated_date']=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            mySQL.insertData('lianjia_fang_list', result)
            print getCurrentTime(), u'在售：', result['fang_key'], result['quyu'], result['bankuai'], result['xiaoqu'], \
                                                 result['huxing'], result['price'], result['price_pre'], result['mianji']
            # fangList.append(result)
    return result

def getLianjiaTransList(fang_url):
    '''
    获取房源成交信息
    :param fang_url:
    :return:
    '''
    result = {}
    base_url = 'http://sh.lianjia.com'
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

def getXiaoquList(fang_url):
    '''
    获取小区信息，并爬取小区在售房源
    :param fang_url:
    :return:
    '''
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
                print getCurrentTime(), u'小区：', result['xiaoqu_key'], result['quyu'], result['bankuai'], result[
                    'xiaoqu_name'], result['age'], \
                    result['subway'], result['xiaoqu_url'], result['price'], result['onsale_num'], result['fang_url']
                getLianjiaList(result['fang_url'])
            except Exception, e:
                print  getCurrentTime(), u"Exception:%d: %s" % (e.args[0], e.args[1])
    return result

def getFangMain():
    '''
    获取房源明细，因为链家每个条件组合最多显示100页数据，这里依次按照一级区域、二级区域和页码循环爬取
    :return:
    '''
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
        while subRegions:
            try:
                subRegion = subRegions.pop()
                maxPage=0
                maxPage = getMaxPage( 'http://sh.lianjia.com/ershoufang/' + subRegion['code'])
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
        print getCurrentTime(), region['name'], ':', 'Scrapy Finished'
    print getCurrentTime(), 'getFangMain Scrapy Success'

def getTransMain():
    '''
    获取房源成交明细，因为链家每个条件组合最多显示100页数据，这里依次按照一级区域、二级区域和页码循环爬取
    '''

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
        #subRegions.reverse()
        while subRegions:
            try:
                subRegion = subRegions.pop()
                print getCurrentTime(), region['name'], ':', subRegion['name'], 'Scrapy Starting.....'
                for i in range(start_page, end_page):
                                 chengjiao_url = 'http://sh.lianjia.com/chengjiao/' + subRegion['code'] + '/d' + str(i)
                                 print getCurrentTime(), subRegion['name'], chengjiao_url
                                 time.sleep(sleep_time)
                                 fang = getLianjiaTransList(chengjiao_url)
                                 if len(fang) < 1:
                                    print getCurrentTime(), region['name'], ':', subRegion['name'], u' : getLianjiaTransList Scrapy Finished'
                                    break

                # for a in range(1,9):#面积
                #        for l in range(1,7):#户型
                #             for i in range(start_page, end_page):
                #                  linkdetail='a'+str(a)+'d' + str(i)+'l'+str(l)
                #                  chengjiao_url = 'http://sh.lianjia.com/chengjiao/' + subRegion['code'] +'/'+linkdetail
                #                  print getCurrentTime(), subRegion['name'], chengjiao_url
                #                  time.sleep(sleep_time)
                #                  fang = getLianjiaTransList(chengjiao_url)
                #                  if len(fang) < 1:
                #                     print getCurrentTime(), region['name'], ':', subRegion['name'], u' : getLianjiaTransList Scrapy Finished'
                #                     break
                print getCurrentTime(), region['name'], ':', subRegion['name'], 'Scrapy Finished'
            except Exception, e:
                print  getCurrentTime(), u"Exception:%s" % (e.message)
        print getCurrentTime(), region['name'], ':', 'Scrapy Finished'
    print getCurrentTime(), 'getTransMain Scrapy Success'

def getXiaoquMain():
    '''
    获取小区及房源明细，因为链家每个条件组合最多显示100页数据，这里依次按照一级区域、二级区域和页码循环爬取
    :return:
    '''
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
        while subRegions:
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
        print getCurrentTime(), region['name'], ':', 'Scrapy Finished'
    print getCurrentTime(), 'getXiaoquMain Scrapy Success'

def getLineMain():
    '''
    按照地铁线及站点爬取房源信息
    :return:
    '''
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
        print getCurrentTime(),linesStation['name'],':','linesStations:Scrapy Finished'
    print getCurrentTime(),line['name'],':','getLineMain:Scrapy Finished'

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
        while subRegions:
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

                for i in range(start_page, end_page):
                    chengjiao_url = 'http://sh.lianjia.com/chengjiao/' + subRegion['code'] + '/d' + str(i)
                    print getCurrentTime(), subRegion['name'], chengjiao_url
                    time.sleep(sleep_time)
                    fang = getLianjiaTransList(chengjiao_url)
                    if len(fang) < 1:
                        print getCurrentTime(), region['name'], ':', subRegion['name'], u' : getLianjiaTransList Scrapy Finished'
                        break

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
        print getCurrentTime(), region['name'], ':', 'Scrapy Finished'
    print getCurrentTime(), 'Lianjia Shanghai All Scrapy Success'

def main():
    print getCurrentTime(), 'Main Scrapy Starting'
    global mySQL, start_page, end_page, sleep_time,isproxy,header,proxy
    mySQL = MySQL()
    mySQL._init_('115.159.209.101', 'root', 'root', 'fang')
    start_page=1
    end_page = 101
    sleep_time=0.1
    isproxy=0
    header = randHeader()
    proxy = {"http": "http://110.37.84.147:8080", "https": "http://110.37.84.147:8080"}#这里需要替换成可用的代理IP
    #getFangMain()
    #getTransMain()
    #getXiaoquMain()
    #getLineMain()
    mainAll()
    url='http://sh.lianjia.com/chengjiao/'
    #print getMaxPage(url)
    #getFangMaxPagesMain()

if __name__ == "__main__":
    main()