# -*- coding: utf-8 -*-
"""
Created on Tue Nov 15 13:26:03 2016

@author: Bryson Xue
@target_rul: http://mops.twse.com.tw/mops/web/t163sb05
"""
import requests
from bs4 import BeautifulSoup
import time
import pandas as pd
import sqlite3
import re 
import datetime
from dateutil import parser
import sys
from random import randint


# 自動結轉資料
def mode_c():
	#str_date = "2016-04-05"
	str_date = str(datetime.datetime.now())
	
	# 轉換日期為C8格式字串
	dt_c8 = parser.parse(str_date).strftime("%Y%m%d")
	yyyy = dt_c8[0:4]
	#print(yyyy)

	# 轉西元年為民國年
	yyy = str(int(yyyy) - 1911)
	#print(yyy)

	# 取出月日的部分
	mmdd = dt_c8[4:]
	#print(mmdd)

	# 註：依證券交易法第36條及證券期貨局相關函令規定，財務報告申報期限如下：
	# 1.一般行業申報期限：第一季為5月15日，第二季為8月14日，第三季為11月14日，年度為3月31日。
	# 2.金控業申報期限：第一季為5月30日，第二季為8月31日，第三季為11月29日，年度為3月31日。
	# 3.銀行及票券業申報期限：第一季為5月15日，第二季為8月31日，第三季為11月14日，年度為3月31日。
	# 4.保險業申報期限：第一季為5月15日，第二季為8月31日，第三季為11月14日，年度為3月31日。
	# 5.證券業申報期限：第一季為5月15日，第二季為8月31日，第三季為11月14日，年度為3月31日。

	# 只在以下特定幾天結轉季報資料
	if mmdd == "0405":
		yyy = str(int(yyy) - 1)
		qq = "04"
	elif mmdd == "0605":
		qq = "01"
	elif mmdd == "0905":
		qq = "02"
	elif mmdd == "1209":
		qq = "03"
	else:
		file.write("mode_c 未到批次結轉時間，執行結束...\n")
		sys.exit(mmdd + " No need to get data.")

	file.write("mode_c 自動抓取當季 yyyqq=" + yyy + qq + "\n")
	print("mode_c 自動抓取當季 yyyqq=" + yyy + qq)

     # 開始抓取資料
	MOPS_YQ_2(yyy, qq)



# 手動輸入條件結轉資料
def mode_h():
	yyyy = str(input("輸入抓取資料年分(YYYY):"))
	qq = str(input("輸入抓取資料季別(QQ):"))

	# 轉西元年為民國年
	yyy = str(int(yyyy) - 1911)

	# 寫入LOG File
	file.write("mode_h 手動結轉 yyyqq=" + yyy + qq + "\n")
	print("mode_h 手動結轉 yyyqq=" + yyy + qq)

      # 開始抓取資料
	MOPS_YQ_2(yyy, qq)



# 跑特定區間，結轉資料(自行修改參數條件)
def mode_a():
	for y in range(2015,2017,1):
		#print("y=" + str(y))
		yyy = str(y - 1911)
		q = 1
		while q <= 4:
			if q == 1:
				qq = "01"
			elif q == 2:
				qq = "02"
			elif q == 3:
				qq = "03"
			else:
				qq = "04"

			file.write("mode_a 特定區間，結轉yyyqq=" + yyy + qq + "\n")
			print("mode_a 特定區間，結轉yyyqq=" + yyy + qq)
   
                 # 開始抓取資料
			MOPS_YQ_2(yyy, qq)

			q += 1


def proc_db(df, yyyy, qq):
    #print("this is def proc_db df ==>")
    #print(df)
    
    for i in range(0,len(df)):
        #print(str(df.index[i]))
        comp_id = str(df.iloc[i][0])
        comp_name = str(df.iloc[i][1])
        bvps = str(df.iloc[i][2]) # 每股參考淨值
        bvps = re.sub("[^-0-9^.]", "", bvps) # 數字做格式控制
        
        print(comp_id + "  " + comp_name + "   " + bvps + "\n")
        # 最後維護日期時間
        str_date = str(datetime.datetime.now())
        date_last_maint = parser.parse(str_date).strftime("%Y%m%d")
        time_last_maint = parser.parse(str_date).strftime("%H%M%S")
        prog_last_maint = "MOPS_YQ_2"

        sqlstr = "select count(*) from MOPS_YQ "
        sqlstr = sqlstr + "where "
        sqlstr = sqlstr + "COMP_ID='" + comp_id + "' and "
        sqlstr = sqlstr + "YYYY='" + yyyy + "' and "
        sqlstr = sqlstr + "QQ='" + qq + "' "

        #print(sqlstr)
        cursor = conn.execute(sqlstr)
        result = cursor.fetchone()

        if result[0] == 0:
            sqlstr  = "insert into MOPS_YQ values ("
            sqlstr += "'" + comp_id + "',"
            sqlstr += "'" + comp_name + "',"
            sqlstr += "'" + yyyy + "',"
            sqlstr += "'" + qq + "',"
            sqlstr += "0,"
            sqlstr += " " + bvps + ","
            sqlstr += "'" + date_last_maint + "',"
            sqlstr += "'" + time_last_maint + "',"
            sqlstr += "'" + prog_last_maint + "' "
            sqlstr += ") "
        else:
            sqlstr  = "update MOPS_YQ set "
            sqlstr += "BVPS=" + bvps + ","
            sqlstr += "DATE_LAST_MAINT='" + date_last_maint + "',"
            sqlstr += "TIME_LAST_MAINT='" + time_last_maint + "',"
            sqlstr += "PROG_LAST_MAINT='" + prog_last_maint + "' "
            sqlstr += "where "
            sqlstr += "COMP_ID='" + comp_id + "' and "
            sqlstr += "YYYY='" + yyyy + "' and "
            sqlstr += "QQ='" + qq + "' "

        try:
            cursor = conn.execute(sqlstr)
            conn.commit()
        except sqlite3.Error as er:
            file.write("\n@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n")
            file.write("\n資料 " + comp_id + "," + comp_name + "," + yyyy + qq + "," + bvps + "\n")
            file.write("資料庫錯誤:\n" + er.args[0] + "\n")
            file.write("\n@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n")
            print ("\n@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n")
            print ("\n資料 " + comp_id + "," + comp_name + "," + yyyy + qq + "," + bvps + "\n")
            print ("資料庫錯誤:\n" + er.args[0] + "\n")
            print ("\n@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n")

        # 關閉DB cursor
        cursor.close()


def MOPS_YQ_2(yyy, qq):
    yyyy = str(int(yyy) + 1911)
    
    headers = {'User-Agent':'User-Agent:Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36'}
    session = requests.session()
    
    # 讀取查詢頁面
    r = session.get("http://mops.twse.com.tw/mops/web/t163sb05", headers=headers)
    
    # 隨機等待3~9秒的時間
    random_sec = randint(3,9)
    
    print("Going to wait!!")
    print("Waiting sec=" + str(random_sec))
    time.sleep(random_sec)
    
    URL = 'http://mops.twse.com.tw/mops/web/ajax_t163sb05'
    
    payload = {
    "encodeURIComponent": "1",
    "step": "1",
    "firstin": "1",
    "off": "1",
    "TYPEK": "sii",
    "year": yyy,
    "season": qq
    }
    
    r = requests.post(URL, data=payload, headers=headers)
    r.encoding = "utf-8"
    
    sp = BeautifulSoup(r.text, 'html.parser')
    #print(sp)
    
    tr_hd = sp.findAll('tr', attrs={'class':'tblHead'})
    tr_odd = sp.findAll('tr', attrs={'class':'odd'})  # tag-attrs
    tr_even = sp.findAll('tr', attrs={'class':'even'}) 
    
    #print(tr[1])
    #print(str(tr_odd[0]))
    #sys.exit("test end...\n")
    
    ###############################################################
    # tr_even處理                                                  #
    ###############################################################
    tr_even_cnt = len(tr_even)
    print("tr_even_cnt=" + str(tr_even_cnt) + "\n\n")
    
    # 處理 2801 彰化銀行 ~ 2849 安泰銀行
    #print([th.text for th in tr_hd[0].select('th')])
    #print([td.text for td in tr_even[0].select('td')])
    #print([td.text for td in tr_even[8].select('td')])
    i=0
    ls=[]
    while i <= 8:
        head = [th.text for th in tr_hd[0].select('th')]
        #print(head)
        data = [td.text for td in tr_even[i].select('td')]
        #print(data)
        ls.append(data)
    
        tr_even_cnt -= 1
        i += 1
    
    #print(ls)
    df = pd.DataFrame(ls, columns = head)
    #print(df)
    df2 = df.loc[:,['公司代號', '公司名稱', '每股參考淨值']]
    #print(df2)
    proc_db(df2, yyyy, qq)
    
    # 處理 1101 台泥 ~ 9958 世紀鋼構
    #print([th.text for th in tr_hd[2].select('th')])
    #print([td.text for td in tr_even[9].select('td')])
    #print([td.text for td in tr_even[859].select('td')])
    i=9
    ls=[]
    while i <= 859:
        head = [th.text for th in tr_hd[2].select('th')]
        #print(head)
        data = [td.text for td in tr_even[i].select('td')]
        #print(data)
        ls.append(data)
    
        tr_even_cnt -= 1
        i += 1
    
    #print(ls)
    df = pd.DataFrame(ls, columns = head)
    #print(df)
    df2 = df.loc[:,['公司代號', '公司名稱', '每股參考淨值']]
    #print(df2)
    proc_db(df2, yyyy, qq)
    
    
    # 處理 2816 旺旺保險 ~ 2867 三商美邦
    #print([th.text for th in tr_hd[4].select('th')])
    #print([td.text for td in tr_even[860].select('td')])
    #print([td.text for td in tr_even[866].select('td')])
    i=860
    ls=[]
    while i <= 866:
        head = [th.text for th in tr_hd[4].select('th')]
        #print(head)
        data = [td.text for td in tr_even[i].select('td')]
        #print(data)
        ls.append(data)
    
        tr_even_cnt -= 1
        i += 1
    
    #print(ls)
    df = pd.DataFrame(ls, columns = head)
    #print(df)
    df2 = df.loc[:,['公司代號', '公司名稱', '每股參考淨值']]
    #print(df2)
    proc_db(df2, yyyy, qq)
    
    #print("final tr_even_cnt=" + str(tr_even_cnt) + "\n\n")
    if tr_even_cnt != 0:
        file.write("Err tr_even_cnt=" + str(tr_even_cnt) + "\n\n")
    
    ###############################################################
    # tr_odd處理                                                  #
    ###############################################################
    tr_odd_cnt = len(tr_odd)
    print("tr_odd_cnt=" + str(tr_odd_cnt) + "\n\n")
    
    # 處理 2855 統一證券 ~ 6005 群益證
    i=0
    ls=[]
    while i <= 2:
        head = [th.text for th in tr_hd[1].select('th')]
        #print(head)
        data = [td.text for td in tr_odd[i].select('td')]
        #print(data)
        ls.append(data)
    
        tr_odd_cnt -= 1
        i += 1
    
    #print(ls)
    df = pd.DataFrame(ls, columns = head)
    #print(df)
    df2 = df.loc[:,['公司代號', '公司名稱', '每股參考淨值']]
    #print(df2)
    proc_db(df2, yyyy, qq)
    
    # 處理 2880 華南金 ~ 5880 合庫金
    i=3
    ls=[]
    while i <= 16:
        head = [th.text for th in tr_hd[3].select('th')]
        #print(head)
        data = [td.text for td in tr_odd[i].select('td')]
        #print(data)
        ls.append(data)
        
        tr_odd_cnt -= 1
        i += 1
    
    #print(ls)
    df = pd.DataFrame(ls, columns = head)
    #print(df)
    df2 = df.loc[:,['公司代號', '公司名稱', '每股參考淨值']]
    #print(df2)
    proc_db(df2, yyyy, qq)
    
    # 處理 1409 新纖 ~ 2905 三商
    i=17
    ls=[]
    while i <= 20:
        head = [th.text for th in tr_hd[5].select('th')]
        #print(head)
        data = [td.text for td in tr_odd[i].select('td')]
        #print(data)
        ls.append(data)
        
        tr_odd_cnt -= 1
        i += 1
    
    #print(ls)
    df = pd.DataFrame(ls, columns = head)
    #print(df)
    df2 = df.loc[:,['公司代號', '公司名稱', '每股參考淨值']]
    #print(df2)
    proc_db(df2, yyyy, qq)
    
    #print("final tr_odd_cnt=" + str(tr_odd_cnt) + "\n\n")
    if tr_odd_cnt != 0:
        file.write("Err tr_odd_cnt=" + str(tr_odd_cnt) + "\n\n")        
        


        
########################################################
#  program Main                                        #
########################################################
# 寫入LOG File
dt=datetime.datetime.now()

print("##############################################")
print("##      公開觀測資訊站~資產負債表資料讀取   ##")
print("##                                          ##")
print("##                                          ##")
print("##   datetime: " + str(dt) +            "   ##")
print("##############################################")

str_date = str(dt)
str_date = parser.parse(str_date).strftime("%Y%m%d")

name = "MOPS_YQ_2_LOG_" + str_date + ".txt"
file = open(name, 'a', encoding = 'UTF-8')

tStart = time.time()#計時開始
file.write("\n\n\n*** LOG datetime  " + str(datetime.datetime.now()) + " ***\n")

# 建立資料庫連線
conn = sqlite3.connect('market_price.sqlite')

try:
	run_mode = sys.argv[1]
	run_mode = run_mode.upper()
except Exception as e:
	run_mode = "C"

print("you choose mode " + run_mode)

if run_mode == "C":
	file.write("mode_c 自動抓取當季，結轉資料...\n")
	mode_c()
elif run_mode == "H":
	file.write("mode_h 手動輸入區間，結轉資料...\n")
	mode_h()
elif run_mode == "A":
	file.write("mode_a 跑特定區間，結轉資料...\n")
	mode_a()
else:
	file.write("模式錯誤，結束程式...\n")
	sys.exit("模式錯誤，結束程式...\n")

tEnd = time.time()#計時結束
file.write ("\n\n\n結轉耗時 %f sec\n" % (tEnd - tStart)) #會自動做進位
file.write("*** End LOG ***\n")

# 資料庫連線關閉
conn.close()

# Close File
file.close()

print("正常執行結束...")