# -*- coding: utf-8 -*-
"""
Created on Tue Nov 15 13:26:03 2016

@author: Bryson Xue
@target_rul: http://mops.twse.com.tw/mops/web/t163sb04
"""
import requests
from bs4 import BeautifulSoup
import time
import sys
import pandas as pd
import re 
import datetime
from dateutil import parser
import sqlite3
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
#	if mmdd == "0405":
	if mmdd == "0322":
		yyy = str(int(yyy) - 1)
		qq = "04"
	elif mmdd == "0605":
		qq = "01"
	elif mmdd == "0905":
		qq = "02"
	elif mmdd == "1205":
		qq = "03"
	else:
		file.write("mode_c 未到批次結轉時間，執行結束...\n")
		sys.exit(mmdd + " No need to get data.")

	file.write("mode_c 自動抓取當季 yyyqq=" + yyy + qq + "\n")
	print("mode_c 自動抓取當季 yyyqq=" + yyy + qq)

     # 開始抓取資料
	MOPS_YQ_1(yyy, qq)



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
	MOPS_YQ_1(yyy, qq)



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
			MOPS_YQ_1(yyy, qq)

			q += 1



def proc_db(df, yyyy, qq):
    #print("this is def proc_db df ==>")
    #print(df)
    
    for i in range(0,len(df)):
        #print(str(df.index[i]))
        comp_id = str(df.iloc[i][0])
        comp_name = str(df.iloc[i][1])
        eps = str(df.iloc[i][2])
        eps = re.sub("[^-0-9^.]", "", eps) # 數字做格式控制
        
        print(comp_id + "  " + comp_name + "   " + eps + "\n")
        # 最後維護日期時間
        str_date = str(datetime.datetime.now())
        date_last_maint = parser.parse(str_date).strftime("%Y%m%d")
        time_last_maint = parser.parse(str_date).strftime("%H%M%S")
        prog_last_maint = "MOPS_YQ_1"

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
            sqlstr += " " + eps + ","
            sqlstr += "0,"
            sqlstr += "'" + date_last_maint + "',"
            sqlstr += "'" + time_last_maint + "',"
            sqlstr += "'" + prog_last_maint + "' "
            sqlstr += ") "
        else:
            sqlstr  = "update MOPS_YQ set "
            sqlstr += "eps=" + eps + ","
            sqlstr += "date_last_maint='" + date_last_maint + "',"
            sqlstr += "time_last_maint='" + time_last_maint + "',"
            sqlstr += "prog_last_maint='" + prog_last_maint + "' "
            sqlstr += "where "
            sqlstr += "COMP_ID='" + comp_id + "' and "
            sqlstr += "YYYY='" + yyyy + "' and "
            sqlstr += "QQ='" + qq + "' "

        try:
            cursor = conn.execute(sqlstr)
            conn.commit()
        except sqlite3.Error as er:
            file.write("\n@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n")
            file.write("\n資料 " + comp_id + "," + comp_name + "," + yyyy + qq + "," + eps + "\n")
            file.write("資料庫錯誤:\n" + er.args[0] + "\n")
            file.write("\n@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n")
            print ("\n@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n")
            print ("\n資料 " + comp_id + "," + comp_name + "," + yyyy + qq + "," + eps + "\n")
            print ("資料庫錯誤:\n" + er.args[0] + "\n")
            print ("\n@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n")

        # 關閉DB cursor
        cursor.close()
        
        
def MOPS_YQ_1(yyy, qq):
    headers = {'User-Agent':'User-Agent:Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36'}
    session = requests.session()
    
    # 隨機等待3~9秒的時間
    random_sec = randint(3,9)

    # 讀取查詢頁面
    r = session.get("http://mops.twse.com.tw/mops/web/t163sb04", headers=headers)

    print("Going to wait!!")
    print("Waiting sec=" + str(random_sec))
    time.sleep(random_sec)
    #time.sleep(5)
    
    # 拋送查詢條件到頁面，並取回查詢結果內容
    URL = 'http://mops.twse.com.tw/mops/web/ajax_t163sb04'
    """
    payload = {
           "encodeURIComponent": "1",
           "step": "1",
           "firstin": "1",
           "off": "1",
           "TYPEK": "sii",
           "year": "105",
           "season": "01"
           }    
    """
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

    try:
        table = sp.findAll('table', attrs={'class':'hasBorder'})  # tag-attrs
        #print(table)
        #print(type(table))
        #print(type(table[0]))
        #print(str(len(table)))
        #print(str(len(table[5].find_all('tr'))))
    except:
        sys.exit("@@@ 網頁讀取異常或該網頁無資料. @@@")
        
    yyyy = str(int(yyy) + 1911)
    tb_cnt = len(table) # 網頁上的表格總數
    i = 0
    #while i <= 1:
    while i < tb_cnt:
        # 讀取表格抬頭
        #print("tb_cnt=" + str(i) + "...\n")
        head = [[th.text for th in row.select('th')]
                for row in table[i].select('tr')]
        #print(head)
    
        # 讀取表格資料
        data = [[td.text for td in row.select('td')]  # http://stackoverflow.com/questions/14487526/turning-beautifulsoup-output-into-matrix
                for row in table[i].select('tr')]
        #print(data)   # list
    
        df = pd.DataFrame(data=data[1:len(data)], columns = head[0])
        #print(df)
        #print(df.loc[:,['公司代號', '公司名稱', '基本每股盈餘（元）']])
    
        df2 = df.loc[:,['公司代號', '公司名稱', '基本每股盈餘（元）']]
        #print("this is df2 ==>")
        #print(df2)

        # 資料庫存取
        proc_db(df2, yyyy, qq)
        i += 1    

        
############################################################################
# Main                                                                     #
############################################################################
print("Executing MOPS_YQ_1_V2...")

# 寫入LOG File
dt=datetime.datetime.now()

print("##############################################")
print("##      公開觀測資訊站~綜合損益表資料讀取   ##")
print("##                                          ##")
print("##                                          ##")
print("##   datetime: " + str(dt) +            "   ##")
print("##############################################")

str_date = str(dt)
str_date = parser.parse(str_date).strftime("%Y%m%d")

name = "MOPS_YQ_1_LOG_" + str_date + ".txt"
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

print ("End of prog...")