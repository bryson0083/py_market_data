# 上市公司清單讀取(STOCK_COMP_LIST_V2)
#   Target: http://isin.twse.com.tw/isin/C_public.jsp?strMode=2
#   Version: 0.1
#   Date: 2016-11-15
#   Author: Bryson Xue
#   To-Do List: 

import requests as rs
#import lxml.html as LH
from bs4 import BeautifulSoup
import pandas as pd

from dateutil import parser
#from dateutil.parser import parse

import datetime
#import os.path
import sqlite3
import time
import sys

############################################################################
# Main                                                                     #
############################################################################
print("Executing STOCK_COMP_LIST_V2...")

# 寫入LOG File
dt=datetime.datetime.now()
print("##############################################")
print("##      台灣證券交易所~上市公司清單讀取     ##")
print("##                                          ##")
print("##                                          ##")
print("##   datetime: " + str(dt) +            "   ##")
print("##############################################")
str_date = str(dt)
str_date = parser.parse(str_date).strftime("%Y%m%d")

name = "STOCK_COMP_LIST_" + str_date + ".txt"
file = open(name, 'a', encoding = 'UTF-8')

# 設定瀏覽器header
headers = {
'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.94 Safari/537.36'
} 

# 目標網址
url = 'http://isin.twse.com.tw/isin/C_public.jsp?strMode=2'

res = rs.get(url, headers = headers)
res.encoding = 'big5' # 'utf-8'  #  http://sh3ll.me/2014/06/18/python-requests-encoding/
#print(res.text)

sp = BeautifulSoup(res.text, 'html.parser')
#print(sp)

tStart = time.time()#計時開始
file.write("\n\n\n*** LOG datetime  " + str(datetime.datetime.now()) + " ***\n")

try:
    table = sp.find('table', attrs={'class':'h4'})  # tag-attrs
    #print(table)
except:
    file.write("@@@ 網頁讀取異常或該網頁無資料. @@@\n")
    file.close()
    sys.exit("@@@ 網頁讀取異常或該網頁無資料. @@@")
    
data = [[td.text for td in row.select('td')]  # http://stackoverflow.com/questions/14487526/turning-beautifulsoup-output-into-matrix
         for row in table.select('tr')]
#print(data)   # list

df = pd.DataFrame(data=data[1:len(data)], columns = data[0])
#print(df)

# 建立資料庫連線
conn = sqlite3.connect('market_price.sqlite')

for i in range(1,len(df)):
    #print(str(df.index[i]))
    
    # 排除"沒有國際證券辨識號碼(ISIN Code)"的資料
    isin_code = str(df.iloc[i][1])
    if isin_code != "None":
        t0 = df.iloc[i][0]
        #print("t0=" + str(t0) + "  " + str(df.iloc[i][1]) + "\n")
        
        # 從t0拆解出代號跟公司名稱
        t = str(t0).split("\u3000") 
        comp_id = str(t[0]).strip(" ")	# 公司代號
        comp_name = str(t[1]).strip(" ")	# 公司名稱
        ipo_date = str(df.iloc[i][2]).replace("/","") # 公開上市日期
        industry = str(df.iloc[i][4])	# 產業類別
        #print(comp_id+" "+comp_name+" "+ipo_date+" "+industry+"\n")
    
        # 最後維護日期時間
        str_date = str(datetime.datetime.now())
        date_last_maint = parser.parse(str_date).strftime("%Y%m%d")
        time_last_maint = parser.parse(str_date).strftime("%H%M%S")
        prog_last_maint = "STOCK_COMP_LIST"
        
        # 上市認購(售)權證的資料就不需要了(用代碼前兩碼來判斷)
        if (comp_id[0:2] != "03" or \
            comp_id[0:2] != "04" or \
            comp_id[0:2] != "05" or \
            comp_id[0:2] != "06" or \
            comp_id[0:2] != "07" or \
            comp_id[0:2] != "08" \
        ):
            sqlstr = "select count(*) from STOCK_COMP_LIST "
            sqlstr = sqlstr + "where "
            sqlstr = sqlstr + "COMP_ID='" + comp_id + "' "
            
            cursor = conn.execute(sqlstr)
            result = cursor.fetchone()
            
            if result[0] == 0:
                sqlstr  = "insert into STOCK_COMP_LIST ("
                sqlstr += "COMP_ID,SEAR_COMP_ID,COMP_NAME,"
                sqlstr += "IPO_DATE,LATEST_QUO_DATE,INDUSTRY,"
                sqlstr += "DATE_LAST_MAINT,TIME_LAST_MAINT,"
                sqlstr += "PROG_LAST_MAINT"
                sqlstr += ") values ("
                sqlstr += "'" + comp_id + "',"
                sqlstr += "'" + comp_id + ".TW',"
                sqlstr += "'" + comp_name + "',"
                sqlstr += "'" + ipo_date + "',"
                sqlstr += "' ',"
                sqlstr += "'" + industry + "',"
                sqlstr += "'" + date_last_maint + "',"
                sqlstr += "'" + time_last_maint + "',"
                sqlstr += "'" + prog_last_maint + "'"
                sqlstr += ")"
                
                try:
                    file.write("新增股票:"+comp_id+" "+comp_name+" "+ \
                               ipo_date+" "+industry+"\n")
                    cursor = conn.execute(sqlstr)
                    conn.commit()
                except sqlite3.Error as er:
                    file.write("資料庫錯誤:\n" + er.args[0] + "\n")
                    print("er:" + er.args[0])
    
        # 關閉cursor
        cursor.close()

    # 測試限制筆數用
    #if i==50:
    #    break

# 關閉資料庫連線
conn.close()    

tEnd = time.time()#計時結束
file.write ("\n\n\n結轉耗時 %f sec\n" % (tEnd - tStart)) #會自動做進位
file.write("*** End LOG ***\n")

# Close File
file.close()

print ("End of prog...")