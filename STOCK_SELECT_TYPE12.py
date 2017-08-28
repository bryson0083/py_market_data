# -*- coding: utf-8 -*-
"""
選股程式~STOCK_SELECT_TYPE12
STOCK_SELECT_TYPE12.py

@author: Bryson Xue
@target_rul: 
	
@Note: 
	選股條件，KD位於低檔(20以下)，日均量大於500張
@Ref:

"""

import talib
from talib import MA_Type
import sqlite3
import numpy as np
import pandas as pd
import xlsxwriter
import datetime
from datetime import date, timedelta
from dateutil import parser
from dateutil.relativedelta import relativedelta
import sys, os.path
import time
#import math, statistics

#K線型態判斷
def Stock_Ana(arg_stock, str_prev_date, str_today):
	sear_comp_id = arg_stock[0]
	#日線資料讀取
	strsql  = "select quo_date, open, high, low, close, vol from STOCK_QUO "
	strsql += "where "
	strsql += "SEAR_COMP_ID='" + sear_comp_id + "' and "
	strsql += "QUO_DATE between '" + str_prev_date + "' and '" + str_today + "' "
	strsql += "order by QUO_DATE "

	cursor = conn.execute(strsql)
	result = cursor.fetchall()

	none_flag = False
	if len(result) > 0:
		df = pd.DataFrame(result, columns = ['date','open','high','low','close','vol'])
		#print(df)
	else:
		none_flag = True

	#關閉cursor
	cursor.close()

	ls_result = []
	if none_flag == False:
		stock_vol = df['vol'].tail(6)/1000	#取近六天成交量，並由股換算為張
		avg_vol = stock_vol.mean()	#取平均

		#LIST轉換為Numpy Array
		npy_high = np.array(df['high'])
		npy_low = np.array(df['low'])
		npy_close = np.array(df['close'])

		last_close = df['close'].tail(1)
		prev_close = df['close'].shift(1).tail(1)

		# http://www.tadoc.org/indicator/STOCH.htm
		slowk, slowd = talib.STOCH (npy_high,
									npy_low,
									npy_close,
									fastk_period=9,
									slowk_period=3,
									slowk_matype=0,
									slowd_period=3,
									slowd_matype=0)

		#print("slowk=" + str(slowk[-1]))
		#print("slowd=" + str(slowd[-1]))

		# KD位於低檔(20以下)，日均量大於500張
		if (slowk[-1] <= 20) and \
		   (slowd[-1] <= 20) and \
		   (slowk[-1] > slowd[-1]) and \
		   (avg_vol > 500):
			#print("##" + arg_stock[0] + "##" + arg_stock[1]+ "##" + str(slowk[-1])+ "##" + str(slowd[-1]) + "##\n")
			ls_result = [[arg_stock[0],arg_stock[1],slowk[-1],slowd[-1]]]
			df_result = pd.DataFrame(ls_result, columns=['代號', '名稱', 'SLOWK', 'SLOWD'])

	if len(ls_result) == 0:
		df_result = pd.DataFrame()

	return df_result

############################################################################
# Main                                                                     #
############################################################################
print("Executing STOCK_SELECT_TYPE12...")

global err_flag
err_flag = False

#產生日期區間(當天日期，往前推90天)
today = datetime.datetime.now()
prev_date = today + timedelta(days=-90)

str_today = today.strftime("%Y%m%d")
str_prev_date = prev_date.strftime("%Y%m%d")

# 寫入LOG File
dt=datetime.datetime.now()
str_date = parser.parse(str(dt)).strftime("%Y%m%d")

name = "STOCK_SELECT_TYPE12_" + str_date + ".txt"
file = open(name, 'a', encoding = 'UTF-8')
tStart = time.time()#計時開始
file.write("\n\n\n*** LOG datetime  " + str(datetime.datetime.now()) + " ***\n")
file.write("偵測日期區間:" + str_prev_date + "~" + str_today + "\n")

#建立資料庫連線
conn = sqlite3.connect("market_price.sqlite")

strsql  = "select SEAR_COMP_ID,COMP_NAME, STOCK_TYPE from STOCK_COMP_LIST "
#strsql += "where SEAR_COMP_ID='0050.TW' "
strsql += "order by STOCK_TYPE, SEAR_COMP_ID "
#strsql += "limit 300 "

cursor = conn.execute(strsql)
result = cursor.fetchall()

df_result = pd.DataFrame()
if len(result) > 0:
	for stock in result:
		#print(stock)
		try:
			df = Stock_Ana(stock, str_prev_date, str_today)

			if len(df)>0:
				df_result = pd.concat([df_result, df], ignore_index=True)
		except Exception as e:
			err_flag = True
			print("Function Stock_Ana raise exception:\n" + str(e) + "\n")

#關閉cursor
cursor.close()

#關閉資料庫連線
conn.close()

no_data_flag = False
#資料進行排序
if len(df_result)>0:
	df_result = df_result.sort_values(by=['SLOWK', 'SLOWD', '代號'], ascending=[True, True, True])
else:
	print('無符合條件之股票.')
	no_data_flag = True

if no_data_flag == False:
	#結果寫入EXCEL檔
	file_name = 'STOCK_SELECT_TYPE12_' + str_date + '.xlsx'
	writer = pd.ExcelWriter(file_name, engine='xlsxwriter')
	df_result.to_excel(writer, sheet_name='stock', index=False)
	writer.save()

tEnd = time.time()#計時結束
file.write ("\n\n\n結轉耗時 %f sec\n" % (tEnd - tStart)) #會自動做進位
file.write("*** End LOG ***\n")

# Close File
file.close()

#若執行過程無錯誤，執行結束後刪除log檔案
if err_flag == False:
	os.remove(name)

print("End of prog.")