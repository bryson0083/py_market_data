# -*- coding: utf-8 -*-
"""
選股程式~STOCK_SELECT_TYPE13
STOCK_SELECT_TYPE12.py

@author: Bryson Xue
@target_rul: 
	
@Note: 
	選股條件，MA5、MA10、MA20均線糾結，最近一日收盤K線突破，日均量大於500張
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
import sys
import time
import math, statistics

#檢查list數值是否呈現遞減(mode='D')或遞增(mode='U')，回傳Y/N
def trend_chk(df, mode):
	i = 0
	flag = "Y"
	for elem in df:
		if i == 0:
			prev_elem = elem
			i += 1
			continue
		else:
			if (mode == "D") and (elem >= prev_elem):
				flag = "N"
				break
			elif (mode == "U") and (elem <= prev_elem):
				flag = "N"
				break
			else:
				prev_elem = elem
		i += 1
	return flag


#K線型態判斷
def Patt_Recon(arg_stock, str_prev_date, str_today):
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
		#LIST轉換為Numpy Array
		npy_close = np.array(df['close'])
		stock_vol = df['vol'].tail(6)/1000	#取近六天成交量，並由股換算為張
		last_vol = df['vol'].tail(1)/1000	#取最近一天成交量，並換算為張
		last_open = df['open'].tail(1)
		last_close = df['close'].tail(1)
		prev_close = df['close'].shift(1).tail(1)
		avg_vol = stock_vol.mean()	#取平均

		#最近一天交易日，成交量漲幅
		rt = 0
		if avg_vol > 0:
			rt = (last_vol.iloc[0] - avg_vol) / avg_vol * 100

		#最近一天交易日，K棒本體漲幅
		kbody_chg = 0
		if last_open.iloc[0] > 0:
			kbody_chg = (last_close.iloc[0] - last_open.iloc[0]) / last_open.iloc[0] * 100

		#最近一天交易日當天漲幅
		rise_rt = 0
		if prev_close.iloc[0] > 0:
			rise_rt = (last_close.iloc[0] - prev_close.iloc[0]) / prev_close.iloc[0] * 100

		#計算5MA
		ma5 = talib.MA(npy_close, timeperiod=5, matype=0)
		df['ma5'] = ma5

		#計算10MA
		ma10 = talib.MA(npy_close, timeperiod=10, matype=0)
		df['ma10'] = ma10

		#計算20MA
		ma20 = talib.MA(npy_close, timeperiod=20, matype=0)
		df['ma20'] = ma20

		#計算50MA
		ma50 = talib.MA(npy_close, timeperiod=50, matype=0)
		df['ma50'] = ma50

		#最近一天交易日20MA值
		last_20ma = df['ma20'].tail(1)

		#最近一天交易日50MA值
		last_50ma = df['ma50'].tail(1)

		#三條MA線近六天的數值，全部丟到一個list下，一起計算變異數
		ls_ma = []
		ls_ma.extend(df['ma5'].tail(6))
		ls_ma.extend(df['ma10'].tail(6))
		ls_ma.extend(df['ma20'].tail(6))
		var_val = statistics.variance(ls_ma)

		ma10_slice = df['ma10'].tail(6)			#往前取6天10MA值
		chk_u_yn1 = trend_chk(ma10_slice, 'U')	#判斷是否uptrend

		ma50_slice = df['ma50'].tail(6)			#往前取6天50MA值
		chk_u_yn2 = trend_chk(ma50_slice, 'U')	#判斷是否uptrend


		#三條均線變異數介於0~1間、最近一天成交量相對平均量成長20%、紅K棒實體2%以上(或當天上漲2%以上)、
		#最近一天收盤價在20MA跟50MA之上、成交量均量需大於500張
		if (var_val > 0 and var_val < 1) and \
		   rt >= 20 and \
		   (kbody_chg >= 2 or rise_rt >= 2) and \
		   (last_close.iloc[0] > last_20ma.iloc[0]) and \
		   (last_close.iloc[0] > last_50ma.iloc[0]) and \
		   chk_u_yn1 == 'Y' and \
		   chk_u_yn2 == 'Y' and \
		   avg_vol > 500:
			ls_result = [[arg_stock[0],arg_stock[1],var_val,rt]]
			df_result = pd.DataFrame(ls_result, columns=['stock_id', 'stock_name', 'var', 'burst_rt'])

	if len(ls_result) == 0:
		df_result = pd.DataFrame()

	return df_result

############################################################################
# Main                                                                     #
############################################################################
#產生日期區間(當天日期，往前推90天)
today = datetime.datetime.now()
prev_date = today + timedelta(days=-90)
#prev_date = today + timedelta(days=-600)

str_today = today.strftime("%Y%m%d")
str_prev_date = prev_date.strftime("%Y%m%d")

# 寫入LOG File
dt=datetime.datetime.now()
str_date = parser.parse(str(dt)).strftime("%Y%m%d")

name = "STOCK_SELECT_TYPE13_" + str_date + ".txt"
file = open(name, 'a', encoding = 'UTF-8')
tStart = time.time()#計時開始
file.write("\n\n\n*** LOG datetime  " + str(datetime.datetime.now()) + " ***\n")
file.write("偵測日期區間:" + str_prev_date + "~" + str_today + "\n")

#建立資料庫連線
conn = sqlite3.connect("market_price.sqlite")

strsql  = "select SEAR_COMP_ID,COMP_NAME, STOCK_TYPE from STOCK_COMP_LIST "
#strsql += "where SEAR_COMP_ID='0050.TW' "
strsql += "order by STOCK_TYPE, SEAR_COMP_ID "

cursor = conn.execute(strsql)
result = cursor.fetchall()

df_result = pd.DataFrame()
if len(result) > 0:
	for stock in result:
		#print(stock)
		df = Patt_Recon(stock, str_prev_date, str_today)

		if len(df)>0:
			df_result = pd.concat([df_result, df], ignore_index=True)

#關閉cursor
cursor.close()

#關閉資料庫連線
conn.close()

#資料進行排序
if len(df_result)>0:
	df_result = df_result.sort_values(by=['var', 'burst_rt'], ascending=[True, False])

#結果寫入EXCEL檔
file_name = 'STOCK_SELECT_TYPE13_' + str_date + '.xlsx'
writer = pd.ExcelWriter(file_name, engine='xlsxwriter')
df_result.to_excel(writer, sheet_name='stock', index=False)
writer.save()

tEnd = time.time()#計時結束
file.write ("\n\n\n結轉耗時 %f sec\n" % (tEnd - tStart)) #會自動做進位
file.write("*** End LOG ***\n")

# Close File
file.close()

print("End of prog.")