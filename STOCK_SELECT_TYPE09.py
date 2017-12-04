# -*- coding: utf-8 -*-
"""
技術分析選股 TYPE 9

@author: Bryson Xue

@target_rul: 

@Note: 
	上市櫃股票，3、5、8日均線糾結選股(當天大漲) + 個股當日三大法人買賣超資訊
	並加上以上過濾條件
	A. 3條均線變異數介於0~1間(變異數值太大，線就離太遠了)
	B. 近六天成交量平均量成長30%以上，且日均量500張以上
	C. 當天上漲3%以上
	D. 最近一交易日，收盤價在8MA跟50MA之上

	買賣超代碼
	A類: 三大法人均買超
	B類: 外資、投信買超
	C類: 三大法人均賣超
	D類: 外資、投信賣超
	E類: 三大法人行為出現轉折
	Z類: 無明顯趨向(盤整)

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
import math, statistics

#個股資料讀取判斷
def Stock_Ana(arg_stock, str_prev_date, str_today):
	global err_flag
	global file
	global conn
	
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

		#最近一天交易日當天漲幅
		rise_rt = 0
		if prev_close.iloc[0] > 0:
			rise_rt = (last_close.iloc[0] - prev_close.iloc[0]) / prev_close.iloc[0] * 100

		#計算3MA
		ma3 = talib.MA(npy_close, timeperiod=3, matype=0)
		df['ma3'] = ma3

		#計算5MA
		ma5 = talib.MA(npy_close, timeperiod=5, matype=0)
		df['ma5'] = ma5

		#計算8MA
		ma8 = talib.MA(npy_close, timeperiod=8, matype=0)
		df['ma8'] = ma8

		#計算50MA
		ma50 = talib.MA(npy_close, timeperiod=50, matype=0)
		df['ma50'] = ma50

		#最近一天交易日8MA值
		last_8ma = df['ma8'].tail(1)

		#最近一天交易日50MA值
		last_50ma = df['ma50'].tail(1)

		#三條MA線近六天的數值，全部丟到一個list下，一起計算變異數
		ls_ma = []
		ls_ma.extend(df['ma3'].tail(6))
		ls_ma.extend(df['ma5'].tail(6))
		ls_ma.extend(df['ma8'].tail(6))
		var_val = statistics.variance(ls_ma)

		#三條均線變異數介於0~1間、最近一天成交量相對平均量成長30%、當天上漲3%以上、
		#最近一天收盤價在8MA跟50MA之上、成交量均量需大於500張
		if (var_val > 0 and var_val < 1) and \
		   rt >= 30 and \
		   rise_rt >= 3 and \
		   (last_close.iloc[0] > last_8ma.iloc[0]) and \
		   (last_close.iloc[0] > last_50ma.iloc[0]) and \
		   avg_vol > 500:

			#讀取個股籌碼資料
			strsql  = "select FR_BAS_CNT, IT_BAS_CNT, DE_BAS_CNT, RANK "
			strsql += "from REPORT_CHIP_ANA "
			strsql += "where "
			strsql += "SEAR_COMP_ID='" + sear_comp_id + "' "

			cursor = conn.execute(strsql)
			result = list(cursor.fetchone())

			if len(result) > 0:
				fr_bas_cnt = str(result[0])
				it_bas_cnt = str(result[1])
				de_bas_cnt = str(result[2])
				rank = result[3]
			else:
				fr_bas_cnt = "0"
				it_bas_cnt = "0"
				de_bas_cnt = "0"
				rank = " "

			#關閉cursor
			cursor.close()

			#print("##" + arg_stock[0] + "##" + arg_stock[1] + "##" + fr_bas_cnt + "##" + it_bas_cnt + "##" + de_bas_cnt + "##" + rank + "##\n")

			ls_result = [[arg_stock[0],arg_stock[1],var_val,rt,fr_bas_cnt,it_bas_cnt,de_bas_cnt,rank]]
			df_result = pd.DataFrame(ls_result, columns=['代號', '名稱', 'var', 'burst_rt','外資買賣超天數','投信買賣超天數','自營商買賣超天數','類別'])

	if len(ls_result) == 0:
		df_result = pd.DataFrame()

	return df_result

def MAIN_STOCK_SELECT_TYPE09():
	global err_flag
	global file
	global conn
	err_flag = False

	print("Executing " + os.path.basename(__file__) + "...")

	#產生日期區間(當天日期，往前推90天)
	today = datetime.datetime.now()
	prev_date = today + timedelta(days=-90)

	str_today = today.strftime("%Y%m%d")
	str_prev_date = prev_date.strftime("%Y%m%d")

	print_dt = str(str_today) + (' ' * 22)
	print("##############################################")
	print("##            技術分析選股 TYPE 09          ##")
	print("##                                          ##")
	print("##                                          ##")
	print("##  datetime: " + print_dt +               "##")
	print("##############################################")
	print("\n\n")
	print("資料涵蓋範圍: " + str_prev_date + "~" + str_today + "\n")

	# 寫入LOG File
	dt=datetime.datetime.now()
	str_date = parser.parse(str(dt)).strftime("%Y%m%d")

	name = "STOCK_SELECT_TYPE09_" + str_date + ".txt"
	file = open(name, 'a', encoding = 'UTF-8')
	tStart = time.time()#計時開始
	file.write("\n\n\n*** LOG datetime  " + str(datetime.datetime.now()) + " ***\n")
	file.write("Executing " + os.path.basename(__file__) + "...\n")
	file.write("資料涵蓋範圍: " + str_prev_date + "~" + str_today + "\n")

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

	#資料進行排序
	if len(df_result)>0:
		df_result = df_result.sort_values(by=['類別', 'var', 'burst_rt'], ascending=[True, True, False])
	else:
		df_result = pd.DataFrame([], columns=['今日無符合條件之股票.'])
		print('今日無符合條件之股票.')

	#結果寫入EXCEL檔
	file_name = 'STOCK_SELECT_TYPE09_' + str_date + '.xlsx'
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

	print("\n\n技術分析選股TYPE 09執行結束...\n\n\n")

if __name__ == '__main__':
	MAIN_STOCK_SELECT_TYPE09()