# -*- coding: utf-8 -*-
"""
上市櫃股價月線計算

@author: Bryson Xue

@target_rul: 

@Note: 
    基於上市櫃股票之日線報價數據，計算月線數據

"""
import sqlite3
import pandas as pd
import datetime
from dateutil import parser
from dateutil.relativedelta import relativedelta
import sys
import time
import os

def READ_DAY_QUO(arg_sear_comp_id, arg_date_st, arg_date_ed):
	global err_flag
	global file
	global conn

	#讀取月線開盤價
	strsql  = "select OPEN from STOCK_QUO "
	strsql += "where "
	strsql += "SEAR_COMP_ID = '" + arg_sear_comp_id + "' and "
	strsql += "QUO_DATE >= '" + arg_date_st + "' and "
	strsql += "QUO_DATE <= '" + arg_date_ed + "' and "
	strsql += "CLOSE > 0 "
	strsql += "order by QUO_DATE "
	strsql += "limit 1 "

	cursor = conn.execute(strsql)
	result = cursor.fetchone()

	if result[0] is not None:
		mon_open = result[0]
	else:
		mon_open = 0

	#關閉cursor
	cursor.close()

	#讀取月線收盤價，與當月最後收盤日期
	strsql  = "select CLOSE, QUO_DATE from STOCK_QUO "
	strsql += "where "
	strsql += "SEAR_COMP_ID = '" + arg_sear_comp_id + "' and "
	strsql += "QUO_DATE >= '" + arg_date_st + "' and "
	strsql += "QUO_DATE <= '" + arg_date_ed + "' and "
	strsql += "CLOSE > 0 "
	strsql += "order by QUO_DATE desc "
	strsql += "limit 1 "

	cursor = conn.execute(strsql)
	result = cursor.fetchone()

	if result[0] is not None:
		mon_close = result[0]
		mon_close_dt = result[1]
	else:
		mon_close = 0
		mon_close_dt = ""

	#關閉cursor
	cursor.close()

	#讀取月線最低價、最高價、成交量
	strsql  = "select min(LOW), max(HIGH), sum(VOL) from STOCK_QUO "
	strsql += "where "
	strsql += "SEAR_COMP_ID = '" + arg_sear_comp_id + "' and "
	strsql += "QUO_DATE >= '" + arg_date_st + "' and "
	strsql += "QUO_DATE <= '" + arg_date_ed + "' and "
	strsql += "CLOSE > 0 "	

	cursor = conn.execute(strsql)
	result = cursor.fetchone()

	if result[0] is not None:
		mon_low = result[0]
		mon_high = result[1]
		mon_vol = result[2]
	else:
		mon_low = 0
		mon_high = 0
		mon_vol = 0

	#關閉cursor
	cursor.close()

	#print(arg_sear_comp_id + ",open=" + str(mon_open) + ", close=" + str(mon_close) + ",high=" + str(mon_high) + ",low=" + str(mon_low) + ",vol=" + str(mon_vol) + ",lat_dt=" + mon_close_dt)

	# 最後維護日期時間
	str_date = str(datetime.datetime.now())
	date_last_maint = parser.parse(str_date).strftime("%Y%m%d")
	time_last_maint = parser.parse(str_date).strftime("%H%M%S")
	prog_last_maint = "QUO_MONTH"

	#資料存檔
	strsql  = "delete from STOCK_QUO_MONTH "
	strsql += "where "
	strsql += "SEAR_COMP_ID = '" + arg_sear_comp_id + "' and "
	strsql += "QUO_DATE >= '" + arg_date_st + "' and "
	strsql += "QUO_DATE <= '" + arg_date_ed + "' "

	conn.execute(strsql)

	try:
		strsql  = "insert into STOCK_QUO_MONTH ("
		strsql += "SEAR_COMP_ID,QUO_DATE,OPEN,HIGH,LOW,CLOSE,VOL,"
		strsql += "DATE_LAST_MAINT,TIME_LAST_MAINT,PROG_LAST_MAINT"
		strsql += ") values ("
		strsql += "'" + arg_sear_comp_id + "', "
		strsql += "'" + mon_close_dt + "', "
		strsql += str(mon_open) + ", "
		strsql += str(mon_high) + ", "
		strsql += str(mon_low) + ", "
		strsql += str(mon_close) + ", "
		strsql += str(mon_vol) + ", "
		strsql += "'" + date_last_maint + "', "
		strsql += "'" + time_last_maint + "', "
		strsql += "'" + prog_last_maint + "' "
		strsql += ")"

		conn.execute(strsql)

	except sqlite3.Error as er:
		err_flag = True
		print("insert into STOCK_QUO_MONTH er=" + er.args[0])
		print(arg_sear_comp_id + " " + mon_close_dt + "資料寫入異常...Rollback!")
		file.write("insert into STOCK_QUO_MONTH er=" + er.args[0] + "\n")
		file.write(arg_sear_comp_id + " " + mon_close_dt + "資料寫入異常...Rollback!\n")
		file.write(strsql + "\n")
		conn.execute("rollback")

	if err_flag == False:
		conn.commit()

def READ_MONTH_QUO(arg_date):
	global err_flag
	global file
	global conn

	arg_date_st = arg_date
	arg_date_ed = arg_date[0:6] + "31"
	#print(arg_date_st + "~" + arg_date_ed)

	strsql  = "select distinct SEAR_COMP_ID from STOCK_QUO "
	strsql += "where "
	strsql += "QUO_DATE >= '" + arg_date_st + "' and "
	strsql += "QUO_DATE <= '" + arg_date_ed + "' "
	strsql += "order by SEAR_COMP_ID "
	strsql += "limit 10 "

	cursor = conn.execute(strsql)
	result = cursor.fetchall()
	re_len = len(result)

	if re_len > 0:
		i = 0
		for row in result:
			i += 1
			#print(row[0])
			READ_DAY_QUO(row[0], arg_date_st, arg_date_ed)

	#關閉cursor
	cursor.close()

def MAIN_QUO_MONTH():
	global err_flag
	global file
	global conn

	err_flag = False

	print("Executing " + os.path.basename(__file__) + "...")

	#建立資料庫連線
	conn = sqlite3.connect("market_price.sqlite")

	#取得當天日期
	dt = datetime.datetime.now()
	dt2 = parser.parse(str(dt)).strftime("%Y%m%d")
	now_date = parser.parse(str(dt)).strftime("%Y/%m/%d")

	#LOG檔
	name = "QUO_MONTH_" + dt2 + ".txt"
	file = open(name, 'a', encoding = 'UTF-8')

	#程式運行過程計時開始
	tStart = time.time()#計時開始
	file.write("\n\n\n*** LOG datetime  " + str(datetime.datetime.now()) + " ***\n")
	file.write("Executing " + os.path.basename(__file__) + "...\n\n")

	#設定結轉起始日期
	strsql = "select MAX(QUO_DATE) from STOCK_QUO_MONTH "

	cursor = conn.execute(strsql)
	result = cursor.fetchone()
	re_len = len(result)

	#print(result)

	if result[0] is not None:
		#start_date = result[0]
		start_date = datetime.datetime.strptime(result[0], "%Y%m%d").date()

		#for test 手動指定起始日期
		#start_date = datetime.datetime.strptime('20170101', "%Y%m%d").date()

		start_date = start_date + relativedelta(days=-31)
		start_date = str(start_date)[0:10]
		start_date = parser.parse(start_date).strftime("%Y/%m/%d")
	else:
		start_date = '2000/01/01'

	#關閉cursor
	cursor.close()

	#計算區間的月份數，作為迴圈終止條件
	a = datetime.datetime.strptime(start_date, "%Y/%m/%d")
	b = datetime.datetime.strptime(now_date, "%Y/%m/%d")
	#print(str(a) + "~" + str(b) + "\n")

	int_diff_month = (b.year * 12 + b.month) - (a.year * 12 + a.month) + 1
	#print("months=" + str(int_diff_month) + "\n")

	i = 1
	dt = ""
	duration_mon = ""
	while i <= int_diff_month:
		#print(str(i) + "\n")
		if i==1:
			str_date = start_date[0:8] + "01"
		else:
			str_date = parser.parse(str(dt)).strftime("%Y/%m/%d")

		duration_mon = str_date.replace("/","")

		#print(duration_mon)
		#計算月線
		READ_MONTH_QUO(duration_mon)

		#日期往後推一個月
		dt = datetime.datetime.strptime(str_date, "%Y/%m/%d").date()
		dt = dt + relativedelta(months=1)
		i += 1

	#關閉資料庫連線
	conn.close()

	tEnd = time.time()#計時結束
	file.write ("\n\n\n結轉耗時 %f sec\n" % (tEnd - tStart)) #會自動做進位
	file.write("*** End LOG ***\n")

	# Close File
	file.close()

	#若執行過程無錯誤，執行結束後刪除log檔案
	if err_flag == False:
		os.remove(name)

	print("上市櫃股價月線計算作業結束...\n\n\n")

if __name__ == '__main__':
	MAIN_QUO_MONTH()