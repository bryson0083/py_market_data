# -*- coding: utf-8 -*-
"""
上市櫃股價周線計算

@author: Bryson Xue

@target_rul: 

@Note: 
    基於上市櫃股票之日線報價數據，計算周線數據

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

	#讀取周線開盤價
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
		week_open = result[0]
	else:
		week_open = 0

	#關閉cursor
	cursor.close()

	#讀取周線收盤價，與當周最後收盤日期
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
		week_close = result[0]
		week_close_dt = result[1]
	else:
		week_close = 0
		week_close_dt = ""

	#關閉cursor
	cursor.close()

	#讀取周線最低價、最高價、成交量
	strsql  = "select min(LOW), max(HIGH), sum(VOL) from STOCK_QUO "
	strsql += "where "
	strsql += "SEAR_COMP_ID = '" + arg_sear_comp_id + "' and "
	strsql += "QUO_DATE >= '" + arg_date_st + "' and "
	strsql += "QUO_DATE <= '" + arg_date_ed + "' and "
	strsql += "CLOSE > 0 "	

	cursor = conn.execute(strsql)
	result = cursor.fetchone()

	if result[0] is not None:
		week_low = result[0]
		week_high = result[1]
		week_vol = result[2]
	else:
		week_low = 0
		week_high = 0
		week_vol = 0

	#關閉cursor
	cursor.close()

	#print(arg_sear_comp_id + ",open=" + str(week_open) + ", close=" + str(week_close) + ",high=" + str(week_high) + ",low=" + str(week_low) + ",vol=" + str(week_vol) + ",lat_dt=" + week_close_dt)

	# 最後維護日期時間
	str_date = str(datetime.datetime.now())
	date_last_maint = parser.parse(str_date).strftime("%Y%m%d")
	time_last_maint = parser.parse(str_date).strftime("%H%M%S")
	prog_last_maint = "WEEK_QUO"

	#資料存檔
	strsql  = "delete from STOCK_QUO_WEEKLY "
	strsql += "where "
	strsql += "SEAR_COMP_ID = '" + arg_sear_comp_id + "' and "
	strsql += "QUO_DATE = '" + week_close_dt + "' "

	conn.execute(strsql)

	try:
		strsql  = "insert into STOCK_QUO_WEEKLY ("
		strsql += "SEAR_COMP_ID,QUO_DATE,OPEN,HIGH,LOW,CLOSE,VOL,"
		strsql += "DATE_LAST_MAINT,TIME_LAST_MAINT,PROG_LAST_MAINT"
		strsql += ") values ("
		strsql += "'" + arg_sear_comp_id + "', "
		strsql += "'" + week_close_dt + "', "
		strsql += str(week_open) + ", "
		strsql += str(week_high) + ", "
		strsql += str(week_low) + ", "
		strsql += str(week_close) + ", "
		strsql += str(week_vol) + ", "
		strsql += "'" + date_last_maint + "', "
		strsql += "'" + time_last_maint + "', "
		strsql += "'" + prog_last_maint + "' "
		strsql += ")"

		conn.execute(strsql)

	except sqlite3.Error as er:
		err_flag = True
		print("insert into STOCK_QUO_WEEKLY er=" + er.args[0])
		print(arg_sear_comp_id + " " + week_close_dt + "資料寫入異常...Rollback!")
		file.write("insert into STOCK_QUO_WEEKLY er=" + er.args[0] + "\n")
		file.write(arg_sear_comp_id + " " + week_close_dt + "資料寫入異常...Rollback!\n")
		file.write(strsql + "\n")
		conn.execute("rollback")

	if err_flag == False:
		conn.commit()

def READ_WEEK_QUO(arg_date_st, arg_date_ed):
	global err_flag
	global file
	global conn

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

def MAIN_QUO_WEEK():
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
	#now_date = "2017/02/08"
	#print(now_date)

	#LOG檔
	name = "QUO_WEEK_" + dt2 + ".txt"
	file = open(name, 'a', encoding = 'UTF-8')

	#程式運行過程計時開始
	tStart = time.time()#計時開始
	file.write("\n\n\n*** LOG datetime  " + str(datetime.datetime.now()) + " ***\n")
	file.write("Executing " + os.path.basename(__file__) + "...\n\n")

	#設定結轉起始日期
	strsql = "select MAX(QUO_DATE) from STOCK_QUO_WEEKLY "

	cursor = conn.execute(strsql)
	result = cursor.fetchone()
	re_len = len(result)

	#print(result)

	if result[0] is not None:
		#start_date = result[0]
		start_date = datetime.datetime.strptime(result[0], "%Y%m%d").date()

		#for test 手動指定起始日期
		#start_date = datetime.datetime.strptime('20080101', "%Y%m%d").date()

		start_date = start_date + relativedelta(days=-7)
		start_date = str(start_date)[0:10]
		start_date = parser.parse(start_date).strftime("%Y/%m/%d")
	else:
		start_date = '2000/01/01'

	#關閉cursor
	cursor.close()

	#計算區間的天數，作為迴圈終止條件
	a = datetime.datetime.strptime(start_date, "%Y/%m/%d")
	b = datetime.datetime.strptime(now_date, "%Y/%m/%d")
	delta = b - a
	int_diff_date = delta.days + 1
	#print("days=" + str(int_diff_date) + "\n")

	i = 1
	dt = ""
	duration_st = ""
	duration_ed = ""
	while i <= int_diff_date:
		#print(str(i) + "\n")
		if i==1:
			str_date = start_date
		else:
			str_date = parser.parse(str(dt)).strftime("%Y/%m/%d")
		
		#判斷該日期是星期幾(1~7表示周一到周日)
		dt_wday = datetime.datetime.strptime(str_date, '%Y/%m/%d').date().isoweekday()
		#print(str_date + "   " + str(dt_wday) + "\n")

		cal_yn = "N"
		if dt_wday == 1:
			duration_st = parser.parse(str(str_date)).strftime("%Y%m%d")
		elif duration_st > "" and dt_wday == 7:
			duration_ed = parser.parse(str(str_date)).strftime("%Y%m%d")
			cal_yn = "Y"
		else:
			duration_ed = parser.parse(str(str_date)).strftime("%Y%m%d")

		if i == int_diff_date:
			cal_yn = "Y"

		#若cal_yn為Y，結算一次周K
		if cal_yn == "Y":
			#print("區間開始於" + duration_st + "~" + duration_ed + "\n")
			READ_WEEK_QUO(duration_st, duration_ed)

		#日期往後推一天
		dt = datetime.datetime.strptime(str_date, "%Y/%m/%d").date()
		dt = dt + relativedelta(days=1)
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

	print("上市櫃股價周線計算作業結束...\n\n\n")

if __name__ == '__main__':
	MAIN_QUO_WEEK()