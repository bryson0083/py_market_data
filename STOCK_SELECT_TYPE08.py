# -*- coding: utf-8 -*-
"""
技術分析選股 TYPE 8

@author: Bryson Xue

@target_rul: 

@Note: 
	上市櫃股票，籌碼面選股
	A類: 三大法人均買超
	B類: 外資、投信買超
	
"""
import sqlite3
import pandas as pd
import xlsxwriter
import datetime
from dateutil import parser
import os.path
import time

def MAIN_STOCK_SELECT_TYPE08():
	global err_flag
	err_flag = False

	print("Executing " + os.path.basename(__file__) + "...")

	# 寫入LOG File
	dt=datetime.datetime.now()
	str_date = parser.parse(str(dt)).strftime("%Y%m%d")

	print_dt = str(str_date) + (' ' * 22)
	print("##############################################")
	print("##            技術分析選股 TYPE 08          ##")
	print("##                                          ##")
	print("##                                          ##")
	print("##  datetime: " + print_dt +               "##")
	print("##############################################")
	print("\n\n")
	print("分析日期: " + str_date + "\n")

	name = "STOCK_SELECT_TYPE08_" + str_date + ".txt"
	file = open(name, 'a', encoding = 'UTF-8')
	tStart = time.time()#計時開始
	file.write("\n\n\n*** LOG datetime  " + str(datetime.datetime.now()) + " ***\n")
	file.write("Executing " + os.path.basename(__file__) + "...\n")

	#建立資料庫連線
	conn = sqlite3.connect("market_price.sqlite")

	strsql  = "select SEAR_COMP_ID, STOCK_NAME, FR_BAS_CNT, IT_BAS_CNT, DE_BAS_CNT, "
	strsql += "FR_BAS_VOL, IT_BAS_VOL, DE_BAS_VOL, RANK, REMARK "
	strsql += "from REPORT_CHIP_ANA "
	strsql += "where RANK in ('A', 'B') "
	strsql += "order by RANK, FR_BAS_CNT, IT_BAS_CNT, DE_BAS_CNT "

	try:
		#df_result = pd.read_sql(strsql, conn, index_col='SEAR_COMP_ID', columns=['股票代號', '名稱', '外資買賣超天數', '投信買賣超天數', '自營商買賣超天數', '外資買賣超張數', '投信買賣超張數', '自營商買賣超張數', '類別', '備註'])
		df_result = pd.read_sql(strsql, conn, columns=['股票代號', '名稱', '外資買賣超天數', '投信買賣超天數', '自營商買賣超天數', '外資買賣超張數', '投信買賣超張數', '自營商買賣超張數', '類別', '備註'])
		#print(df_result)
	except Exception as e:
		err_flag = True
		print("$$$ Err:" + str_date + " 選股報表產生錯誤. $$$")
		print(str(e) + "\n\n")
		file.write("$$$ Err:" + str_date + " 選股報表產生錯誤. $$$\n")
		file.write(str(e) + "\n\n")

	#關閉資料庫連線
	conn.close()

	#結果寫入EXCEL檔
	file_name = 'STOCK_SELECT_TYPE08_' + str_date + '.xlsx'
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

	print("\n\n技術分析選股TYPE 08執行結束...\n\n\n")

if __name__ == '__main__':
	MAIN_STOCK_SELECT_TYPE08()