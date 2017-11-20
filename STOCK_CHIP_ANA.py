# -*- coding: utf-8 -*-
"""
三大法人買賣超分析

@author: Bryson Xue

@target_rul: 

@Note: 
	每日計算上市櫃股票，三大法人買賣超狀況
	並計算連續買超、賣超天數，並予以類別分類

	買賣超代碼
	A類: 三大法人均買超
	B類: 外資、投信買超
	C類: 三大法人均賣超
	D類: 外資、投信賣超
	E類: 三大法人行為出現轉折
	Z類: 無明顯趨向(盤整)

"""
import sqlite3
import pandas as pd
import xlsxwriter
import datetime
from datetime import date, timedelta
from dateutil import parser
from dateutil.relativedelta import relativedelta
import sys, os
import time

#個股籌碼分析
def CHIP_ANA(arg_stock, str_prev_date, str_today):
	global err_flag
	global file
	global conn

	sear_comp_id = arg_stock[0]

	#個股籌碼資料讀取
	strsql  = "select QUO_DATE, COMP_NAME, FOREIGN_INV_NET_BAS, INV_TRUST_NET_BAS, "
	strsql += "DEALER_NET_BAS from STOCK_CHIP_ANA "
	strsql += "where "
	strsql += "SEAR_COMP_ID='" + sear_comp_id + "' and "
	strsql += "QUO_DATE between '" + str_prev_date + "' and '" + str_today + "' "
	strsql += "order by QUO_DATE "

	cursor = conn.execute(strsql)
	result = cursor.fetchall()

	none_flag = False
	if len(result) > 0:
		df = pd.DataFrame(result, columns = ['quo_date','comp_name','fr_bas','it_bas','de_bas'])
		#print(df)
	else:
		none_flag = True

	#關閉cursor
	cursor.close()

	ls_result = []
	if none_flag == False:

		fr_rev_flag = False
		it_rev_flag = False
		de_rev_flag = False
		acc_fr_bas = 0
		acc_it_bas = 0
		acc_de_bas = 0

		for i in range(0,len(df)):
			quo_date = str(df.loc[i]['quo_date'])
			comp_name = df.loc[i]['comp_name']
			fr_bas = df.loc[i]['fr_bas']
			it_bas = df.loc[i]['it_bas']
			de_bas = df.loc[i]['de_bas']

			if fr_bas > 0:		#外資買超
				fr_bas_cnt = 1
			elif fr_bas < 0:	#外資賣超
				fr_bas_cnt = -1
			else:
				fr_bas_cnt = 0

			if it_bas > 0:		#投信買超
				it_bas_cnt = 1
			elif it_bas < 0:	#投信賣超
				it_bas_cnt = -1
			else:
				it_bas_cnt = 0

			if de_bas > 0:		#自營商買超
				de_bas_cnt = 1
			elif de_bas < 0:	#自營商賣超
				de_bas_cnt = -1
			else:
				de_bas_cnt = 0

			#外資買賣超判斷
			if ((acc_fr_bas > 0) and (fr_bas_cnt < 0)) or \
			   ((acc_fr_bas < 0) and (fr_bas_cnt > 0)):	
				fr_rev_flag = True
				acc_fr_bas = 0
			else:
				fr_rev_flag = False

			acc_fr_bas += fr_bas_cnt	#累計買賣超天數

			#投信買賣超判斷
			if ((acc_it_bas > 0) and (it_bas_cnt < 0)) or \
			   ((acc_it_bas < 0) and (it_bas_cnt > 0)):	
				it_rev_flag = True
				acc_it_bas = 0
			else:
				it_rev_flag = False

			acc_it_bas += it_bas_cnt	#累計買賣超天數

			#自營商買賣超判斷
			if ((acc_de_bas > 0) and (de_bas_cnt < 0)) or \
			   ((acc_de_bas < 0) and (de_bas_cnt > 0)):	
				de_rev_flag = True
				acc_de_bas = 0
			else:
				de_rev_flag = False

			acc_de_bas += de_bas_cnt	#累計買賣超天數
			#print(quo_date + "," + str(acc_de_bas) + "," + str(de_bas_cnt) + "\n")

		#最近一個交易日三大法人買賣超張數
		last_day_fr_bas = int(df['fr_bas'].tail(1) / 1000)
		last_day_it_bas = int(df['it_bas'].tail(1) / 1000)
		last_day_de_bas = int(df['de_bas'].tail(1) / 1000)

		#依據法人買賣行為分類
		rank = ""
		if (acc_fr_bas > 0) and (acc_it_bas > 0) and (acc_de_bas > 0):
			rank = "A"	#三大法人均買超
		elif (acc_fr_bas > 0) and (acc_it_bas > 0) and (acc_de_bas <= 0):
			rank = "B"	#外資、投信買超
		elif (acc_fr_bas < 0) and (acc_it_bas < 0) and (acc_de_bas < 0):
			rank = "C"	#三大法人均賣超
		elif (acc_fr_bas < 0) and (acc_it_bas < 0) and (acc_de_bas >= 0):
			rank = "D"	#外資、投信賣超
		elif (fr_rev_flag == True) or (it_rev_flag == True) or (de_rev_flag == True):
			rank = "E"	#三大法人行為出現轉折
		else:
			rank = "Z"	#無明顯趨向(盤整)

		remark = ""
		if (acc_fr_bas > 0) and (fr_rev_flag == True):
			remark += "外資轉賣為買、"
		elif (acc_fr_bas < 0) and (fr_rev_flag == True):
			remark += "外資轉買為賣、"
		elif (acc_it_bas > 0) and (it_rev_flag == True):
			remark += "投信轉賣為買、"
		elif (acc_it_bas < 0) and (it_rev_flag == True):
			remark += "投信轉買為賣、"
		elif (acc_de_bas > 0) and (de_rev_flag == True):
			remark += "自營商轉賣為買、"
		elif (acc_de_bas < 0) and (de_rev_flag == True):
			remark += "自營商轉買為賣、"

		remark = remark[0:len(remark)-1]

		ls_result = [[sear_comp_id,comp_name,acc_fr_bas,acc_it_bas,acc_de_bas,last_day_fr_bas,last_day_it_bas,last_day_de_bas,rank,remark]]
		df_result = pd.DataFrame(ls_result, columns=['股票代號', '名稱', '外資買賣超天數', '投信買賣超天數', '自營商買賣超天數', '外資買賣超張數', '投信買賣超張數', '自營商買賣超張數', '類別', '備註'])

	if len(ls_result) == 0:
		df_result = pd.DataFrame()

	#print(df_result)
	return df_result

def MAIN_STOCK_CHIP_ANA():
	global err_flag
	global file
	global conn
	err_flag = False

	print("Executing " + os.path.basename(__file__) + "...")

	#產生日期區間(當天日期，往前推30天)
	today = datetime.datetime.now()
	prev_date = today + timedelta(days=-30)

	str_today = today.strftime("%Y%m%d")
	str_prev_date = prev_date.strftime("%Y%m%d")

	print_dt = str(str_today) + (' ' * 22)
	print("##############################################")
	print("##            三大法人買賣超分析            ##")
	print("##                                          ##")
	print("##                                          ##")
	print("##  datetime: " + print_dt +               "##")
	print("##############################################")
	print("\n\n")
	print("資料涵蓋範圍: " + str_prev_date + "~" + str_today + "\n")

	# 寫入LOG File
	dt=datetime.datetime.now()
	str_date = parser.parse(str(dt)).strftime("%Y%m%d")

	name = "STOCK_CHIP_ANA_" + str_date + ".txt"
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
			df = CHIP_ANA(stock, str_prev_date, str_today)

			if len(df)>0:
				df_result = pd.concat([df_result, df], ignore_index=True)

	#關閉cursor
	cursor.close()

	#print(df_result)

	#資料進行排序
	if len(df_result)>0:
		df_result = df_result.sort_values(by=['類別', '股票代號','外資買賣超天數','投信買賣超天數'], ascending=[True, True, True, True])

	#結果寫入EXCEL檔
	file_name = 'STOCK_CHIP_ANA_' + str_date + '.xlsx'
	writer = pd.ExcelWriter(file_name, engine='xlsxwriter')
	df_result.to_excel(writer, sheet_name='stock', index=False)
	writer.save()

	#更換欄位名稱，將結果寫入資料庫
	df_result.columns = ['SEAR_COMP_ID', 'STOCK_NAME', 'FR_BAS_CNT', 'IT_BAS_CNT', 'DE_BAS_CNT', 'FR_BAS_VOL', 'IT_BAS_VOL', 'DE_BAS_VOL', 'RANK', 'REMARK']
	df_result.to_sql(name='REPORT_CHIP_ANA', con=conn, index=False, if_exists='replace')

	#關閉資料庫連線
	conn.close()

	tEnd = time.time()#計時結束
	file.write ("\n\n\n結轉耗時 %f sec\n" % (tEnd - tStart)) #會自動做進位
	file.write("*** End LOG ***\n")

	# Close File
	file.close()

	#若執行過程無錯誤，執行結束後刪除log檔案
	print(err_flag)
	if err_flag == False:
		print("sssssssssssssssssssssssssssss")
		os.remove(name)

	print("\n\n三大法人買賣超分析，執行結束...\n\n\n")

if __name__ == '__main__':
	MAIN_STOCK_CHIP_ANA()