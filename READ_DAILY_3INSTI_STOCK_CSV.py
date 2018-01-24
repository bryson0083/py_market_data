# -*- coding: utf-8 -*-
"""
證交所三大法人個股買賣超日報~每日收盤CSV檔讀取並寫入資料庫

@author: Bryson Xue

@target_rul: 

@Note: 
	證交所三大法人個股買賣超日報
	讀取下載後的CSV檔，寫入資料庫

	1. 20171218起，原始資料"外資買賣超股數"拆分為，
	   外陸資買賣超股數(不含外資自營商)、外資自營商買賣超股數

	   PS:
	   20171215前的資料欄位，維持不變
	
"""
import csv
import pandas as pd
from dateutil import parser
import datetime
from dateutil.relativedelta import relativedelta
import os
import sqlite3
import time

def READ_CSV(arg_date):
	global err_flag
	global file

	print("讀取" + arg_date + " 證交所三大法人個股買賣超日報CSV資料檔.")
	file_name = "./daily_3insti_stock_data/" + arg_date + ".csv"
	
	#判斷CSV檔案是否存在，若無檔案則跳回主程式
	is_existed = os.path.exists(file_name)
	if is_existed == False:
		print(arg_date + "無報價CSV檔.\n")
		file.write(arg_date + "無報價CSV檔.\n")
		return
	
	#讀取每日報價CSV檔
	#with open('1050511.csv', 'r') as f:
	with open(file_name, 'r') as f:
		reader = csv.reader(f)
		quo_list = list(reader)
	
	#關閉CSV檔案
	f.close
	#print(quo_list)
	
	#檢查檔案當天是否有交易資料，若無交易資料則跳回主程式
	#(若檔案內容為"當天無交易資料"，表示當天未開盤)
	for item in quo_list:
		for j in item:
			if j == "當天無交易資料":
				print(arg_date + "當天未開盤，無交易資料.\n")
				return
	
	#取得要讀取的資料起點位置(識別"證券代號")
	st_idx = 0
	i = 0
	for item in quo_list:
		#print("i=" + str(i) + "\n")
		for j in item:
			if j == "證券代號":
				st_idx = i
				#print("start from " + str(i) + "\n")
		i += 1

	try:
		# 讀取當天個股三大法人買賣超資料到list中
		i = 0
		idx = st_idx
		all_data = []
		while True:
			#for item in quo_list[idx]:
			#	print(item)

			data = [str(item) for item in quo_list[idx]]
			data = list(filter(None, data))	#過濾empty string，寫法等同於[str(item) for item in quo_list[idx] if item]，但filter運算較快

			# 判斷若list data長度不滿16，跳出迴圈
			# (20171218起，格式與之前不同，中止條件不同)
			col_ub = 16		#適用於20171215前資料
			if arg_date >= '20171218':
				col_ub = 19

			if len(data) != col_ub:
				break

			all_data.append(data)
			idx += 1

	except Exception as e:
		err_flag = True
		print("$$$ Err:" + arg_date + " 三大法人個股買賣超CSV資料讀取異常. $$$")
		print(e)
		file.write("$$$ Err:" + arg_date + " 三大法人個股買賣超CSV資料讀取異常. $$$")
		file.write(e)
		file.write("\n\n")
	
	#all_data list拋到pandas
	df = pd.DataFrame(all_data[1:], columns = all_data[0])

	#適用於20171215前資料
	ls_title = ['證券代號', '證券名稱', '外資買賣超股數', '投信買賣超股數', '自營商買賣超股數']
	if arg_date >= '20171218':
		ls_title = ['證券代號', '證券名稱', '外陸資買賣超股數(不含外資自營商)', '外資自營商買賣超股數', '投信買賣超股數', '自營商買賣超股數']

	df2 = df.loc[:,ls_title]
	#print(df2.head())

	#寫入、更新資料庫
	STORE_DB(df2, arg_date)

def STORE_DB(arg_df, arg_date):
	global err_flag
	global file
	global conn

	#print(arg_df)
	quo_date = arg_date
	
	#建立資料庫連線
	conn = sqlite3.connect("market_price.sqlite")
	
	commit_flag = True
	for i in range(0,len(arg_df)):
		#print(str(df.index[i]))
		comp_id = str(arg_df.loc[i]['證券代號'])
		comp_id = comp_id.replace('"','').replace("=","").strip() + ".TW"
		
		comp_name = str(arg_df.loc[i]['證券名稱']).strip()

		if arg_date >= '20171218':
			fr1 = arg_df.loc[i]['外陸資買賣超股數(不含外資自營商)'].replace(",","").strip()
			fr2 = arg_df.loc[i]['外資自營商買賣超股數'].replace(",","").strip()
			fr_net_bas = int(fr1) + int(fr2)
		else:
			fr_net_bas = arg_df.loc[i]['外資買賣超股數'].replace(",","").strip()

		it_net_bas = arg_df.loc[i]['投信買賣超股數'].replace(",","").strip()
		dl_net_bas = arg_df.loc[i]['自營商買賣超股數'].replace(",","").strip()
		
		# 最後維護日期時間
		str_date = str(datetime.datetime.now())
		date_last_maint = parser.parse(str_date).strftime("%Y%m%d")
		time_last_maint = parser.parse(str_date).strftime("%H%M%S")
		prog_last_maint = "READ_DAILY_3INSTI_STOCK_CSV"

		#print(comp_id + "#" + comp_name + "#" + str(fr_net_bas) + "#" + str(it_net_bas) + "#" + str(dl_net_bas) + "#\n")

		#檢查資料是否已存在
		strsql  = "select count(*) from STOCK_CHIP_ANA "
		strsql += "where QUO_DATE = '" + quo_date + "' and "
		strsql += "SEAR_COMP_ID='" + comp_id + "' "
		
		#print(strsql + "\n")
		cursor = conn.execute(strsql)
		result = cursor.fetchone()
		
		if result[0] == 0:
			#print(comp_id + "沒資料\n")
			# 日報價資料寫入
			strsql  = "insert into STOCK_CHIP_ANA ("
			strsql += "QUO_DATE,SEAR_COMP_ID,COMP_NAME,FOREIGN_INV_NET_BAS,"
			strsql += "INV_TRUST_NET_BAS,DEALER_NET_BAS,"
			strsql += "DATE_LAST_MAINT,TIME_LAST_MAINT,PROG_LAST_MAINT"
			strsql += ") values ("
			strsql += "'" + quo_date + "',"
			strsql += "'" + comp_id + "',"
			strsql += "'" + comp_name + "',"
			strsql += str(fr_net_bas) + ","
			strsql += str(it_net_bas) + ","
			strsql += str(dl_net_bas) + ","
			strsql += "'" + date_last_maint + "',"
			strsql += "'" + time_last_maint + "',"
			strsql += "'" + prog_last_maint + "' "
			strsql += ")"

		else:
			#print(comp_id + "有資料\n")
			#針對現有資料更新
			strsql  = "update STOCK_CHIP_ANA set "
			strsql += "FOREIGN_INV_NET_BAS=" + str(fr_net_bas) + ","
			strsql += "INV_TRUST_NET_BAS=" + str(it_net_bas) + ","
			strsql += "DEALER_NET_BAS=" + str(dl_net_bas) + ","
			strsql += "DATE_LAST_MAINT='" + date_last_maint + "',"
			strsql += "TIME_LAST_MAINT='" + time_last_maint + "',"
			strsql += "PROG_LAST_MAINT='" + prog_last_maint + "' "
			strsql += "where "
			strsql += "QUO_DATE = '" + quo_date + "' and "
			strsql += "SEAR_COMP_ID='" + comp_id + "' "

		try:
			#print(strsql)
			conn.execute(strsql)
		except sqlite3.Error as er:
			commit_flag = False
			err_flag = True
			print("insert/update STOCK_CHIP_ANA er=" + er.args[0])
			print(comp_id + " " + comp_name + " " + quo_date + "資料異常.")
			print(strsql)
			file.write("insert/update STOCK_CHIP_ANA er=" + er.args[0] + "\n")
			file.write(comp_id + " " + comp_name + " " + quo_date + "資料異常.\n")
			file.write(strsql + "\n")

		#關閉cursor
		cursor.close()

	# 最後commit
	if commit_flag == True:
		conn.commit()
		print(quo_date + " 證交所三大法人個股買賣超日報，寫入/更新成功.\n")
		file.write(quo_date + " 證交所三大法人個股買賣超日報，寫入/更新成功.\n")
	else:
		conn.execute("rollback")
		print(quo_date + " 證交所三大法人個股買賣超日報，寫入/更新失敗Rollback.\n")
		file.write(quo_date + " 證交所三大法人個股買賣超日報，寫入/更新失敗Rollback.\n")
	
	#關閉資料庫連線
	conn.close

def MAIN_READ_DAILY_3INSTI_STOCK_CSV():
	global err_flag
	global file
	global conn
	err_flag = False

	print("Executing " + os.path.basename(__file__) + "...")

	#起訖日期(預設跑當天日期到往前推7天)
	dt = datetime.datetime.now()
	start_date = dt + datetime.timedelta(days=-7)
	start_date = parser.parse(str(start_date)).strftime("%Y%m%d")
	end_date = parser.parse(str(dt)).strftime("%Y%m%d")

	#for需要時手動設定日期區間用
	start_date = "20180101"
	end_date = "20180118"

	# 寫入LOG File
	str_date = str(datetime.datetime.now())
	str_date = parser.parse(str_date).strftime("%Y%m%d")
	name = "READ_DAILY_3INSTI_STOCK_CSV_LOG_" + str_date + ".txt"
	file = open(name, 'a', encoding = 'UTF-8')

	print_dt = str(str_date) + (' ' * 22)
	print("##############################################")
	print("##       證交所三大法人個股買賣超日報       ##")
	print("##              CSV檔寫入資料庫             ##")
	print("##                                          ##")
	print("##  datetime: " + print_dt +               "##")
	print("##############################################")
	print("\n\n")
	print("結轉日期" + start_date + "~" + end_date + "\n")

	tStart = time.time()#計時開始
	file.write("\n\n\n*** LOG datetime  " + str(datetime.datetime.now()) + " ***\n")
	file.write("Executing " + os.path.basename(__file__) + "...\n")
	file.write("結轉日期區間:" + start_date + "~" + end_date + "\n")

	#計算區間的天數，作為迴圈終止條件
	date_fmt = "%Y%m%d"
	a = datetime.datetime.strptime(start_date, date_fmt)
	b = datetime.datetime.strptime(end_date, date_fmt)
	delta = b - a
	int_diff_date = delta.days + 1
	#print("days=" + str(int_diff_date) + "\n")

	i = 1
	dt = ""
	while i <= int_diff_date:
		#print(str(i) + "\n")
		if i==1:
			str_date = start_date
		else:
			str_date = parser.parse(str(dt)).strftime("%Y%m%d")
			
		#print(str_date + "\n")
		#讀取日期當天報價CSV檔
		READ_CSV(str_date)
		
		#日期往後推一天
		dt = datetime.datetime.strptime(str_date, date_fmt).date()
		dt = dt + relativedelta(days=1)
		i += 1

	tEnd = time.time()#計時結束
	file.write ("\n\n\n結轉耗時 %f sec\n" % (tEnd - tStart)) #會自動做進位
	file.write("*** End LOG ***\n")

	# Close File
	file.close()

	#若執行過程無錯誤，執行結束後刪除log檔案
	if err_flag == False:
		os.remove(name)

	print("\n\n證交所三大法人個股買賣超日報，每日收盤CSV檔資料庫處理結束...\n\n\n")

if __name__ == '__main__':
	MAIN_READ_DAILY_3INSTI_STOCK_CSV()