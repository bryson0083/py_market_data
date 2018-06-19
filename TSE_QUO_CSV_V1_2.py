# -*- coding: utf-8 -*-
"""
證交所上市收盤CSV檔案資料，轉入DB處理

@author: Bryson Xue

@target_rul: 

@Note: 
    CSV檔案資料，寫入資料庫

"""
import csv
import pandas as pd
from dateutil import parser
import datetime
from dateutil.relativedelta import relativedelta
import os
import sqlite3
import time

def TSE_QUO_READ_CSV(arg_date):
	global err_flag
	global file

	# 轉民國年日期
	str_date = str(int(arg_date[0:4]) - 1911) + arg_date[4:]
	str_date = str_date.replace("/","")
	print("讀取" + str_date + "收盤資料CSV檔案.")
	
	file_name = "./tse_quo_data/" + str_date + ".csv"
	
	#判斷CSV檔案是否存在，若無檔案則跳回主程式
	is_existed = os.path.exists(file_name)
	if is_existed == False:
		err_flag = True
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
	#(若檔案內容為"當天無收盤資料"，表示當天未開盤)
	for item in quo_list:
		for j in item:
			if j == "當天無收盤資料":
				print(arg_date + "當天未開盤，無交易資料.\n\n")
				file.write(arg_date + "當天未開盤，無交易資料.\n\n")
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

	# 讀取當天個股收盤資料到list中
	idx = st_idx
	all_data = []
	while idx < len(quo_list):
		data = [str(item) for item in quo_list[idx]]
		data = list(filter(None, data))	#過濾empty string，寫法等同於[str(item) for item in quo_list[idx] if item]，但filter運算較快
		#print(data)

		all_data.append(data)
		idx += 1
	
	#all_data list拋到pandas
	df = pd.DataFrame(all_data[1:], columns = all_data[0])
	df2 = df.loc[:,['證券代號', '證券名稱', '開盤價', '最高價', '最低價', '收盤價', '成交股數', '本益比']]
	#print(df2)

	#寫入、更新資料庫
	TSE_QUO_DB(df2, arg_date)
	
def TSE_QUO_DB(arg_df, arg_date):
	global err_flag
	global file

	#print(arg_df)
	quo_date = arg_date.replace("/", "")
	
	#建立資料庫連線
	conn = sqlite3.connect("market_price.sqlite")
	
	commit_flag = "Y"
	cnt = 0
	for i in range(0,len(arg_df)):
		#print(str(df.index[i]))
		comp_id = str(arg_df.iloc[i][0])
		comp_id = comp_id.replace('"','').replace("=","").strip() + ".TW"
		
		comp_name = str(arg_df.iloc[i][1])
		q_open = arg_df.iloc[i][2].replace('"','').replace(",","").replace("=","").strip()	# 開盤價
		q_high = arg_df.iloc[i][3].replace('"','').replace(",","").replace("=","").strip()	# 最高價
		q_low = arg_df.iloc[i][4].replace('"','').replace(",","").replace("=","").strip()	# 最低價
		q_close = arg_df.iloc[i][5].replace('"','').replace(",","").replace("=","").strip()	# 收盤價
		q_vol = arg_df.iloc[i][6].replace('"','').replace(",","").replace("=","").strip()	# 成交量
		q_per = str(arg_df.iloc[i][7]).replace('"','').replace(",","").replace("=","").strip()	# 本益比
		
		# 最後維護日期時間
		str_date = str(datetime.datetime.now())
		date_last_maint = parser.parse(str_date).strftime("%Y%m%d")
		time_last_maint = parser.parse(str_date).strftime("%H%M%S")
		prog_last_maint = "TSE_QUO_CSV"
		
		#處理若資料為"--"時，給予預設值0
		if q_open == "--":
			q_open = 0
		if q_high == "--":
			q_high = 0
		if q_low == "--":
			q_low = 0
		if q_close == "--":
			q_close = 0

		#print(comp_id + "#" + comp_name + "#" + str(q_open) + "#" + str(q_high) + "#" + str(q_low) + "#" + str(q_close) + "#" + str(q_vol) + "#" + str(q_per) + "#\n")

		#原始CSV中，有部分股票，當天有交易，但是沒有任何成交量
		#因此這些股票的當天交易資料，不寫入資料庫
		if float(q_close) > 0 and int(q_vol) > 0:
			insert_yn = "Y"
		else:
			insert_yn = "N"

		#檢查資料是否已存在
		strsql  = "select count(*) from STOCK_QUO "
		strsql += "where QUO_DATE = '" + quo_date + "' and "
		strsql += "SEAR_COMP_ID='" + comp_id + "' "
		
		#print(strsql + "\n")
		cursor = conn.execute(strsql)
		result = cursor.fetchone()
		
		if result[0] == 0:
			#print(comp_id + "沒資料\n")
			# 日報價資料寫入
			if insert_yn == "Y":
				strsql  = "insert into STOCK_QUO ("
				strsql += "SEAR_COMP_ID,QUO_DATE,OPEN,HIGH,LOW,"
				strsql += "CLOSE,VOL,ADJ_CLOSE,PER,"
				strsql += "DATE_LAST_MAINT,TIME_LAST_MAINT,PROG_LAST_MAINT"
				strsql += ") values ("
				strsql += "'" + comp_id + "',"
				strsql += "'" + quo_date + "',"
				strsql += str(q_open) + ","
				strsql += str(q_high) + ","
				strsql += str(q_low) + ","
				strsql += str(q_close) + ","
				strsql += str(q_vol) + ","
				strsql += "0,"
				strsql += str(q_per) + ","
				strsql += "'" + date_last_maint + "',"
				strsql += "'" + time_last_maint + "',"
				strsql += "'" + prog_last_maint + "' "
				strsql += ")"
				
				try:
					cnt += 1
					conn.execute(strsql)
				except sqlite3.Error as er:
					err_flag = True
					commit_flag = "N"
					print("insert into STOCK_QUO er=" + er.args[0])
					print(comp_id + " " + comp_name + " " + quo_date + "資料寫入異常...Rollback!\n")
					file.write("insert into STOCK_QUO er=" + er.args[0] + "\n")
					file.write(comp_id + " " + comp_name + " " + quo_date + "資料寫入異常...Rollback!\n")
					file.write(strsql + "\n")
					conn.execute("rollback")
			
		"""
		else:
			#print(comp_id + "有資料\n")
			#針對現有資料更新
			strsql  = "update STOCK_QUO set "
			strsql += "OPEN=" + str(q_open) + ","
			strsql += "HIGH=" + str(q_high) + ","
			strsql += "LOW=" + str(q_low) + ","
			strsql += "CLOSE=" + str(q_close) + ","
			strsql += "VOL=" + str(q_vol) + ","
			strsql += "PER=" + str(q_per) + ","
			strsql += "DATE_LAST_MAINT='" + date_last_maint + "',"
			strsql += "TIME_LAST_MAINT='" + time_last_maint + "',"
			strsql += "PROG_LAST_MAINT='" + prog_last_maint + "' "
			strsql += "where "
			strsql += "QUO_DATE = '" + quo_date + "' and "
			strsql += "SEAR_COMP_ID='" + comp_id + "' "
			
			try:
				conn.execute(strsql)
			except sqlite3.Error as er:
				err_flag = True
				commit_flag = "N"
				print("update STOCK_QUO er=" + er.args[0] + "\n")
				print(comp_id + " " + comp_name + " " + quo_date + "資料更新異常...Rollback!\n")
				print(strsql + "\n")
				file.write("update STOCK_QUO er=" + er.args[0] + "\n")
				file.write(comp_id + " " + comp_name + " " + quo_date + "資料更新異常...Rollback!\n")
				file.write(strsql + "\n")
				conn.execute("rollback")
		"""

		#關閉cursor
		cursor.close()
		
	# 最後commit
	if commit_flag == "Y":
		conn.commit()
		print(quo_date + "上市收盤報價資料，資料庫轉入成功.")
		print("本次新增資料筆數，共" + str(cnt) + "筆.")
		print("PS:已存在資料不再更新.\n\n")
		file.write(quo_date + "上市收盤報價資料，資料庫轉入成功.\n")
		file.write("本次新增資料筆數，共" + str(cnt) + "筆.\n")
		file.write("PS:已存在資料不再更新.\n\n")

	#關閉資料庫連線
	conn.close

def MAIN_TSE_QUO_READ_CSV():
	global err_flag
	global file
	err_flag = False

	print("Executing " + os.path.basename(__file__) + "...")

	#起訖日期(預設跑當天日期到往前推7天)
	str_date = str(datetime.datetime.now())
	str_date = parser.parse(str_date).strftime("%Y/%m/%d")
	end_date = str_date

	date_1 = datetime.datetime.strptime(end_date, "%Y/%m/%d")
	start_date = date_1 + datetime.timedelta(days=-7)
	start_date = str(start_date)[0:10]
	start_date = parser.parse(start_date).strftime("%Y/%m/%d")

	#for需要時手動設定日期區間用
	#start_date = "2018/06/19"
	#end_date = "2018/06/19"

	print("##############################################")
	print("##      台灣證券交易所~上市收盤資料CSV      ##")
	print("##          檔案資料寫入資料庫作業          ##")
	print("##                                          ##")
	print("##   結轉日期: " + start_date + "~" + end_date + "        ##")
	print("##############################################")
	print("\n\n")

	# 寫入LOG File
	dt=datetime.datetime.now()
	str_date = str(dt)
	str_date = parser.parse(str_date).strftime("%Y%m%d")

	name = "TSE_QUO_CSV_LOG_" + str_date + ".txt"
	file = open(name, 'a', encoding = 'UTF-8')

	tStart = time.time()#計時開始
	file.write("\n\n\n*** LOG datetime  " + str(datetime.datetime.now()) + " ***\n")
	file.write("Executing " + os.path.basename(__file__) + "...\n\n")
	file.write("結轉日期區間:" + start_date + "~" + end_date + "\n")

	#計算區間的天數，作為迴圈終止條件
	date_fmt = "%Y/%m/%d"
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
			str_date = parser.parse(str(dt)).strftime("%Y/%m/%d")
			
		#print(str_date + "\n")
		#讀取日期當天報價CSV檔
		TSE_QUO_READ_CSV(str_date)
		
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

	print("證交所上市收盤資料CSV檔案，轉入資料庫作業結束...\n\n\n")

if __name__ == '__main__':
	MAIN_TSE_QUO_READ_CSV()