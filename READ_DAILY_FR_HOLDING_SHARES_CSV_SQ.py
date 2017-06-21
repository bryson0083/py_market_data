# -*- coding: utf-8 -*-
"""
櫃買中心外資及陸資投資持股統計~每日收盤CSV檔讀取並寫入資料庫

@author: Bryson Xue
@target_rul: 

@Note: 
	櫃買中心外資及陸資投資持股統計
	讀取下載後的CSV檔，寫入資料庫
	
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
	print("讀取" + arg_date + "櫃買中心外資及陸資投資持股統計CSV資料檔.\n")
	file_name = "./daily_fr_holding_shares_sq/" + arg_date + ".csv"
	
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

	#取得CSV檔最後一筆資料的位置
	last_row_idx = len(quo_list)

	try:
		# 讀取當天外資及陸資投資持股統計資料到list中
		i = 0
		idx = st_idx
		all_data = []
		while True:
			#for item in quo_list[idx]:
			#	print("##" + str(item) + "##")
			
			#讀取到CSV檔到沒有資料，跳出迴圈
			if idx == last_row_idx:
				#print("終止條件啟動")
				break
			
			data = [str(item) for item in quo_list[idx]]
			all_data.append(data)
			idx += 1

	except Exception as e:
		print("$$$ Err:" + arg_date + " 外資及陸資投資持股統計CSV資料讀取異常. $$$")
		print(e)
		file.write("$$$ Err:" + arg_date + " 外資及陸資投資持股統計CSV資料讀取異常. $$$")
		file.write(e)
		err_flag = True
	
	#all_data list拋到pandas
	df = pd.DataFrame(all_data[1:], columns = all_data[0])
	df2 = df.loc[:,['證券代號', '證券名稱', '發行股數', '外資及陸資尚可投資股數', '全體外資及陸資持有股數', '外資及陸資尚可投資比率', '全體外資及陸資持股比率', '外資及陸資共用法令投資上限比率', '陸資法令投資上限比率', '最近一次上櫃公司申報外資持股異動日期']]
	#print(df)

	#寫入、更新資料庫
	STORE_DB(df2, arg_date)

def STORE_DB(arg_df, arg_date):
	global err_flag

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

		issued_shares = arg_df.loc[i]['發行股數'].replace(",","").strip()
		fr_inv_bls = arg_df.loc[i]['外資及陸資尚可投資股數'].replace(",","").strip()
		fr_holding_shares = arg_df.loc[i]['全體外資及陸資持有股數'].replace(",","").strip()
		fr_inv_bls_rt = arg_df.loc[i]['外資及陸資尚可投資比率'].replace(",","").strip()
		fr_holding_rt = arg_df.loc[i]['全體外資及陸資持股比率'].replace(",","").strip()
		fr_inv_uplimit = arg_df.loc[i]['外資及陸資共用法令投資上限比率'].replace(",","").strip()
		cn_inv_uplimit = arg_df.loc[i]['陸資法令投資上限比率'].replace(",","").strip()
		last_claim_date = str(arg_df.loc[i]['最近一次上櫃公司申報外資持股異動日期'].replace("/","").strip())

		if len(last_claim_date) < 6:
			last_claim_date = ' '
		else:
			yyyy = int(last_claim_date[0:3]) + 1911
			last_claim_date = str(yyyy) + last_claim_date[3:]

		# 最後維護日期時間
		str_date = str(datetime.datetime.now())
		date_last_maint = parser.parse(str_date).strftime("%Y%m%d")
		time_last_maint = parser.parse(str_date).strftime("%H%M%S")
		prog_last_maint = "READ_DAILY_FR_HOLDING_SHARES_CSV_SQ"

		#if comp_id == "1102.TW":
		#	print(comp_id + "#" + comp_name + "#" + str(last_claim_date) + "#" + str(fr_holding_rt) + "#" + str(fr_inv_uplimit) + "#" + str(cn_inv_uplimit) + "#\n")
		#	break

		#檢查資料是否已存在
		strsql  = "select count(*) from STOCK_FR_HOLDING_SHARES "
		strsql += "where QUO_DATE = '" + quo_date + "' and "
		strsql += "SEAR_COMP_ID='" + comp_id + "' "
		
		#print(strsql + "\n")
		cursor = conn.execute(strsql)
		result = cursor.fetchone()
		
		if result[0] == 0:
			#print(comp_id + "沒資料\n")
			# 日報價資料寫入
			strsql  = "insert into STOCK_FR_HOLDING_SHARES ("
			strsql += "QUO_DATE,SEAR_COMP_ID,COMP_NAME,ISSUED_SHARES,FR_INV_BLS,"
			strsql += "FR_HOLDING_SHARES,FR_INV_BLS_RT,FR_HOLDING_RT,"
			strsql += "FR_INV_UPLIMIT,CN_INV_UPLIMIT,LAST_CLAIM_DATE,"
			strsql += "DATE_LAST_MAINT,TIME_LAST_MAINT,PROG_LAST_MAINT"
			strsql += ") values ("
			strsql += "'" + quo_date + "',"
			strsql += "'" + comp_id + "',"
			strsql += "'" + comp_name + "',"
			strsql += str(issued_shares) + ","
			strsql += str(fr_inv_bls) + ","
			strsql += str(fr_holding_shares) + ","
			strsql += str(fr_inv_bls_rt) + ","
			strsql += str(fr_holding_rt) + ","
			strsql += str(fr_inv_uplimit) + ","
			strsql += str(cn_inv_uplimit) + ","
			strsql += "'" + last_claim_date + "',"
			strsql += "'" + date_last_maint + "',"
			strsql += "'" + time_last_maint + "',"
			strsql += "'" + prog_last_maint + "' "
			strsql += ")"

		else:
			#print(comp_id + "有資料\n")
			#針對現有資料更新
			strsql  = "update STOCK_FR_HOLDING_SHARES set "
			strsql += "ISSUED_SHARES=" + str(issued_shares) + ","
			strsql += "FR_INV_BLS=" + str(fr_inv_bls) + ","
			strsql += "FR_HOLDING_SHARES=" + str(fr_holding_shares) + ","
			strsql += "FR_INV_BLS_RT=" + str(fr_inv_bls_rt) + ","
			strsql += "FR_HOLDING_RT=" + str(fr_holding_rt) + ","
			strsql += "FR_INV_UPLIMIT=" + str(fr_inv_uplimit) + ","
			strsql += "CN_INV_UPLIMIT=" + str(cn_inv_uplimit) + ","
			strsql += "LAST_CLAIM_DATE='" + str(last_claim_date) + "',"
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
			print("insert/update STOCK_FR_HOLDING_SHARES er=" + er.args[0] + "\n")
			print(comp_id + " " + comp_name + " " + quo_date + "資料異常.\n")
			print(strsql + "\n")
			file.write("insert/update STOCK_FR_HOLDING_SHARES er=" + er.args[0] + "\n")
			file.write(comp_id + " " + comp_name + " " + quo_date + "資料異常.\n")
			file.write(strsql + "\n")

		#關閉cursor
		cursor.close()

	# 最後commit
	if commit_flag == True:
		conn.commit()
		print(quo_date + "櫃買中心外資及陸資投資持股統計，寫入成功.\n")
		file.write(quo_date + "櫃買中心外資及陸資投資持股統計，寫入成功.\n")
	else:
		conn.execute("rollback")
		print(quo_date + "櫃買中心外資及陸資投資持股統計，寫入失敗Rollback.\n")
		file.write(quo_date + "櫃買中心外資及陸資投資持股統計，寫入失敗Rollback.\n")
	
	#關閉資料庫連線
	conn.close

############################################################################
# Main                                                                     #
############################################################################
print("Executing READ_DAILY_FR_HOLDING_SHARES_CSV_SQ...")
global err_flag
err_flag = False

#起訖日期(預設跑當天日期到往前推7天)
dt = datetime.datetime.now()
start_date = dt + datetime.timedelta(days=-7)
start_date = parser.parse(str(start_date)).strftime("%Y%m%d")
end_date = parser.parse(str(dt)).strftime("%Y%m%d")

#for需要時手動設定日期區間用
#start_date = "20170101"
#end_date = "20170619"

# 寫入LOG File
str_date = str(datetime.datetime.now())
str_date = parser.parse(str_date).strftime("%Y%m%d")
name = "READ_DAILY_FR_HOLDING_SHARES_CSV_SQ_LOG_" + str_date + ".txt"
file = open(name, 'a', encoding = 'UTF-8')

print_dt = str(str_date) + (' ' * 22)
print("##############################################")
print("##       櫃買中心外資及陸資投資持股統計     ##")
print("##              CSV檔寫入資料庫             ##")
print("##                                          ##")
print("##  datetime: " + print_dt +               "##")
print("##############################################")

print("結轉日期" + start_date + "~" + end_date)

tStart = time.time()#計時開始
file.write("\n\n\n*** LOG datetime  " + str(datetime.datetime.now()) + " ***\n")
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

print("End of prog...")