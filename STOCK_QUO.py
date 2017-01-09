# 上市公司清單讀取(STOCK_QUO)
#
# 資料來源: 讀取股價歷史資料
# 
#
# last_modify: 2016/11/08
#

import sqlite3
import sys
import os.path
import time
import pandas as pd
import pandas_datareader.data as web
import datetime
from dateutil import parser
from dateutil.parser import parse
from random import randint

def GetQuo(sear_comp_id):
	start = datetime.datetime.strptime('12/01/2016', '%m/%d/%Y')
	end = datetime.datetime.strptime('01/03/2017', '%m/%d/%Y')
	file.write("\nGetQuo 開始擷取 " + sear_comp_id +"  日期區間:" + str(start) + "~" + str(end) + "股價報價資料.\n")
	print("\nGetQuo 開始擷取 " + sear_comp_id +"  日期區間:" + str(start) + "~" + str(end) + "股價報價資料.\n")
	
	commit_flag = "Y"
	try:
		com_stock = web.DataReader(sear_comp_id,'yahoo',start,end)
		#print("GetQuo 原始資料 From Yahoo\n" + str(com_stock) + "\n")

		df = pd.DataFrame(com_stock)
		for i in range(0,len(df)):
			quo_date = str(df.index[i])	# 報價日期
			quo_date = parser.parse(quo_date).strftime("%Y%m%d")
			sear_comp_id = sear_comp_id	# 公司代號
			q_open = df.iloc[i][0]	# 開盤價
			q_high = df.iloc[i][1]	# 最高價
			q_low = df.iloc[i][2]	# 最低價
			q_close = df.iloc[i][3]	# 收盤價
			q_vol = df.iloc[i][4]	# 成交量
			q_adj_close = df.iloc[i][5]	# 調整收盤價

			# 最後維護日期時間
			str_date = str(datetime.datetime.now())
			date_last_maint = parser.parse(str_date).strftime("%Y%m%d")
			time_last_maint = parser.parse(str_date).strftime("%H%M%S")
			prog_last_maint = "STOCK_QUO"

			# 讀取報價資料是否已存在
			strsql  = "select count(*) from STOCK_QUO "
			strsql += "where "
			strsql += "quo_date='" + quo_date + "' and "
			strsql += "sear_comp_id='" + sear_comp_id + "'"

			cursor2 = conn.execute(strsql)
			result = cursor2.fetchone()

			if result[0] == 0:
				# 日報價資料寫入
				strsql  = "insert into STOCK_QUO ("
				strsql += "SEAR_COMP_ID,QUO_DATE,OPEN,HIGH,LOW,"
				strsql += "CLOSE,VOL,ADJ_CLOSE,"
				strsql += "DATE_LAST_MAINT,TIME_LAST_MAINT,PROG_LAST_MAINT"
				strsql += ") values ("
				strsql += "'" + sear_comp_id + "',"
				strsql += "'" + quo_date + "',"
				strsql += str(q_open) + ","
				strsql += str(q_high) + ","
				strsql += str(q_low) + ","
				strsql += str(q_close) + ","
				strsql += str(q_vol) + ","
				strsql += str(q_adj_close) + ","
				strsql += "'" + date_last_maint + "',"
				strsql += "'" + time_last_maint + "',"
				strsql += "'" + prog_last_maint + "' "
				strsql += ")"

				try:
					#print(strsql + "\n")
					conn.execute(strsql)
				except sqlite3.Error as er:
					commit_flag = "N"
					print("insert into STOCK_QUO er=" + er.args[0] + "\n")
					print(sear_comp_id + " " + quo_date + "資料寫入異常...Rollback!\n")
					file.write("insert into STOCK_QUO er=" + er.args[0] + "\n")
					file.write(sear_comp_id + " " + quo_date + "資料寫入異常...Rollback!\n")
					conn.execute("rollback")
			#else:
				#print(sear_comp_id + " " + quo_date + "報價資料已存在!\n")
				#file.write("\n" + sear_comp_id + " " + quo_date + "報價資料已存在!\n")

			# 關閉cursor
			cursor2.close()

			# 讀取上市公司清單，最新報價日期欄位
			strsql  = "select LATEST_QUO_DATE from STOCK_COMP_LIST "
			strsql += "where sear_comp_id='" + sear_comp_id + "'"

			cursor2 = conn.execute(strsql)
			result2 = cursor2.fetchone()

			if result2 is not None:
				latest_quo_date = str(result2[0])
			else:
				latest_quo_date = ""

			# 關閉cursor
			cursor2.close()

			#print("quo_date=" + quo_date + ",latest_quo_date=" + latest_quo_date + "\n")
			if quo_date > latest_quo_date:
				# 更新上市公司清單，最新報價日期欄位
				strsql  = "update STOCK_COMP_LIST set "
				strsql += "LATEST_QUO_DATE = '" + quo_date + "' "
				strsql += "where sear_comp_id = '" + sear_comp_id + "'"

				try:
					conn.execute(strsql)
				except sqlite3.Error as er:
					commit_flag = "N"
					print("update STOCK_COMP_LIST LATEST_QUO_DATE er=" + er.args[0] + "\n")
					print(sear_comp_id + " " + quo_date + "資料更新異常...Rollback!\n")
					file.write("update STOCK_COMP_LIST LATEST_QUO_DATE er=" + er.args[0] + "\n")
					file.write(sear_comp_id + " " + quo_date + "資料更新異常...Rollback!\n")
					conn.execute("rollback")

		# 最後commit
		if commit_flag == "Y":
			conn.commit()
			print(sear_comp_id + " " + quo_date + "報價資料寫入成功.\n")
			file.write(sear_comp_id + " " + quo_date + "報價資料寫入成功.\n")

	except:
		NoQuo(sear_comp_id)
		print("@Err. GetQuo sear id=" + sear_comp_id +"  " + str(start) + "~" + str(end)+"\n")
		print("無法讀取YAHOO股價資料，或無資料!!")
		file.write("@Err. GetQuo sear id=" + sear_comp_id +"  " + str(start) + "~" + str(end)+"\n")
		file.write("無法讀取YAHOO股價資料，或無資料!!\n")

def NoQuo(sear_comp_id):
	strsql  = "update STOCK_COMP_LIST set "
	strsql += "LATEST_QUO_DATE = 'NA' "
	strsql += "where sear_comp_id = '" + sear_comp_id + "'"
	
	try:
		conn.execute(strsql)
	except sqlite3.Error as er:
		print("update STOCK_COMP_LIST in NoQuo er=" + er.args[0] + "\n")
		file.write("@Err. NoQuo sear id=" + sear_comp_id +"  " + "\n")
		conn.execute("rollback")


# 寫入LOG File
dt=datetime.datetime.now()

print("##############################################")
print("##           個股歷史股價資料讀取           ##")
print("##               STOCK_QUO.PY               ##")
print("##                                          ##")
print("##   datetime: " + str(dt) +            "   ##")
print("##############################################")

str_date = str(dt)
str_date = parser.parse(str_date).strftime("%Y%m%d")

name = "STOCK_QUO_LOG_" + str_date + ".txt"
file = open(name, 'a', encoding = 'UTF-8')

tStart = time.time()#計時開始
file.write("\n\n\n*** LOG datetime  " + str(datetime.datetime.now()) + " ***\n")

conn = sqlite3.connect("market_price.sqlite")

# 從上市公司清單中，讀取需要更新股價資料的公司
sqlstr  = "select comp_id, sear_comp_id from STOCK_COMP_LIST "
sqlstr += "where "
sqlstr += "length(SEAR_COMP_ID) < 8 and "
sqlstr += "latest_quo_date <> 'NA' and "
sqlstr += "latest_quo_date < '20170103' "
sqlstr += "order by comp_id "
#sqlstr += "limit 100"

cursor = conn.execute(sqlstr)
result = cursor.fetchall()

re_len = len(result)
#print("re_len=" + str(re_len) + "\n")

if re_len > 0:
	for row in result:
		sear_comp_id = row[1]
		#print(row[1])
		GetQuo(sear_comp_id)
		
		# 隨機等待3~9秒的時間
		#random_sec = randint(1,3)
		#print("Waiting sec=" + str(random_sec))
		#time.sleep(random_sec)
else:
	file.write(sear_comp_id + "無須更新資料或無該公司資料...\n")
	print(sear_comp_id + "無須更新資料或無該公司資料...")


tEnd = time.time()#計時結束
file.write ("\n\n\n結轉耗時 %f sec\n" % (tEnd - tStart)) #會自動做進位
file.write("*** End LOG ***\n")

# Close File
file.close()

# 關閉cursor
cursor.close()

# 關閉資料庫連線
conn.close()

print("End of program...")