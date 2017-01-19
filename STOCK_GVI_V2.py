# -*- coding: utf-8 -*-
"""
Created on Tue Jan 17 13:30:44 2017

@author: bryson0083
"""

import sqlite3
import datetime
from dateutil import parser
import pandas as pd
#import sys

def store_db(arg_df, arg_date):
	#前處理->清除先前已跌出榜外的資料
	sqlstr = "delete from STOCK_GVI where STATUS='ABD' "
	conn.execute(sqlstr)
	conn.commit()
	
	#前處理->所有資料狀態修改
	sqlstr = "update STOCK_GVI set STATUS='ABD' "
	conn.execute(sqlstr)
	conn.commit()
	
	#前處理->所有資料排名，備份到prev_rank
	sqlstr = "update STOCK_GVI set prev_rank = rank "
	conn.execute(sqlstr)
	conn.commit()	
	
	cnt = 1
	commit_flag = "Y"
	for i in range(0,len(arg_df)):
		comp_id = str(arg_df.iloc[i][0])
		comp_name = str(arg_df.iloc[i][1])
		price = arg_df.iloc[i][2]
		eps = arg_df.iloc[i][3]
		bvps = arg_df.iloc[i][4]
		pbr = arg_df.iloc[i][5]
		sroe = arg_df.iloc[i][6]
		estm_y_roe = arg_df.iloc[i][7]
		gvi = arg_df.iloc[i][8]
		rank = cnt
		rec_date = arg_date
		
		# 最後維護日期時間
		str_date = str(datetime.datetime.now())
		date_last_maint = parser.parse(str_date).strftime("%Y%m%d")
		time_last_maint = parser.parse(str_date).strftime("%H%M%S")
		prog_last_maint = "STOCK_GVI"
		
		#print(comp_id + " " + comp_name + " " + str(price) + "\n")
		
		#檢查是否已經在榜內清單中
		sqlstr  = "select PREV_RANK, REC_DATE from STOCK_GVI "
		sqlstr += "where COMP_ID = '" + comp_id + "'"
		
		#print(sqlstr)
		cursor = conn.execute(sqlstr)
		result = cursor.fetchone()
		
		if result is None:
			is_existed = "N"
		else:
			is_existed = "Y"
			
			if rank > result[0]:
				status = "DOW"
			elif rank == result[0]:
				status = "REM"
			else:
				status = "UPP"
			
			rec_date = result[1]
		
		#關閉cursor
		cursor.close()
		
		#計算各項績效值
		#RATE_1 計算進榜至今漲跌幅
		sqlstr  = "select CLOSE from STOCK_QUO "
		sqlstr += "where "
		sqlstr += "QUO_DATE='" + rec_date + "' and "
		sqlstr += "SEAR_COMP_ID='" + comp_id + ".TW' "
		
		cursor = conn.execute(sqlstr)
		result = cursor.fetchone()
		
		if result is not None:
			rec_price = result[0]
		else:
			rec_price = price
		
		rate_1 = (price - rec_price) / rec_price * 100
		
		#關閉cursor
		cursor.close()
		
		#print(comp_id + " " + rec_date + " " + str(price) + " " + str(rec_price) + " " + str(rate_1) + "\n")
		#sys.exit("test end...")
		
		#RATE_2 計算近一周績效(固定往前推7天)
		str_date = str(datetime.datetime.now())
		str_date = parser.parse(str_date).strftime("%Y/%m/%d")
		end_date = str_date
		
		date_1 = datetime.datetime.strptime(end_date, "%Y/%m/%d")
		prev_date = date_1 + datetime.timedelta(days=-7)
		prev_date = str(prev_date)[0:10]
		prev_date = parser.parse(prev_date).strftime("%Y%m%d")
		
		sqlstr  = "select CLOSE from STOCK_QUO "
		sqlstr += "where "
		sqlstr += "QUO_DATE<='" + prev_date + "' and "
		sqlstr += "SEAR_COMP_ID='" + comp_id + ".TW' "
		sqlstr += "order by QUO_DATE desc "
		sqlstr += "limit 1 "
		
		cursor = conn.execute(sqlstr)
		result = cursor.fetchone()
		
		if result is not None:
			prev_price = result[0]
		else:
			prev_price = price
		
		rate_2 = (price - prev_price) / prev_price * 100
		#print(comp_id + " " + prev_date + " " + str(price) + " " + str(prev_price) + " " + str(rate_2) + "\n")
		
		#關閉cursor
		cursor.close()
		
		#RATE_3 計算近一個月績效(固定往前推30天)
		str_date = str(datetime.datetime.now())
		str_date = parser.parse(str_date).strftime("%Y/%m/%d")
		end_date = str_date
		
		date_1 = datetime.datetime.strptime(end_date, "%Y/%m/%d")
		prev_date = date_1 + datetime.timedelta(days=-30)
		prev_date = str(prev_date)[0:10]
		prev_date = parser.parse(prev_date).strftime("%Y%m%d")
		
		sqlstr  = "select CLOSE from STOCK_QUO "
		sqlstr += "where "
		sqlstr += "QUO_DATE<='" + prev_date + "' and "
		sqlstr += "SEAR_COMP_ID='" + comp_id + ".TW' "
		sqlstr += "order by QUO_DATE desc "
		sqlstr += "limit 1 "
		
		cursor = conn.execute(sqlstr)
		result = cursor.fetchone()
		
		if result is not None:
			prev_price = result[0]
		else:
			prev_price = price
		
		rate_3 = (price - prev_price) / prev_price * 100
		#print(comp_id + " " + prev_date + " " + str(price) + " " + str(prev_price) + " " + str(rate_2) + "\n")
		
		#關閉cursor
		cursor.close()
		
		#RATE_4 計算近半年績效(固定往前推180天)
		str_date = str(datetime.datetime.now())
		str_date = parser.parse(str_date).strftime("%Y/%m/%d")
		end_date = str_date
		
		date_1 = datetime.datetime.strptime(end_date, "%Y/%m/%d")
		prev_date = date_1 + datetime.timedelta(days=-180)
		prev_date = str(prev_date)[0:10]
		prev_date = parser.parse(prev_date).strftime("%Y%m%d")
		
		sqlstr  = "select CLOSE from STOCK_QUO "
		sqlstr += "where "
		sqlstr += "QUO_DATE<='" + prev_date + "' and "
		sqlstr += "SEAR_COMP_ID='" + comp_id + ".TW' "
		sqlstr += "order by QUO_DATE desc "
		sqlstr += "limit 1 "
		
		cursor = conn.execute(sqlstr)
		result = cursor.fetchone()
		
		if result is not None:
			prev_price = result[0]
		else:
			prev_price = price
		
		rate_4 = (price - prev_price) / prev_price * 100
		#print(comp_id + " " + prev_date + " " + str(price) + " " + str(prev_price) + " " + str(rate_2) + "\n")
		
		#關閉cursor
		cursor.close()
		
		if is_existed == "N":
			sqlstr  = "insert into STOCK_GVI (COMP_ID, COMP_NAME, EPS, BVPS, PRICE, "
			sqlstr += "PBR, SROE, ESTM_Y_ROE, GVI, REC_DATE, RANK, PREV_RANK, "
			sqlstr += "STATUS,DATE_LAST_MAINT,TIME_LAST_MAINT,PROG_LAST_MAINT"
			sqlstr += ") values ("
			sqlstr += "'" + comp_id + "', "
			sqlstr += "'" + comp_name + "',"
			sqlstr += str(eps) + ","
			sqlstr += str(bvps) + ","
			sqlstr += str(price) + ","
			sqlstr += str(pbr) + ","
			sqlstr += str(sroe) + ","
			sqlstr += str(estm_y_roe) + ","
			sqlstr += str(gvi) + ","
			sqlstr += "'" + rec_date + "',"
			sqlstr += str(rank) + ","
			sqlstr += "0,"
			sqlstr += "'NEW',"
			sqlstr += "'" + date_last_maint + "',"
			sqlstr += "'" + time_last_maint + "',"
			sqlstr += "'" + prog_last_maint + "' "
			sqlstr += ")"
		else:
			sqlstr  = "update STOCK_GVI set "
			sqlstr += "EPS=" + str(eps) + ", "
			sqlstr += "BVPS=" + str(bvps) + ", "
			sqlstr += "PRICE=" + str(price) + ", "
			sqlstr += "PBR=" + str(pbr) + ", "
			sqlstr += "SROE=" + str(sroe) + ", "
			sqlstr += "ESTM_Y_ROE=" + str(estm_y_roe) + ", "
			sqlstr += "GVI=" + str(gvi) + ", "
			sqlstr += "RANK=" + str(rank) + ", "
			sqlstr += "STATUS='" + status + "', "
			sqlstr += "RATE_1=" + str(rate_1) + ", "
			sqlstr += "RATE_2=" + str(rate_2) + ", "
			sqlstr += "RATE_3=" + str(rate_3) + ", "
			sqlstr += "RATE_4=" + str(rate_4) + ", "
			sqlstr += "DATE_LAST_MAINT='" + date_last_maint + "', "
			sqlstr += "TIME_LAST_MAINT='" + time_last_maint + "', "
			sqlstr += "PROG_LAST_MAINT='" + prog_last_maint + "' "
			sqlstr += "where "
			sqlstr += "COMP_ID='" + comp_id + "' "			
			
		try:
			#print(sqlstr + "\n")
			conn.execute(sqlstr)
		except sqlite3.Error as er:
			commit_flag = "N"
			print(sqlstr + "\n")
			print("process STOCK_GVI error er=" + er.args[0] + "\n")
			
		cnt += 1
	
	if commit_flag == "Y":
		conn.commit()
	else:
		conn.execute("rollback")

#############################################
# Main                                      #
#############################################

dt = datetime.datetime.now()
str_date = parser.parse(str(dt)).strftime("%Y%m%d")
print(str_date)

#for 手動跑
#str_date = "20170103"

#建立資料庫連線
conn = sqlite3.connect("market_price.sqlite")

sqlstr  = "SELECT a.SEAR_COMP_ID, b.COMP_ID, a.CLOSE, b.COMP_NAME "
sqlstr += "from STOCK_QUO a, STOCK_COMP_LIST b "
sqlstr += "where "
sqlstr += "a.QUO_DATE = '" + str_date + "' AND "
sqlstr += "a.SEAR_COMP_ID = b.SEAR_COMP_ID "
sqlstr += "ORDER BY a.SEAR_COMP_ID "
#sqlstr += "limit 100 "

#print(sqlstr)
cursor = conn.execute(sqlstr)
result = cursor.fetchall()

re_len = len(result)
if re_len > 0:
	ls_gvi = []
	for row in result:
		sear_comp_id = row[0]
		comp_id = row[1]
		price = row[2]
		comp_name = row[3]
		
		#讀取EPS(資料來源是年累計值，因此要扣掉前一期，取得單季EPS)
		sqlstr  = "select EPS from MOPS_YQ "
		sqlstr += "where COMP_ID = '" + comp_id + "' "
		sqlstr += "order by YYYY || QQ desc "
		sqlstr += "limit 2 "
		
		cursor2 = conn.execute(sqlstr)
		result2 = cursor2.fetchall()
		re_len2 = len(result2)
		
		k = 1
		eps1 = 0
		eps0 = 0
		if re_len2 > 0:
			for row2 in result2:
				if k == 1:
					eps1 = row2[0]
				elif k == 2:
					eps0 = row2[0]
				k += 1
				
			#計算單季EPS
			eps = eps1 - eps0
		else:
			eps = 0
		
		#關閉cursor
		cursor2.close()
		
		#讀取BVPS
		sqlstr  = "select BVPS from MOPS_YQ "
		sqlstr += "where COMP_ID = '" + comp_id + "' "
		sqlstr += "order by YYYY || QQ desc "
		sqlstr += "limit 1 "
		
		cursor2 = conn.execute(sqlstr)
		result2 = cursor2.fetchone()
		
		if result2 is not None:
			bvps = result2[0]
		else:
			bvps = 0
		
		#關閉cursor
		cursor2.close()
		
		#print(sear_comp_id + " " + comp_id + " " + str(price) + " " + comp_name + " " + str(eps) + " " + str(bvps) + "\n")
		if price != 0:
			bop = bvps / price 	#淨值股價比
		else:
			bop = 0
		
		if bvps != 0:
			pb = price / bvps 	#股價淨值比
		else:
			pb = 0
		
		if bvps != 0:
			sroe = eps / bvps 	#單季ROE
		else:
			sroe = 0
		
		#預估年ROE
		#(原書上有做判斷，若單季ROE超過5%以上，
		# 則以5%作為計算值，這部分我沒有照書上的設定)
		#estm_y_roe = 4 * min(sroe, 0.05)
		estm_y_roe = 4 * sroe
		
		#GVI指標計算
		gvi = bop * (1 + estm_y_roe)**5
		
		#計算完的結果，塞到list
		data = [comp_id, comp_name, price, eps, bvps, pb, sroe, estm_y_roe, gvi]
		ls_gvi.append(data)
		#print(sear_comp_id + " " + comp_id + " " + str(price) + " " + comp_name + " " + str(eps) + " " + str(bvps) + " " + str(gvi) + "\n")
		
	df = pd.DataFrame(ls_gvi, columns = ['股票編號','股票名稱', '股價', 'EPS','BVPS','股價淨值比','季ROE','估計年ROE','GVI'])
	df = df.sort_values(by=['GVI'], ascending=[False])[1:101]
	#print(df)
	
	"""
	#計算結果直接寫入excel檔
	file_name = 'STOCK_GVI_' + str_date + '.xlsx'
	writer = pd.ExcelWriter(file_name, engine='xlsxwriter')
	df.to_excel(writer, sheet_name='GVI', index=False)
	writer.save()
	"""
	
	#運算結果寫入db
	store_db(df, str_date)
	
else:
	print(str_date + " no data....")

#關閉cursor
cursor.close()

#關閉資料庫連線
conn.close