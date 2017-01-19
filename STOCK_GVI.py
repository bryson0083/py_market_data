# -*- coding: utf-8 -*-
"""
Created on Tue Jan 17 13:30:44 2017

@author: bryson0083
"""

import sqlite3
import datetime
from dateutil import parser
import pandas as pd

dt = datetime.datetime.now()
str_date = parser.parse(str(dt)).strftime("%Y%m%d")
print(str_date)

#str_date = "20170117"

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
	#print(df.values)
	
	#運算結果寫入EXCEL檔
	file_name = 'STOCK_GVI_' + str_date + '.xlsx'
	writer = pd.ExcelWriter(file_name, engine='xlsxwriter')
	df.to_excel(writer, sheet_name='GVI', index=False)
	writer.save()
	
else:
	print("no data....")

#關閉cursor
cursor.close()

#關閉資料庫連線
conn.close