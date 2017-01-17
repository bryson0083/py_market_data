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

str_date = "20170116"

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
		
		#讀取EPS、BVPS
		sqlstr  = "select EPS, BVPS from MOPS_YQ "
		sqlstr += "where COMP_ID = '" + comp_id + "' "
		sqlstr += "order by YYYY || QQ desc "
		sqlstr += "limit 1 "
		
		cursor2 = conn.execute(sqlstr)
		result2 = cursor2.fetchone()
		
		if result2 is not None:
			eps = result2[0]
			bvps = result2[1]
		else:
			eps = 0
			bvps = 0
		
		#關閉cursor
		cursor2.close()
		
		#print(sear_comp_id + " " + comp_id + " " + str(price) + " " + comp_name + " " + str(eps) + " " + str(bvps) + "\n")
		
		if price > 0 and eps != 0 and bvps != 0:
			gvi = (bvps/price)*(1+(4*eps/bvps))**5
			
			data = [comp_id, comp_name, price, eps, bvps, gvi]
			ls_gvi.append(data)
		else:
			gvi = 0
		
		#print(sear_comp_id + " " + comp_id + " " + str(price) + " " + comp_name + " " + str(eps) + " " + str(bvps) + " " + str(gvi) + "\n")
		
	df = pd.DataFrame(ls_gvi, columns = ['股票編號','股票名稱', '股價', 'EPS','BVPS','GVI'])
	df = df.sort_values(by=['GVI'], ascending=[False])
	writer = pd.ExcelWriter('STOCK_GVI.xlsx', engine='xlsxwriter')
	df.to_excel(writer, sheet_name='Sheet1')
	#print(df)
else:
	print("no data....")


#關閉cursor
cursor.close()

#關閉資料庫連線
conn.close

