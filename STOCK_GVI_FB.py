# -*- coding: utf-8 -*-
"""
GVI指標選股結果，上傳Firebase

@author: Bryson Xue

@target_rul: 

@Note: 
	選股結果，上傳Firebase，供網頁讀取

"""
from firebase import firebase
import sqlite3
import os

def dict_factory(cursor, row):
	d = {}
	for idx, col in enumerate(cursor.description):
		d[col[0]] = row[idx]
	return d

def MAIN_STOCK_GVI_FB():
	print("Executing " + os.path.basename(__file__) + "...")

	#建立sqlite資料庫連線
	conn = sqlite3.connect("market_price.sqlite")

	sqlstr  = "select COMP_ID, COMP_NAME, STOCK_TYPE, EPS, BVPS, PRICE, PBR, SROE, ESTM_Y_ROE,"
	sqlstr += "GVI,REC_DATE,RANK,PREV_RANK,STATUS,RATE_1,RATE_2,RATE_3,RATE_4 "
	sqlstr += "from STOCK_GVI "
	sqlstr += "where "
	sqlstr += "STATUS <> 'ABD' "
	sqlstr += "order by RANK "
	#sqlstr += "limit 10 "

	conn.row_factory = dict_factory
	cursor = conn.execute(sqlstr)
	result = cursor.fetchall()

	#print(result)

	#建立firebase資料連線
	db_url = 'https://brysonxue-bfca6.firebaseio.com/'
	fdb = firebase.FirebaseApplication(db_url, None)

	#轉存firebase前，清空線上的資料
	fdb.delete('/GVI', None)

	#DB資料轉存firebase
	for row in result:
		fdb.post('/GVI', row)

	#關閉cursor
	cursor.close()

	#關閉資料庫連線
	conn.close()

	print("End of prog...\n\n")

if __name__ == '__main__':
	MAIN_STOCK_GVI_FB()