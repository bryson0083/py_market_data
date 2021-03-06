# -*- coding: utf-8 -*-
"""
技術分析選股 TYPE 10 上傳Firebase

@author: Bryson Xue

@target_rul: 

@Note: 
	選股結果上傳Firebase

"""
import datetime
from dateutil import parser
import pandas as pd
import xlrd
import sys, os
from firebase import firebase

def MAIN_STOCK_SELECT_TYPE10_FB():
	print("Executing " + os.path.basename(__file__) + "...")

	dt=datetime.datetime.now()
	str_date = parser.parse(str(dt)).strftime("%Y%m%d")
	exl_name = "STOCK_SELECT_TYPE10_" + str_date + ".xlsx"

	try:
		wb = xlrd.open_workbook(exl_name, on_demand = True)
	except Exception as e:
		print('Err: 讀取 ' + exl_name + ' 選股excel檔異常.')
		print(e.args)
		return

	sh = wb.sheet_by_index(0)
	row_cnt = sh.nrows

	na_flag = False
	#print(sh.cell(0,0).value)
	if sh.cell(0,0).value == '今日無符合條件之股票.':
		na_flag = True

	row_ls = []
	if na_flag == False:
		for i in range(1,row_cnt):
			d = {}
			#print(i)
			d['股票代號'] = sh.cell(i,0).value.replace('.TW','')
			d['名稱'] = sh.cell(i,1).value
			d['外資買賣超天數'] = sh.cell(i,4).value
			d['投信買賣超天數'] = sh.cell(i,5).value
			d['自營商買賣超天數'] = sh.cell(i,6).value
			d['類別'] = sh.cell(i,7).value
			#print(d)
			row_ls.append(d)

	#關閉excel檔案
	wb.release_resources()
	del wb

	#print(row_ls)
	
	try:
		#建立firebase資料連線
		db_url = 'https://brysonxue-bfca6.firebaseio.com/'
		fdb = firebase.FirebaseApplication(db_url, None)

		#轉存firebase前，清空線上的資料
		fdb.delete('/STOCK_S10', None)

		if na_flag == False:
			#資料轉存firebase
			for row in row_ls:
				fdb.post('/STOCK_S10', row)

			print('上傳STOCK_S10成功.')
		else:
			print('今日無符合條件之股票，不做資料上傳.')

	except Exception as e:
		print('Err: 資料上傳Firebase table => STOCK_S10異常.')
		print(e.args)
		return

	print("End of prog.\n\n")

if __name__ == '__main__':
	MAIN_STOCK_SELECT_TYPE10_FB()	