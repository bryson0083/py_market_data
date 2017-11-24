# -*- coding: utf-8 -*-
"""
判斷台北股市當天是否有開盤交易

@author: Bryson Xue

@target_rul: 

@Note: 

"""
import os
import csv
import datetime
from dateutil.parser import parse
from dateutil import parser

def MAIN_CK_OPEN(arg_date):
	#print("Executing " + os.path.basename(__file__) + "...")

	# 轉民國年日期
	str_date = str(int(arg_date[0:4]) - 1911) + arg_date[4:]
	str_date = str_date.replace("/","")
	#print("讀取" + str_date + "收盤資料CSV檔案.")
	
	file_name = "./tse_quo_data/" + str_date + ".csv"
	
	#判斷CSV檔案是否存在，若無檔案則跳回主程式
	is_existed = os.path.exists(file_name)
	if is_existed == False:
		#print(arg_date + "無報價CSV檔.")
		return False
	
	#讀取每日報價CSV檔
	with open(file_name, 'r') as f:
		reader = csv.reader(f)
		quo_list = list(reader)
	
	#關閉CSV檔案
	f.close

	#檢查檔案當天是否有交易資料，若無交易資料則跳回主程式
	#(若檔案內容為"當天無收盤資料"，表示當天未開盤)
	for item in quo_list:
		for j in item:
			if j == "當天無收盤資料":
				#print(arg_date + "當天未開盤，無交易資料.")
				return False
			else:
				return True

if __name__ == '__main__':
	#取得目前時間
	dt = datetime.datetime.now()
	arg_date = parser.parse(str(dt)).strftime("%Y%m%d")
	#arg_date = '20171119'

	ck = MAIN_CK_OPEN(arg_date)

	if ck == True:
		print("當天有開市交易.")
	else:
		print("當天為休市狀態.")