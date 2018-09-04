# -*- coding: utf-8 -*-
"""
集保中心~集保戶股權分散表查詢，資料抓取 V2

@Note:
	查詢網頁
	https://www.tdcc.com.tw/smWeb/QryStock.jsp

	股權分散表資料來源
	https://smart.tdcc.com.tw/opendata/getOD.ashx?id=1-5

	下載檔案在chrome headless模式下，目前測試無法運作
	僅能在有開啟瀏覽器的狀態下，下載成功。

@Ref:
	https://www.dataquest.io/blog/python-pandas-databases/
	https://chrisalbon.com/python/data_wrangling/pandas_dataframe_importing_csv/
	https://stackoverflow.com/questions/15017072/pandas-read-csv-and-filter-columns-with-usecols
	http://hant.ask.helplib.com/python/post_4799128


"""
import sys
import fnmatch
import os
import re
import csv
import datetime
import time
import pandas as pd
import sqlite3
from dateutil import parser
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

import timeit

def lv_desc_dic(param):
	code_dic = {
		'1': '1-999',
		'2': '1000-5000',
		'3': '5001-10000',
		'4': '10001-15000',
		'5': '15001-20000',
		'6': '20001-30000',
		'7': '30001-40000',
		'8': '40001-50000',
		'9': '50001-100000',
		'10': '100001-200000',
		'11': '200001-400000',
		'12': '400001-600000',
		'13': '600001-800000',
		'14': '800001-1000000',
		'15': '1000001以上',
		'16': '差異數調整（說明4）',
		'17': '合計'
	}
	lv_desc = code_dic.get(param, '')
	return lv_desc

def rd_comp_list():
	global conn
	strsql = "select SEAR_COMP_ID, COMP_NAME from STOCK_COMP_LIST  order by SEAR_COMP_ID limit 100"
	df = pd.read_sql_query(strsql, conn)
	#print(df)
	return df

def get_comp_name(arg_sear_comp_id):
	if not co_df[co_df.SEAR_COMP_ID == arg_sear_comp_id].empty:
		comp_name = co_df[co_df.SEAR_COMP_ID == arg_sear_comp_id].iloc[0]['COMP_NAME']
	else:
		comp_name = ' '

	return comp_name

def download_tdcc_file():
	print('開始下載資料檔案...')
	cwd = os.getcwd()
	file_path = cwd + r"\tdcc_data"
	#print(file_path)

	chrome_options = Options()
	chrome_options.add_experimental_option("prefs", {
		"download.default_directory": file_path,
		"download.prompt_for_download": False,
		"download.directory_upgrade": True,
		"safebrowsing.enabled": True
	})

	#chrome_options.add_argument('--headless')
	#chrome_options.add_argument('--disable-gpu')
	driver = webdriver.Chrome(chrome_options=chrome_options)

	driver.get("https://smart.tdcc.com.tw/opendata/getOD.ashx?id=1-5")
	time.sleep(10)
	driver.quit()

def store_db(arg_file_name):
	global co_df, err_flag

	rt_flag = True
	ls_head = ['QUO_DATE', 'SEAR_COMP_ID', 'SEQ',
			   'NUM_OF_PEOPLE', 'STOCK_SHARES', 'PER_CENT_RT']
	df = pd.read_csv(arg_file_name, header=0, names=ls_head)

	#print(data_dt)
	#print(df.head(2))

	df = df.fillna(0)
	df['SEAR_COMP_ID'] = df['SEAR_COMP_ID'] + ".TW"

	co_df = rd_comp_list()  # 讀取股票資料清單
	df['COMP_NAME'] = df.apply(lambda row: get_comp_name(row['SEAR_COMP_ID']), axis=1)
	df['LV_DESC'] = df.apply(lambda row: lv_desc_dic(str(row['SEQ'])), axis=1)

	# 最後維護日期時間
	str_date = str(datetime.datetime.now())
	df['DATE_LAST_MAINT'] = parser.parse(str_date).strftime("%Y%m%d")
	df['TIME_LAST_MAINT'] = parser.parse(str_date).strftime("%H%M%S")
	df['PROG_LAST_MAINT'] = "GET_TDCC_STOCK_DISPERSION"

	colorder = ('QUO_DATE', 'SEAR_COMP_ID', 'COMP_NAME', 'SEQ', 'NUM_OF_PEOPLE', 'STOCK_SHARES',
	            'PER_CENT_RT', 'LV_DESC', 'DATE_LAST_MAINT', 'TIME_LAST_MAINT', 'PROG_LAST_MAINT')
	# 調整datagrame欄位順序	http://nullege.com/codes/search/pandas.DataFrame.reindex_axis
	df = df.reindex(colorder, axis=1)
	#print(df.head(20))

	try:
		df.to_sql("STOCK_DISPERSION", conn, if_exists="replace", index=False)
	except Exception as e:
		rt_flag = False
		err_flag = True
		print(arg_file_name + '資料寫入db異常.')
		print(str(e.args))
		log_file.write(arg_file_name + '資料寫入db異常.')
		log_file.write(str(e.args))

	return rt_flag

def trans_uptodate_tdcc():
	global err_flag
	print('結轉最新資料到資料庫...')

	cwd = os.getcwd()
	file_name = cwd + r"\tdcc_data\TDCC_OD_1-5.csv"
	#print(file_name)
	is_existed = os.path.exists(file_name)
	#print(is_existed)

	if is_existed:
		ls_head = ['QUO_DATE', 'SEAR_COMP_ID', 'SEQ',
				   'NUM_OF_PEOPLE', 'STOCK_SHARES', 'PER_CENT_RT']
		df = pd.read_csv(file_name, header=0, names=ls_head)
		data_dt = str(df.loc[0]['QUO_DATE'])	#讀取csv資料日期，作為rename檔名

		#資料寫入資料庫
		rt_flag = store_db(file_name)

		if rt_flag:
			#檔案rename保存
			new_file_name = cwd + "\\tdcc_data\\" + data_dt + ".csv"
			is_existed2 = os.path.exists(new_file_name)

			if is_existed2:
				os.remove(new_file_name)

			os.rename(file_name, new_file_name)
	else:
		err_flag = True
		print("無TDCC_OD_1-5.csv資料檔案存在.")
		log_file.write("無TDCC_OD_1-5.csv資料檔案存在.")


def find_file(arg_start_dt, arg_end_dt):
	pattern = '*.csv'
	cwd = os.getcwd()
	path = cwd + r"\tdcc_data"
	result = []
	for root, dirs, files in os.walk(path):
		for name in files:
			if fnmatch.fnmatch(name, pattern):
				match = re.search(r'\d+', name)
				tmp_dt = str(match.group(0))
				if (tmp_dt >= arg_start_dt) and (tmp_dt <= arg_end_dt):
					result.append(os.path.join(root, name))

	return result

def trans_dt_range_tdcc(arg_start_dt, arg_end_dt):
	file_result = find_file(arg_start_dt, arg_end_dt)
	#print(file_result)

	for file_name in file_result:
		rt_flag = store_db(file_name)

		if rt_flag:
			print(file_name + "結轉完畢.")
		else:
			print(file_name + "結轉失敗.")

def MAIN_GET_TDCC():
	global conn
	global err_flag
	global log_file

	err_flag = False
	print("Executing " + os.path.basename(__file__) + "...")

	#LOG檔
	str_date = str(datetime.datetime.now())
	str_date = parser.parse(str_date).strftime("%Y%m%d")
	name = "GET_TDCC_STOCK_DISPERSION_LOG_" + str_date + ".txt"
	log_file = open(name, "a", encoding="UTF-8")

	print_dt = str(str_date) + (' ' * 22)
	print("##############################################")
	print("##           集保戶股權分散表查詢           ##")
	print("##             (上市、上櫃股票)             ##")
	print("##                                          ##")
	print("##  datetime: " + print_dt + "##")
	print("##############################################")

	tStart = time.time()  # 計時開始
	log_file.write("\n\n\n*** LOG datetime  " +
	           str(datetime.datetime.now()) + " ***\n")
	log_file.write("Executing " + os.path.basename(__file__) + "...\n")

	#建立資料庫連線
	conn = sqlite3.connect('market_price.sqlite')

	#依據所選模式，決定結轉方式
	if len(sys.argv) > 1:
		task_mode = sys.argv[1]
	else:
		task_mode = input("task mode(A:下載並結轉最新資料, B:輸入起訖日期區間結轉資料):")
		
	task_mode = task_mode.upper()
	print("task mode: " + task_mode)

	if task_mode == "A":
		#下載資料檔
		download_tdcc_file()

		#資料結轉資料庫
		trans_uptodate_tdcc()
	else:
		start_dt = input("起始日期(YYYYMMDD):")
		end_dt = input("截止日期(YYYYMMDD):")
		trans_dt_range_tdcc(start_dt, end_dt)

	#關閉資料庫連線
	conn.close()

	tEnd = time.time()  # 計時結束
	log_file.write("\n\n\n結轉耗時 %f sec\n" % (tEnd - tStart))  # 會自動做進位
	log_file.write("*** End LOG ***\n")

	# Close File
	log_file.close()

	#若執行過程無錯誤，執行結束後刪除log檔案
	if err_flag == False:
		os.remove(name)

	print("\n\n集保中心~集保戶股權分散表查詢，資料抓取結束...\n\n\n")

if __name__ == "__main__":
	MAIN_GET_TDCC()
