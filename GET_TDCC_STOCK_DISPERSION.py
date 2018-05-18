# -*- coding: utf-8 -*-
"""
集保中心~集保戶股權分散表查詢，資料抓取

@author: Bryson Xue

@target_rul:
	查詢網頁 => http://www.tdcc.com.tw/smWeb/QryStockAjax.do

@Note:
	集保中心~集保戶股權分散表查詢
	每日資料結轉寫入資料庫
	抓取目標為上市、上櫃股票

@Ref:
	http://www.largitdata.com/course/53/

"""
import sqlite3
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from random import randint
from dateutil import parser
import datetime
from dateutil.relativedelta import relativedelta
import os.path
import sys
from selenium import webdriver


def GET_DATA(arg_stock, arg_date):
	global err_flag
	global file

	#檢查資料庫是否已有資料存在，若已有資料則略過，減少網站讀取
	rt_cnt = CHK_DATA_EXIST(arg_stock[0], arg_date)
	#print("rt_cnt=" + str(rt_cnt))
	if rt_cnt == 0:	#確認資料庫無資料，讀取網頁資料
		rt_flag = GET_WEB_DATA(arg_stock, arg_date)

		if rt_flag == False:
			err_flag = True
			print(str(arg_stock) + " 日期" + arg_date + "資料抓取失敗.")
			file.write(str(arg_stock) + " 日期" + arg_date + "資料抓取失敗.")

		DO_WAIT()	# 避免過度讀取網站，隨機間隔時間再讀取網頁

	else:
		err_flag = True
		print(str(arg_stock) + " 日期" + arg_date + "資料已存在，不再重新抓取.\n")
		#file.write(str(arg_stock) + " 日期" + arg_date + "資料已存在，不再重新抓取.\n")

def DO_WAIT():
	#隨機等待一段時間
	#sleep_sec = randint(30,120)
	sleep_sec = randint(5,10)
	print("間隔等待 " + str(sleep_sec) + " secs.")
	time.sleep(sleep_sec)

def CHK_DATA_EXIST(arg_sear_comp_id, arg_quo_date):
	global conn

	#檢查當天該股票是否已有資料
	strsql  = "select count(*) from STOCK_DISPERSION "
	strsql += "where "
	strsql += "QUO_DATE = '" + arg_quo_date + "' and "
	strsql += "SEAR_COMP_ID = '" + arg_sear_comp_id + "' "

	cursor = conn.execute(strsql)
	result = cursor.fetchone()
	rows_cnt = result[0]

	#關閉cursor
	cursor.close()

	return rows_cnt

def GET_DATE_LIST():
	global err_flag
	global file

	# 取得日期清單
	try:
		# 建立網頁讀取
		#driver = webdriver.Chrome()	# 需要看到執行過程可以用Chrome
		driver = webdriver.PhantomJS()
		driver.get("https://www.tdcc.com.tw/smWeb/QryStock.jsp")

		time.sleep(1)

		#html_source = driver.page_source
		#print(html_source)

		dt_obj = driver.find_element_by_id("scaDates")
		#print(dt_list.text)

		dt_list = []
		for element in dt_obj.find_elements_by_tag_name('option'):
			dt_list.append(element.text)

	except Exception as e:
		err_flag = True
		print("Err from GET_DATE_LIST(): \n$$$ 集保中心網站讀取錯誤，請確認網頁是否正常. $$$\n" + str(e) + "\n")
		file.write("Err from GET_DATE_LIST(): \n$$$ 集保中心網站讀取錯誤，請確認網頁是否正常. $$$\n" + str(e) + "\n")
		return []

	dt_list = sorted(dt_list, reverse=False)

	# 關閉瀏覽器視窗
	driver.quit()
	
	return dt_list

def GET_WEB_DATA(arg_stock, arg_date):
	global err_flag
	global file
	global conn
	rt_flag = True

	sear_comp_id = arg_stock[0]
	comp_id = arg_stock[0].replace(".TW","")
	comp_name = arg_stock[1]
	print("\n抓取" + sear_comp_id + " " + comp_name + " 日期" + arg_date + "集保戶股權分散資料.")

	headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
	s = requests.session()

	payload = {
		   "scaDates": arg_date,
		   "scaDate": arg_date,
		   "SqlMethod": "StockNo",
		   "StockNo": comp_id,
		   "radioStockNo": comp_id,
		   "StockName": "",
		   "REQ_OPR": "SELECT",
		   "clkStockNo": comp_id,
		   "clkStockName": ""
		   }

	# 拋送查詢條件到頁面，並取回查詢結果內容
	try:
		URL = 'http://www.tdcc.com.tw/smWeb/QryStockAjax.do'
		r = s.post(URL, data=payload, headers=headers)
		r.raise_for_status()
		r.encoding = 'big5'
		sp = BeautifulSoup(r.text, 'html.parser')
	except Exception as e:
		err_flag = True
		rt_flag = False
		print("Err from GET_WEB_DATA(): \n")
		print("$$$ 抓取" + sear_comp_id + " " + comp_name + " 日期 " + arg_date + " 集保戶股權分散資料失敗或無資料.$$$\n" + str(e) + "\n")
		file.write("Err from GET_WEB_DATA(): \n")
		file.write("$$$ 抓取" + sear_comp_id + " " + comp_name + " 日期 " + arg_date + " 集保戶股權分散資料失敗或無資料.$$$\n" + str(e) + "\n")
		return rt_flag

	#若有股票代號是查無資料的，則正常結束跳過此代號
	t = r.text
	f_posi = t.find("查無資料!")
	if f_posi > 0:
		rt_flag = False
		print("$$$ 抓取" + sear_comp_id + " " + comp_name + " 日期 " + arg_date + " 集保戶股權分散資料，無此代號資料.$$$\n")
		file.write("$$$ 抓取" + sear_comp_id + " " + comp_name + " 日期 " + arg_date + " 集保戶股權分散資料，無此代號資料.$$$\n")
		return rt_flag

	tb = sp.select('.mt')[1]
	all_data = []
	for tr in tb.select('tr'):
		rdata = [td.text.replace("\u3000","").replace(",","").strip() for td in tr.select('td')]
		all_data.append(rdata)

	ls_head = ['SEQ', 'LV_DESC', 'NUM_OF_PEOPLE', 'STOCK_SHARES', 'PER_CENT_RT']
	df = pd.DataFrame(all_data[1:len(all_data)-1], columns=ls_head)	#最後一筆合計資料不要
	#df = pd.DataFrame(all_data[1:], columns=ls_head)	#最後一筆合計資料不要

	#插入其他必要欄位
	df['QUO_DATE'] = arg_date
	df['SEAR_COMP_ID'] = sear_comp_id
	df['COMP_NAME'] = comp_name

	# 最後維護日期時間
	str_date = str(datetime.datetime.now())
	date_last_maint = parser.parse(str_date).strftime("%Y%m%d")
	time_last_maint = parser.parse(str_date).strftime("%H%M%S")
	prog_last_maint = "GET_TDCC_STOCK_DISPERSION"
	df['DATE_LAST_MAINT'] = date_last_maint
	df['TIME_LAST_MAINT'] = time_last_maint
	df['PROG_LAST_MAINT'] = prog_last_maint

	colorder = ('QUO_DATE', 'SEAR_COMP_ID', 'COMP_NAME', 'SEQ', 'LV_DESC', 'NUM_OF_PEOPLE', 'STOCK_SHARES', 'PER_CENT_RT', 'DATE_LAST_MAINT', 'TIME_LAST_MAINT', 'PROG_LAST_MAINT')
	df = df.reindex_axis(colorder, axis=1)	#調整datagrame欄位順序	http://nullege.com/codes/search/pandas.DataFrame.reindex_axis
	#df.to_sql(name='STOCK_DISPERSION', con=conn, index=False, if_exists='replace')
	#print(df)

	#資料寫入資料庫
	for index, row in df.iterrows():
		#print(row['QUO_DATE'] + " " + row['SEAR_COMP_ID'] + " " + row['COMP_NAME'] + " " + row['SEQ'])
		if len(row['NUM_OF_PEOPLE']) == 0:
			nop = "0"
		else:
			nop = row['NUM_OF_PEOPLE']

		strsql  = "insert into STOCK_DISPERSION ('QUO_DATE', 'SEAR_COMP_ID', 'COMP_NAME', 'SEQ', 'LV_DESC', 'NUM_OF_PEOPLE', 'STOCK_SHARES', 'PER_CENT_RT', 'DATE_LAST_MAINT', 'TIME_LAST_MAINT', 'PROG_LAST_MAINT') values ("
		strsql += "'" + row['QUO_DATE'] + "', "
		strsql += "'" + row['SEAR_COMP_ID'] + "', "
		strsql += "'" + row['COMP_NAME'] + "', "
		strsql += row['SEQ'] + ", "
		strsql += "'" + row['LV_DESC'] + "', "
		strsql += nop + ", "
		strsql += row['STOCK_SHARES'] + ", "
		strsql += row['PER_CENT_RT'] + ", "
		strsql += "'" + row['DATE_LAST_MAINT'] + "', "
		strsql += "'" + row['TIME_LAST_MAINT'] + "', "
		strsql += "'" + row['PROG_LAST_MAINT'] + "' "
		strsql += ")"

		try :
			#print(strsql)
			conn.execute(strsql)
			conn.commit()
		except sqlite3.Error as er:
			err_flag = True
			rt_flag = False
			conn.execute("rollback")
			print("insert STOCK_DISPERSION er=" + er.args[0])
			print(strsql)
			file.write("insert STOCK_DISPERSION er=" + er.args[0] + "\n")
			file.write(strsql + "\n")

	return rt_flag

def MAIN_GET_TDCC_STOCK_DISPERSION(arg_mode='A'):
	global err_flag
	global file
	global conn
	err_flag = False

	print("Executing " + os.path.basename(__file__) + "...")

	#LOG檔
	str_date = str(datetime.datetime.now())
	str_date = parser.parse(str_date).strftime("%Y%m%d")
	name = "GET_TDCC_STOCK_DISPERSION_LOG_" + str_date + ".txt"
	file = open(name, "a", encoding="UTF-8")

	print_dt = str(str_date) + (' ' * 22)
	print("##############################################")
	print("##           集保戶股權分散表查詢           ##")
	print("##             (上市、上櫃股票)             ##")
	print("##                                          ##")
	print("##  datetime: " + print_dt +               "##")
	print("##############################################")

	tStart = time.time()#計時開始
	file.write("\n\n\n*** LOG datetime  " + str(datetime.datetime.now()) + " ***\n")
	file.write("Executing " + os.path.basename(__file__) + "...\n")

	#依據所選模式，決定抓取方式
	#mode A:抓取最近一周資料
	#mode B:抓取日期清單所有資料
	try:
		#run_mode = sys.argv[1]
		run_mode = arg_mode
		run_mode = run_mode.upper()
	except Exception as e:
		run_mode = "A"

	print("you choose mode " + run_mode)
	if run_mode == "A":
		print("選取模式 A: 抓取最近一周資料...\n")
		file.write("選取模式 A: 抓取最近一周資料...\n")
	elif run_mode == "B":
		print("選取模式 B: 抓取日期清單所有資料...\n")
		file.write("選取模式 B: 抓取日期清單所有資料...\n")
	else:
		print("模式錯誤，結束程式...\n")
		file.write("模式錯誤，結束程式...\n")
		sys.exit("模式錯誤，結束程式...\n")

	#建立資料庫連線
	conn = sqlite3.connect("market_price.sqlite")

	#抓取網站日期清單
	dt_list = GET_DATE_LIST()
	#print(dt_list)
	#dt_list = ['20170616', '20170623', '20170630', '20170707']	#for test 手動用

	#依據所選模式抓取資料
	if len(dt_list) > 0:
		#讀取上市櫃股票清單
		strsql  = "select SEAR_COMP_ID,COMP_NAME, STOCK_TYPE from STOCK_COMP_LIST "
		#strsql += "where SEAR_COMP_ID = '2034.TW' "
		strsql += "order by STOCK_TYPE, SEAR_COMP_ID "
		#strsql += "limit 1"

		cursor = conn.execute(strsql)
		result = cursor.fetchall()

		#關閉cursor
		cursor.close()

		if len(result) > 0:
			for stock in result:
				#print(stock)
				if run_mode == "A":
					str_date = str(dt_list[-1:][0])
					GET_DATA(stock, str_date)
				else:	#模式B:抓取日期清單所有股票資料
					for dt in dt_list:
						str_date = str(dt)
						GET_DATA(stock, str_date)
		else:
			err_flag = True
			print("$$$ 未取得公司清單資料. $$$")
			file.write("$$$ 未取得公司清單資料. $$$")

	else:
		err_flag = True
		print("$$$ 未取得日期清單資料，請確認來源網頁是否正常 $$$")
		file.write("$$$ 未取得日期清單資料，請確認來源網頁是否正常 $$$")


	tEnd = time.time()#計時結束
	file.write("\n\n\n結轉耗時 %f sec\n" % (tEnd - tStart)) #會自動做進位
	file.write("*** End LOG ***\n")

	# Close File
	file.close()

	#關閉資料庫連線
	conn.close()

	#若執行過程無錯誤，執行結束後刪除log檔案
	if err_flag == False:
		os.remove(name)

	print("\n\n集保中心~集保戶股權分散表查詢，資料抓取結束...\n\n\n")

if __name__ == '__main__':
	MAIN_GET_TDCC_STOCK_DISPERSION()