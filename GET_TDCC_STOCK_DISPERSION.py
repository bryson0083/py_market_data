# -*- coding: utf-8 -*-
"""
集保中心~集保戶股權分散表查詢，資料抓取

@author: Bryson Xue

@target_rul:
	查詢網頁 => https://www.tdcc.com.tw/smWeb/QryStock.jsp

@Note:
	集保中心~集保戶股權分散表查詢
	每日資料結轉寫入資料庫
	抓取目標為上市、上櫃股票

@Ref:
	http://www.largitdata.com/course/53/
	https://stackoverflow.com/questions/2052390/manually-raising-throwing-an-exception-in-python

['20170407', '20170414', '20170421', '20170428', '20170505', '20170512', '20170519', '20170526', '20170603', '20170609', '20170616', '20170623', '20170630', '20170707', 
 '20170714', '20170721', '20170728', '20170804', '20170811', '20170818', '20170825', '20170901', '20170908', '20170915', '20170922', '20170930', '20171006', '20171013', 
 '20171020', '20171027', '20171103', '20171110', '20171117', '20171124', '20171201', '20171208', '20171215', '20171222', '20171229', '20180105', '20180112', '20180119', 
 '20180126', '20180202', '20180209', '20180214', '20180223', '20180302', '20180309', '20180316', '20180323', '20180331', '20180403', '20180413', '20180420', '20180427', 
 '20180504', '20180511', '20180518', '20180525', '20180601', '20180608', '20180615', '20180622', '20180629']

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

	err_cnt = 0
	# 取得日期清單
	while True:
		try:
			print("抓取日期清單....")
			# 建立網頁讀取
			#driver = webdriver.Chrome()	# 需要看到執行過程可以用Chrome
			driver = webdriver.PhantomJS()
			driver.get("https://www.tdcc.com.tw/smWeb/QryStock.jsp")
			time.sleep(1)

			dt_obj = driver.find_element_by_id("scaDates")
			dt_list = []
			for element in dt_obj.find_elements_by_tag_name('option'):
				dt_list.append(element.text)

			# 關閉瀏覽器視窗
			driver.quit()

			if len(dt_list) == 0:
				raise Exception('Err: This is an empty list.')
			else:
				dt_list = sorted(dt_list, reverse=False)
				print("成功抓取日期清單資料.")
				break

		except Exception as e:
			print("讀取日期清單錯誤，等待重新執行...")

			if err_cnt == 2:
				err_flag = True
				print("錯誤次數已達上限，結束本次抓取")
				print("Err from GET_DATE_LIST(): \n$$$ 集保中心網站讀取錯誤，請確認網頁是否正常. $$$\n" + str(e) + "\n")
				file.write("Err from GET_DATE_LIST(): \n$$$ 集保中心網站讀取錯誤，請確認網頁是否正常. $$$\n" + str(e) + "\n")
				raise Exception('錯誤次數已達上限，結束本次抓取')
			else:
				print(e.args)
				print("Err cnt =>" + str(err_cnt))
				err_cnt += 1
				DO_WAIT()	#等待一段時間重新執行

	return dt_list

def GET_DATA(arg_stock, arg_date):
	global err_flag
	global file

	#檢查資料庫是否已有資料存在，若已有資料則略過，減少網站讀取
	rt_cnt = CHK_DATA_EXIST(arg_stock[0], arg_date)
	#print("rt_cnt=" + str(rt_cnt))
	if rt_cnt == 0:	#確認資料庫無資料，讀取網頁資料
		retry_cnt = 0
		while True:
			try:
				DO_WAIT()	# 避免過度讀取網站，隨機間隔時間再讀取網頁
				rt_flag = GET_WEB_DATA(arg_stock, arg_date)
				#print("retry_cnt=" + str(retry_cnt) + "   rt_flag=" + str(rt_flag))
				
				if rt_flag == False:
					raise Exception(str(arg_stock) + " 日期" + arg_date + "資料抓取失敗，等待一段時間後，重新抓取.")
				else:
					print(str(arg_stock) + " 日期" + arg_date + "資料，成功寫入資料庫.")
					break

			except Exception as e:
				if retry_cnt == 2:
					err_flag = True
					print(str(arg_stock) + " 日期" + arg_date + "資料抓取失敗.\n" + str(e.args) + "\n")
					file.write(str(arg_stock) + " 日期" + arg_date + "資料抓取失敗.\n" + str(e.args) + "\n")
					return
				else:
					print("retry_cnt=" + str(retry_cnt))
					#file.write(str(arg_stock) + " 日期" + arg_date + "資料抓取失敗.\n" + str(e.args) + "\n")
					retry_cnt += 1
	else:
		err_flag = True
		#print(str(arg_stock) + " 日期" + arg_date + "資料已存在，不再重新抓取.\n")
		#file.write(str(arg_stock) + " 日期" + arg_date + "資料已存在，不再重新抓取.\n")

def GET_WEB_DATA(arg_stock, arg_date):
	global err_flag
	global file
	global conn
	rt_flag = False

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
		print("Err from GET_WEB_DATA(): \n")
		print("$$$ 抓取" + sear_comp_id + " " + comp_name + " 日期 " + arg_date + " 集保戶股權分散資料失敗或無資料.$$$\n" + str(e) + "\n")
		file.write("Err from GET_WEB_DATA(): \n")
		file.write("$$$ 抓取" + sear_comp_id + " " + comp_name + " 日期 " + arg_date + " 集保戶股權分散資料失敗或無資料.$$$\n" + str(e) + "\n")
		raise Exception(str(arg_stock) + " 日期" + arg_date + "資料抓取失敗，等待一段時間後，重新抓取. GET_WEB_DATA Sec.1")

	#若有股票代號是無此資料的，則正常結束跳過此代號
	t = r.text
	#print(t)
	f_posi = t.find("無此資料")
	#print("f_posi=" + str(f_posi))
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
			rt_flag = True
		except sqlite3.Error as er:
			err_flag = True
			conn.execute("rollback")
			print("insert STOCK_DISPERSION er=" + er.args[0])
			print(strsql)
			file.write("insert STOCK_DISPERSION er=" + er.args[0] + "\n")
			file.write(strsql + "\n")
			raise Exception(str(arg_stock) + " 日期" + arg_date + "資料抓取失敗，等待一段時間後，重新抓取. GET_WEB_DATA Sec.2")

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
	try:
		dt_list = GET_DATE_LIST()
		#print(dt_list)
	except Exception as e:
		print("抓取日期清單失敗...")
		print(e.args)

	#dt_list = ['20180323']	#for test 手動用

	if run_mode == "A":
		dt_list = dt_list[-1:]

	#print(dt_list)

	if err_flag == False:
		for dt in dt_list:
			str_date = str(dt)

			#讀取上市櫃股票清單
			strsql  = "select STOCK_COMP_LIST.SEAR_COMP_ID, STOCK_COMP_LIST.COMP_NAME, STOCK_COMP_LIST.STOCK_TYPE from STOCK_COMP_LIST "
			strsql += "LEFT JOIN TDCC_IGNR on TDCC_IGNR.SEAR_COMP_ID = STOCK_COMP_LIST.SEAR_COMP_ID "
			strsql += "where "
			#strsql += "SEAR_COMP_ID = '2002A.TW' and "
			strsql += "STOCK_COMP_LIST.IPO_DATE <= '" + str_date + "' and "
			strsql += "TDCC_IGNR.SEAR_COMP_ID is NULL "
			strsql += "order by STOCK_COMP_LIST.STOCK_TYPE, STOCK_COMP_LIST.SEAR_COMP_ID "
			#strsql += "limit 1"

			cursor = conn.execute(strsql)
			result = cursor.fetchall()

			#關閉cursor
			cursor.close()

			if len(result) > 0:
				for stock in result:
					GET_DATA(stock, str_date)
			else:
				err_flag = True
				print("$$$ 未取得公司清單資料. $$$")
				file.write("$$$ 未取得公司清單資料. $$$")

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