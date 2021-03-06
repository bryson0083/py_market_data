# -*- coding: utf-8 -*-
"""
櫃買中心上櫃股票融資融券餘額~每日收盤CSV檔下載

@author: Bryson Xue

@target_rul: 
	查詢網頁 => http://www.tpex.org.tw/web/stock/margin_trading/margin_balance/margin_bal.php?l=zh-tw
	CSV連結  => http://www.tpex.org.tw/web/stock/margin_trading/margin_balance/margin_bal_download.php?l=zh-tw&d=106/06/07&s=0,asc,1

@Note: 
	櫃買中心股票融資融券餘額
	下載網頁CSV檔
	
"""
import requests
import time
from random import randint
from dateutil import parser
import datetime
from dateutil.relativedelta import relativedelta
import os.path
import sys

def DO_WAIT():
	#隨機等待一段時間
	#sleep_sec = randint(30,120)
	sleep_sec = randint(5,10)
	print("間隔等待 " + str(sleep_sec) + " secs.")
	time.sleep(sleep_sec)

def GET_CSV(sear_date):
	global err_flag
	global file
	rt_flag = False

	file_name = "./daily_mtss_data_sq/" + sear_date + ".csv"
	is_existed = os.path.exists(file_name)

	# 網頁查詢日期參數，轉民國年日期
	arg_date = parser.parse(str(sear_date)).strftime("%Y/%m/%d")
	arg_date = str(int(arg_date[0:4]) - 1911) + arg_date[4:]
	str_url = "http://www.tpex.org.tw/web/stock/margin_trading/margin_balance/margin_bal_download.php?l=zh-tw&d=" + arg_date + "&s=0,asc,1"

	# 檢查若已有檔案，則不再下載
	if is_existed == False:
		headers = {'User-Agent':'User-Agent:Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36'}
		session = requests.session()
		
		try:
			# 讀取查詢頁面
			r = session.get(str_url, headers=headers)
			r.encoding = "utf-8"

			# 取得回傳資料的長度
			len_r = len(r.text)
			#print(len_r)

			if len_r == 959:	# 表示當天無收盤資料
				# 若當天無資料，還是產生一個檔案
				print("當天無交易資料\n")
				file2 = open(file_name, "a", encoding="big5")
				file2.write("當天無交易資料")
				file2.close()

			else:
				rt_flag = True
				with open(file_name, 'wb') as f:
					for chunk in r.iter_content(chunk_size=1024): 
						if chunk: # filter out keep-alive new chunks
							f.write(chunk)
				#close file
				f.close()

				DO_WAIT()	# 避免過度讀取網站，隨機間隔時間再讀取網頁


		except Exception as e:
			file.write("$$$ Err:" + sear_date + " 上櫃股票融資融券餘額資料抓取異常，請檢查是否網路有問題或原網頁已更改. $$$")
			err_flag = True
			rt_flag = False

	else:
		rt_flag = False
		print(sear_date + "資料檔已存在，不再更新資料.\n")

	return rt_flag

def MAIN_GET_DAILY_MTSS_SQ(arg_mode='B'):
	global err_flag
	global file
	err_flag = False

	print("Executing " + os.path.basename(__file__) + "...")

	#LOG檔
	str_date = str(datetime.datetime.now())
	str_date = parser.parse(str_date).strftime("%Y%m%d")
	name = "GET_DAILY_MTSS_SQ_LOG_" + str_date + ".txt"
	file = open(name, "a", encoding="UTF-8")

	print_dt = str(str_date) + (' ' * 22)
	print("##############################################")
	print("##        櫃買中心上櫃股票融資融券餘額      ##")
	print("##                                          ##")
	print("##                                          ##")
	print("##  datetime: " + print_dt +               "##")
	print("##############################################")

	#依據所選模式，決定起訖日期
	#mode A:跑當天日期到往前推7天
	#mode B:跑昨天日期到往前推7天
	try:
		#run_mode = sys.argv[1]
		run_mode = arg_mode
		run_mode = run_mode.upper()
	except Exception as e:
		run_mode = "A"

	print("you choose mode " + run_mode)

	dt = datetime.datetime.now()
	if run_mode == "A":
		print("A: 抓取到當天資料...\n")
		file.write("A: 抓取到當天資料...\n")
		start_date = dt + datetime.timedelta(days=-7)
		start_date = parser.parse(str(start_date)).strftime("%Y%m%d")
		end_date = parser.parse(str(dt)).strftime("%Y%m%d")
	elif run_mode == "B":
		print("B: 抓取到昨天資料...\n")
		file.write("B: 抓取到昨天資料...\n")
		start_date = dt + datetime.timedelta(days=-8)
		start_date = parser.parse(str(start_date)).strftime("%Y%m%d")
		end_date = dt + datetime.timedelta(days=-1)
		end_date = parser.parse(str(end_date)).strftime("%Y%m%d")
	else:
		print("模式錯誤，結束程式...\n")
		file.write("模式錯誤，結束程式...\n")
		sys.exit("模式錯誤，結束程式...\n")

	#for需要時手動設定日期區間用(資料最早日期20070101起)
	#start_date = "20170101"
	#end_date = "20170608"

	print("結轉日期" + start_date + "~" + end_date + "\n\n")

	tStart = time.time()#計時開始
	file.write("\n\n\n*** LOG datetime  " + str(datetime.datetime.now()) + " ***\n")
	file.write("Executing " + os.path.basename(__file__) + "...\n")
	file.write("結轉日期" + start_date + "~" + end_date + "\n")

	date_fmt = "%Y%m%d"
	a = datetime.datetime.strptime(start_date, date_fmt)
	b = datetime.datetime.strptime(end_date, date_fmt)
	delta = b - a
	int_diff_date = delta.days
	#print("days=" + str(int_diff_date) + "\n")

	i = 1
	cnt = 1
	dt = ""
	while i <= (int_diff_date+1):
		#print(str(i) + "\n")
		if i==1:
			str_date = start_date
		else:
			str_date = parser.parse(str(dt)).strftime("%Y%m%d")

		#print(str_date + "\n")
		print("下載 " + str_date + " 上櫃股票融資融券餘額資料.")
		rt = GET_CSV(str_date)
		
		if rt == True:
			cnt += 1
			print(str_date + "資料下載成功.\n")
		
		dt = datetime.datetime.strptime(str_date, date_fmt).date()
		dt = dt + relativedelta(days=1)	
		i += 1
		
		# 累計抓滿有收盤資料90天就強制跳出迴圈
		#if cnt == 90:
		#	print("抓滿90天，強制結束.")
		#	file.write("抓滿90天，強制結束.\n")
		#	break

	tEnd = time.time()#計時結束
	file.write ("\n\n\n結轉耗時 %f sec\n" % (tEnd - tStart)) #會自動做進位
	file.write("*** End LOG ***\n")

	# Close File
	file.close()

	#若執行過程無錯誤，執行結束後刪除log檔案
	if err_flag == False:
		os.remove(name)

	print("櫃買中心，每日信用交易統計、融資融券彙總，每日收盤CSV檔下載結束...\n\n\n")

if __name__ == '__main__':
	MAIN_GET_DAILY_MTSS_SQ()	