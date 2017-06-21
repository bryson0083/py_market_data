# -*- coding: utf-8 -*-
"""
櫃買中心外資及陸資投資持股統計~每日收盤資料結轉CSV檔

@author: Bryson Xue
@target_rul: 
	查詢網頁 => http://mops.twse.com.tw/server-java/t13sa150_otc?step=0
@Note: 
	櫃買中心外資及陸資投資持股統計
	每日收盤資料結轉CSV檔
	限定抓取資料，分類項目為"全部"
"""
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

def DO_WAIT():
	#隨機等待一段時間
	#sleep_sec = randint(30,120)
	sleep_sec = randint(5,10)
	print("間隔等待 " + str(sleep_sec) + " secs.\n")
	time.sleep(sleep_sec)

def GET_DATA(arg_date):
	global err_flag
	rt_flag = True

	file_name = "./daily_fr_holding_shares_sq/" + str(arg_date) + ".csv"
	is_existed = os.path.exists(file_name)

	yyyy = str(arg_date[0:4])
	mm = str(arg_date[4:6])
	dd = str(arg_date[6:8])

	# 檢查若已有檔案，則不再下載
	if is_existed == False:
		headers = {'User-Agent':'User-Agent:Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36'}
		session = requests.session()

		# 拋送查詢條件到頁面，並取回查詢結果內容
		URL = 'http://mops.twse.com.tw/server-java/t13sa150_otc'
		payload = {
				  "years": yyyy,
				  "months": mm,
				  "days": dd,
				  "bcode": "",
				  "step": "2"
				  }

		r = requests.post(URL, data=payload, headers=headers)
		r.encoding = "big5"
		sp = BeautifulSoup(r.text, 'html.parser')
		#print(sp)

		try:
			#判斷查詢日期當天是否有資料
			#若當天無資料，則還是產生一個檔案，並回到主程式
			tag_h3 = sp.find('h3')
			h3_text = tag_h3.findAll(lambda tag: tag.name=='font')
			msg_posi = str(h3_text).find("查無所需資料")

			if msg_posi >= 0:
				# 若當天無資料，還是產生一個檔案
				file2 = open(file_name, "a", encoding="big5")
				file2.write("當天無交易資料")
				file2.close()
				return rt_flag

		except Exception as e:
			#抓不到tag資料，表示網頁有抓到資料
			pass

		try:
			#當天有交易資料，讀取網頁table資料
			table = sp.find('table', {"border":"1"})
			rows = table.findAll(lambda tag: tag.name=='tr')

			i = 0
			rhead = []
			all_data = []
			for row in rows:
				#print(row)	
				if i==1:	#讀取表格抬頭
					rhead = [row_th.text for row_th in row.select('th')]

				if i > 1:	# 讀取表格資料
					rdata = [row_td.text.strip() for row_td in row.select('td')]
					rdata = [x for x in rdata if x != []]
					#print(rdata)
					all_data.append(rdata)

				i += 1

			df = pd.DataFrame(data=all_data, columns=rhead)
			#print(df)

			#DataFrame資料寫入csv檔案保存起來
			df.to_csv(file_name, index=False)

			DO_WAIT()	# 避免過度讀取網站，隨機間隔時間再讀取網頁

		except Exception as e:
			err_flag = True
			rt_flag = False
			print("$$$ 判斷網頁有資料，抓取出現異常，確認是否原始網頁結構已修改. $$$\n")
			print(str(e))
			file.write("$$$ 判斷網頁有資料，抓取出現異常，確認是否原始網頁結構已修改. $$$\n")
			file.write(str(e))
	else:
		print(str(arg_date) + "資料檔已存在，不再更新資料.\n\n")
		file.write(str(arg_date) + "資料檔已存在，不再更新資料.\n\n")

	return rt_flag

############################################################################
# Main                                                                     #
############################################################################
print("Executing GET_DAILY_FR_HOLDING_SHARES_SQ ...\n\n")
global err_flag
err_flag = False

#LOG檔
str_date = str(datetime.datetime.now())
str_date = parser.parse(str_date).strftime("%Y%m%d")
name = "GET_DAILY_FR_HOLDING_SHARES_SQ_LOG_" + str_date + ".txt"
file = open(name, "a", encoding="UTF-8")

print_dt = str(str_date) + (' ' * 22)
print("##############################################")
print("##      櫃買中心外資及陸資投資持股統計      ##")
print("##              分類項目:全部               ##")
print("##                                          ##")
print("##  datetime: " + print_dt +               "##")
print("##############################################")

#依據所選模式，決定起訖日期
#mode A:跑當天日期到往前推7天
#mode B:跑昨天日期到往前推7天
try:
	run_mode = sys.argv[1]
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

#for需要時手動設定日期區間用(資料最早日期20040801起)
#start_date = "20170101"
#end_date = "20170615"

print("結轉日期" + start_date + "~" + end_date)

tStart = time.time()#計時開始
file.write("\n\n\n*** LOG datetime  " + str(datetime.datetime.now()) + " ***\n")
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
	print("下載 " + str_date + " OTC外資及陸資投資持股統計.\n")
	rt = GET_DATA(str_date)

	if rt == True:
		cnt += 1
		print(str_date + " 抓取程序正常結束.\n")
	
	dt = datetime.datetime.strptime(str_date, date_fmt).date()
	dt = dt + relativedelta(days=1)	
	i += 1
	
	# 累計抓滿有收盤資料90天就強制跳出迴圈
	#if cnt == 90:
	#	print("抓滿90天，強制結束.")
	#	file.write("抓滿90天，強制結束.\n")
	#	break

tEnd = time.time()#計時結束
file.write("\n\n\n結轉耗時 %f sec\n" % (tEnd - tStart)) #會自動做進位
file.write("*** End LOG ***\n")

# Close File
file.close()

#若執行過程無錯誤，執行結束後刪除log檔案
if err_flag == False:
	os.remove(name)

print("End of prog...")