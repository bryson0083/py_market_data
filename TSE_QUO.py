# -*- coding: utf-8 -*-
"""
Created on Tue Dec 13 10:13:09 2016

@author: bryson0083
"""
import requests
import time
from random import randint
from dateutil import parser
import datetime
from dateutil.relativedelta import relativedelta
import os.path

def DailyQuoCSV(sear_date):
	flag = "N"
	file_name = "./tse_quo_data/" + sear_date.replace("/","") + ".csv"
	is_existed = os.path.exists(file_name)
	
	# 檢查若已有檔案，則不再下載
	if is_existed == False:
		headers = {'User-Agent':'User-Agent:Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36'}
		session = requests.session()
		
		# 讀取查詢頁面
		r = session.get("http://www.tse.com.tw/ch/trading/exchange/MI_INDEX/MI_INDEX.php", headers=headers)
		
		sleep_sec = randint(5,9)
		print("waiting " + str(sleep_sec) + " secs.")
		#time.sleep(5)
		time.sleep(sleep_sec)
		
		# 拋送查詢條件到頁面，並取回查詢結果內容
		URL = 'http://www.tse.com.tw/ch/trading/exchange/MI_INDEX/MI_INDEX.php'
		payload = {
		           "download": "csv",
		           "qdate": sear_date,
		           "selectType": "ALLBUT0999"
		           }
		
		r = requests.post(URL, data=payload, headers=headers)
		
		#為了取得回傳網頁的部分訊息，先轉碼為big5再轉回utf8
		r.encoding = "big5"
		t = r.text[0:20]
		print(t)
		
		f_yn = t.find("查無資料")
		data_yn = "Y"
		if f_yn > 0:	# 表示當天無收盤資料
			data_yn = "N"
			
			# 若當天無資料，還是產生一個檔案
			file2 = open(file_name, "a", encoding="big5")
			file2.write("當天無收盤資料")
			file2.close()
		
		#print("data_yn=" + data_yn + "\n")
		r.encoding = "utf-8"
		if data_yn == "Y":
			flag = "Y"
			with open(file_name, 'wb') as f:
				for chunk in r.iter_content(chunk_size=1024): 
					if chunk: # filter out keep-alive new chunks
						f.write(chunk)
			#close file
			f.close()
			
			sleep_sec = randint(5,9)
			print("waiting " + str(sleep_sec) + " secs.")
			time.sleep(sleep_sec)
	else:
		print(sear_date + "資料檔已存在，不再更新資料.")
	return flag

# 起訖日期
start_date = "2004/02/11"
end_date = "2016/12/31"

str_date = str(datetime.datetime.now())
str_date = parser.parse(str_date).strftime("%Y%m%d")
name = "TSE_QUO_LOG_" + str_date + ".txt"
file = open(name, "a", encoding="UTF-8")

tStart = time.time()#計時開始
file.write("\n\n\n*** LOG datetime  " + str(datetime.datetime.now()) + " ***\n")
file.write("結轉日期" + start_date + "~" + end_date + "\n")

date_fmt = "%Y/%m/%d"
a = datetime.datetime.strptime(start_date, date_fmt)
b = datetime.datetime.strptime(end_date, date_fmt)
delta = b - a
int_diff_date = delta.days
print("days=" + str(int_diff_date) + "\n")

i = 1
cnt = 1
dt = ""
while i <= (int_diff_date+1):
	#print(str(i) + "\n")
	if i==1:
		str_date = start_date
	else:
		str_date = parser.parse(str(dt)).strftime("%Y/%m/%d")

	#print(str_date + "\n")
	
	# 轉民國年日期
	arg_date = str(int(str_date[0:4]) - 1911) + str_date[4:]
	print("讀取" + arg_date + "收盤資料.\n")
	rt = DailyQuoCSV(arg_date)
	
	if rt == "Y":
		cnt += 1
	else:
		print(arg_date + "當天無收盤資料.")
		#file.write(arg_date + "當天無收盤資料.\n")
	
	dt = datetime.datetime.strptime(str_date, date_fmt).date()
	dt = dt + relativedelta(days=1)
	i += 1
	
	# 累計抓滿有收盤資料30天就強制跳出迴圈
	if cnt == 30:
		print("抓滿30天，強制結束.")
		file.write("抓滿30天，強制結束.\n")
		break

tEnd = time.time()#計時結束
file.write ("\n\n\n結轉耗時 %f sec\n" % (tEnd - tStart)) #會自動做進位
file.write("*** End LOG ***\n")

# Close File
file.close()

print("End of prog...")