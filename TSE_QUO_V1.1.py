# -*- coding: utf-8 -*-

import requests
import time
from random import randint
from dateutil import parser
import datetime
from dateutil.relativedelta import relativedelta
import os.path

def DailyQuoCSV(sear_date):
	flag = "N"

	# 轉民國年日期
	file_name_date = str(int(sear_date[0:4]) - 1911) + sear_date[4:]

	file_name = "./tse_quo_data/" + file_name_date + ".csv"
	is_existed = os.path.exists(file_name)
	str_url = "http://www.tse.com.tw/exchangeReport/MI_INDEX?response=csv&date=" + sear_date + "&type=ALLBUT0999"
	#print(str_url)

	# 檢查若已有檔案，則不再下載
	if is_existed == False:
		headers = {'User-Agent':'User-Agent:Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36'}
		session = requests.session()
		
		# 讀取查詢頁面
		r = session.get(str_url, headers=headers)
		r.encoding = "utf-8"

		# 取得回傳資料的長度
		len_r = len(r.text)
		#print(len_r)

		data_yn = "Y"
		if len_r == 0:	# 表示當天無收盤資料
			data_yn = "N"
			
			# 若當天無資料，還是產生一個檔案
			file2 = open(file_name, "a", encoding="big5")
			file2.write("當天無收盤資料")
			file2.close()

		#print("data_yn=" + data_yn + "\n")
		if data_yn == "Y":
			flag = "Y"
			with open(file_name, 'wb') as f:
				for chunk in r.iter_content(chunk_size=1024): 
					if chunk: # filter out keep-alive new chunks
						f.write(chunk)
			#close file
			f.close()
			
			sleep_sec = randint(1,3)
			print("waiting " + str(sleep_sec) + " secs.")
			time.sleep(sleep_sec)

	else:
		print(sear_date + "資料檔已存在，不再更新資料.")

	return flag

############################################################################
# Main                                                                     #
############################################################################
print("Executing TSE_QUO...")

#起訖日期(預設跑當天日期到往前推7天)
str_date = str(datetime.datetime.now())
str_date = parser.parse(str_date).strftime("%Y%m%d")
end_date = str_date

date_1 = datetime.datetime.strptime(end_date, "%Y%m%d")
start_date = date_1 + datetime.timedelta(days=-7)
start_date = str(start_date)[0:10]
start_date = parser.parse(start_date).strftime("%Y%m%d")

#for需要時手動設定日期區間用(最早的資料從2004年2月11日起提供)
#start_date = "20040211"
#end_date = "20170525"

print("結轉日期" + start_date + "~" + end_date)

#LOG檔
str_date = str(datetime.datetime.now())
str_date = parser.parse(str_date).strftime("%Y%m%d")
name = "TSE_QUO_LOG_" + str_date + ".txt"
file = open(name, "a", encoding="UTF-8")

tStart = time.time()#計時開始
file.write("\n\n\n*** LOG datetime  " + str(datetime.datetime.now()) + " ***\n")
file.write("結轉日期" + start_date + "~" + end_date + "\n")

date_fmt = "%Y%m%d"
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
		str_date = parser.parse(str(dt)).strftime("%Y%m%d")

	print("讀取" + str_date + "收盤資料.\n")
	rt = DailyQuoCSV(str_date)
	
	if rt == "Y":
		cnt += 1
	else:
		print(str_date + "當天無收盤資料.")
		#file.write(arg_date + "當天無收盤資料.\n")

	dt = datetime.datetime.strptime(str_date, date_fmt).date()
	dt = dt + relativedelta(days=1)
	i += 1
	
	"""
	# 累計抓滿有收盤資料90天就強制跳出迴圈
	if cnt == 90:
		print("抓滿90天，強制結束.")
		file.write("抓滿90天，強制結束.\n")
		break
	"""
	
tEnd = time.time()#計時結束
file.write ("\n\n\n結轉耗時 %f sec\n" % (tEnd - tStart)) #會自動做進位
file.write("*** End LOG ***\n")

# Close File
file.close()

print("End of prog...")