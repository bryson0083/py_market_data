# -*- coding: utf-8 -*-
"""
證交所三大法人個股買賣超日報~每日收盤CSV檔下載

@author: Bryson Xue
@target_rul: 
	查詢網頁 => http://www.tse.com.tw/zh/page/trading/fund/T86.html
	CSV連結  => http://www.tse.com.tw/fund/T86?response=csv&date=20170606&selectType=ALLBUT0999
@Note: 
	證交所三大法人個股買賣超日報
	下載網頁CSV檔
	限定抓取資料，證券全部(不含權證、牛熊證、可展延牛熊證)
"""
import requests
import time
from random import randint
from dateutil import parser
import datetime
from dateutil.relativedelta import relativedelta
import os.path

def DO_WAIT():
	#隨機等待一段時間
	#sleep_sec = randint(30,120)
	sleep_sec = randint(5,10)
	print("間隔等待 " + str(sleep_sec) + " secs.\n")
	time.sleep(sleep_sec)

def GET_CSV(sear_date):
	global err_flag
	rt_flag = False
	file_name = "./daily_3insti_stock_data/" + sear_date + ".csv"
	is_existed = os.path.exists(file_name)
	str_url = "http://www.tse.com.tw/fund/T86?response=csv&date=" + sear_date + "&selectType=ALLBUT0999"
	
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

			if len_r == 2:	# 表示當天無收盤資料
				# 若當天無資料，還是產生一個檔案
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
			file.write("$$$ Err:" + sear_date + " 三大法人個股買賣超資料抓取異常，請檢查是否網路有問題或原網頁已更改. $$$")
			err_flag = True
			rt_flag = False

	else:
		rt_flag = False
		print(sear_date + "資料檔已存在，不再更新資料.\n\n")

	return rt_flag

############################################################################
# Main                                                                     #
############################################################################
print("Executing GET_DAILY_3INSTI_STOCK ...\n\n")
global err_flag
err_flag = False

#起訖日期(預設跑當天日期到往前推7天)
dt = datetime.datetime.now()
start_date = dt + datetime.timedelta(days=-7)
start_date = parser.parse(str(start_date)).strftime("%Y%m%d")
end_date = parser.parse(str(dt)).strftime("%Y%m%d")

#for需要時手動設定日期區間用(資料最早日期20120502起)
#start_date = "20120502"
#end_date = "20161231"

#LOG檔
str_date = str(datetime.datetime.now())
str_date = parser.parse(str_date).strftime("%Y%m%d")
name = "GET_DAILY_3INSTI_STOCK_LOG_" + str_date + ".txt"
file = open(name, "a", encoding="UTF-8")

print_dt = str(str_date) + (' ' * 22)
print("##############################################")
print("##       證交所三大法人個股買賣超日報       ##")
print("##   全部(不含權證、牛熊證、可展延牛熊證)   ##")
print("##                                          ##")
print("##  datetime: " + print_dt +               "##")
print("##############################################")

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
	print("下載 " + str_date + " 三大法人個股買賣超資料.")
	rt = GET_CSV(str_date)
	
	if rt == True:
		cnt += 1
		print(str_date + "下載成功.\n")
	
	dt = datetime.datetime.strptime(str_date, date_fmt).date()
	dt = dt + relativedelta(days=1)	
	i += 1
	
	# 累計抓滿有收盤資料90天就強制跳出迴圈
	if cnt == 90:
		print("抓滿90天，強制結束.")
		file.write("抓滿90天，強制結束.\n")
		break

tEnd = time.time()#計時結束
file.write ("\n\n\n結轉耗時 %f sec\n" % (tEnd - tStart)) #會自動做進位
file.write("*** End LOG ***\n")

# Close File
file.close()

#若執行過程無錯誤，執行結束後刪除log檔案
if err_flag == False:
	os.remove(name)

print("End of prog...")