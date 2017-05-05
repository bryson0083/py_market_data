"""
公司股利分派公告資料彙總表(上市、上櫃)

說明:
抓取年度台股上市櫃公司，除權息日期、現金股利、股票股利資料。
分三種模式抓取

mode_c: 自動抓取當年度除權息資料，在固定日期區間，每天轉一次資料，其他時間不動作。
mode_h: 手動輸入年度，抓取該年度上市櫃公司除權息資料。
mode_a: 抓取年度區間內，所有台股上市櫃公司除權息資料，須注意本模式結轉資料量大。

資料來源:
公開觀測資訊站 => 彙總報表 => 股東會及股利 => 除權息公告
http://mops.twse.com.tw/mops/web/t108sb27
最早的資料從民國94年(2005年)起提供

STOCK_DIVIDEND.py
"""
import time
import pandas as pd
import sqlite3
import datetime
import sys
import os
import requests
from bs4 import BeautifulSoup
from dateutil import parser
from random import randint
import codecs

# 自動結轉資料
def mode_c():
	#有錯誤出現時，設定err_flag=True作為識別
	global err_flag

	#str_date = "2016-04-05"
	str_date = str(datetime.datetime.now())

	# 轉換日期為C8格式字串
	dt_c8 = parser.parse(str_date).strftime("%Y%m%d")
	yyyy = dt_c8[0:4]
	#print(yyyy)

	# 轉西元年為民國年
	yyy = str(int(yyyy) - 1911)
	#print(yyy)

	# 取出月日的部分
	mmdd = dt_c8[4:]
	#print(mmdd)

	#mmdd = "1206"
	# 只在以下特定幾天結轉季報資料
	if mmdd >= "0501" and mmdd <= "0731":
		#yyy = str(int(yyy) - 1)
		file.write("mode_c: 自動抓取當年度股東會資料 yyy=" + yyy + "\n")
		print("mode_c: 自動抓取當年度股東會資料 yyy=" + yyy)

		# 開始抓取資料(上市)
		print("結轉上市公司除權息資料...")
		GET_STOCK_DIVIDEND(yyy, 'sii')

		# 隨機等待60~120秒的時間
		random_sec = randint(60,120)
		print("隨機等待秒數=" + str(random_sec) + "...")
		time.sleep(random_sec)

		# 開始抓取資料(上櫃)
		print("結轉上櫃公司除權息資料...")
		GET_STOCK_DIVIDEND(yyy, 'otc')

	else:
		file.write("mode_c: 未到批次結轉時間，執行結束...\n")
		print("mode_c: date=" + mmdd + " 未到批次結轉時間，執行結束...")
		err_flag = False


# 手動輸入條件結轉資料
def mode_h():
	yyyy = str(input("輸入抓取資料年分(YYYY):"))

	# 轉西元年為民國年
	yyy = str(int(yyyy) - 1911)

	# 寫入LOG File
	file.write("mode_h: 手動結轉年度 yyy=" + yyy + "\n")
	print("mode_h: 手動結轉年度 yyy=" + yyy)

	# 開始抓取資料(上市)
	print("結轉上市公司除權息資料...")
	GET_STOCK_DIVIDEND(yyy, 'sii')

	# 隨機等待60~120秒的時間
	random_sec = randint(60,120)
	print("隨機等待秒數=" + str(random_sec) + "...")
	time.sleep(random_sec)

	# 開始抓取資料(上櫃)
	print("結轉上櫃公司除權息資料...")
	GET_STOCK_DIVIDEND(yyy, 'otc')


# 跑特定區間，結轉資料(自行修改參數條件)
def mode_a(): #最早的資料年度為2005(民國94年起)
	str_date = str(datetime.datetime.now())

	# 轉換日期為C8格式字串
	dt_c8 = parser.parse(str_date).strftime("%Y%m%d")
	yyyy = int(dt_c8[0:4])
	#print(yyyy)

	for y in range(2005,yyyy,1):
		#print("y=" + str(y))
		yyy = str(y - 1911)
		file.write("mode_a: 特定區間，結轉股東會民國" + yyy + "年度，上市櫃公司除權息資料\n")
		print("mode_a: 特定區間，結轉股東會民國" + yyy + "年度，上市櫃公司除權息資料\n")

		# 開始抓取資料(上市)
		print("結轉上市公司除權息資料...")
		GET_STOCK_DIVIDEND(yyy, 'sii')

		# 隨機等待60~120秒的時間
		random_sec = randint(60,120)
		print("隨機等待秒數=" + str(random_sec) + "...")
		time.sleep(random_sec)

		# 開始抓取資料(上櫃)
		print("結轉上櫃公司除權息資料...")
		GET_STOCK_DIVIDEND(yyy, 'otc')

		# 隨機等待180~300秒的時間
		random_sec = randint(180,300)
		print("隨機等待秒數=" + str(random_sec) + "...")
		time.sleep(random_sec)


def GET_STOCK_DIVIDEND(arg_yyy, arg_typek):
	headers = {'User-Agent':'User-Agent:Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36'}
	session = requests.session()

	# 拋送查詢條件到頁面，並取回查詢結果內容
	URL = 'http://mops.twse.com.tw/mops/web/ajax_t108sb27'
	payload = {
			  "encodeURIComponent": "1",
			  "step": "1",
			  "firstin": "1",
			  "off": "1",
			  "keyword4": "",
			  "code1": "",
			  "TYPEK2": "",
			  "checkbtn": "",
			  "queryName": "",
			  "TYPEK": arg_typek,
			  "co_id_1": "",
			  "co_id_2": "",
			  "year": arg_yyy,
			  "month": "",
			  "b_date": "",
			  "e_date": "",
			  "type": "1"
			  }

	r = requests.post(URL, data=payload, headers=headers)

	r.encoding = "utf-8"
	sp = BeautifulSoup(r.text, 'html.parser')
	#print(sp)

	"""
	#for test 讀取存檔案的網頁，避免過度讀取網站
	f=codecs.open("C:/Users/bryson0083/Desktop/DIVI2015.html", 'r', encoding = 'utf-8')
	#print(f.read())

	data = f.read()
	#data.encoding = "utf-8"
	sp = BeautifulSoup(data, 'html.parser')
	"""

	table = sp.findAll('table', attrs={'class':'hasBorder'})
	tb_cnt = len(table) # 網頁上的表格總數
	#print(tb_cnt)

	#報表的抬頭，從民國105年起有改變
	if int(arg_yyy) <= 104:
		head = ['公司代號', '公司名稱','股利所屬年度','權利分派基準日','盈餘轉增資配股(元/股)', \
				'法定盈餘公積、資本公積轉增資配股(元/股)','除權交易日','配股總股數(股)', \
				'配股總金額(元)','配股總股數佔盈餘配股總股數之比例(%)','員工紅利配股率', \
				'盈餘分配之股東現金股利(元/股)','法定盈餘公積、資本公積發放之現金(元/股)',\
				'除息交易日','現金股利發放日','員工紅利總金額(元)','現金增資總股數(股)', \
				'現金增資認股比率(%)','現金增資認購價(元/股)','董監酬勞(元)','公告日期', \
				'公告時間','普通股每股面額']
	else:
		head = ['公司代號', '公司名稱','股利所屬年度','權利分派基準日','盈餘轉增資配股(元/股)', \
				'法定盈餘公積、資本公積轉增資配股(元/股)','除權交易日', \
				'盈餘分配之股東現金股利(元/股)','法定盈餘公積、資本公積發放之現金(元/股)',\
				'除息交易日','現金股利發放日','現金增資總股數(股)', \
				'現金增資認股比率(%)','現金增資認購價(元/股)','公告日期', \
				'公告時間','普通股每股面額']

	i=0
	all_df = pd.DataFrame()
	while i <= tb_cnt-1:

		# 讀取表格資料
		rdata = [[td.text for td in row.select('td')]
				for row in table[i].select('tr')]
		rdata = [x for x in rdata if x != []]
		#print(rdata)

		df = pd.DataFrame(data=rdata, columns = head)
		all_df = pd.concat([all_df,df],ignore_index=True)

		i += 1

	all_df = all_df.loc[:,['公司代號', '公司名稱', '股利所屬年度','盈餘轉增資配股(元/股)','除權交易日','盈餘分配之股東現金股利(元/股)','除息交易日']]
	#print(all_df)

	#資料庫處裡
	proc_db(arg_yyy, all_df)


def proc_db(arg_yyy, df):
	#print(df)

	#有錯誤出現時，設定err_flag=True作為識別
	global err_flag

	#由於公開觀測資訊站，股利所屬年度，會有某些股票不一樣，因此
	#這部分統一給值，不使用網頁上的表格資料。
	setm_year = str(int(arg_yyy) + 1911 - 1)

	for i in range(0,len(df)):
		comp_id = str(df.loc[i]['公司代號'])
		comp_name = str(df.loc[i]['公司名稱'])
		#yyyy = str(int(df.loc[i]['股利所屬年度']) + 1911)
		cash = df.loc[i]['盈餘分配之股東現金股利(元/股)']	#現金股利
		xd_date = df.loc[i]['除息交易日']
		sre = df.loc[i]['盈餘轉增資配股(元/股)']			#股票股利
		xr_date = df.loc[i]['除權交易日']

		#民國年轉為西元年
		if len(xd_date) > 2:
			tmp_date = xd_date.split("/")
			tmp_year = int(tmp_date[0]) + 1911
			xd_date = str(tmp_year) + tmp_date[1] + tmp_date[2]

		#民國年轉為西元年
		if len(xr_date) > 2:
			tmp_date = xr_date.split("/")
			tmp_year = int(tmp_date[0]) + 1911
			xr_date = str(tmp_year) + tmp_date[1] + tmp_date[2]

		if len(cash.strip()) == 0:
			cash = "0"

		if len(sre.strip()) == 0:
			sre = "0"

		if cash == "0":
			xd_date = " "	#若只有除息日期有值，但現金股利為0，則清空除息日期

		if sre == "0":
			xr_date = " "	#若只有除權日期有值，但股票股利為0，則清空除權日期

		# 最後維護日期時間
		str_date = str(datetime.datetime.now())
		date_last_maint = parser.parse(str_date).strftime("%Y%m%d")
		time_last_maint = parser.parse(str_date).strftime("%H%M%S")
		prog_last_maint = "STOCK_DIVIDEND"

		#if comp_id == "2891":
		#	print(comp_id + " " + comp_name + " " + xd_date + " " + cash + " " + xr_date + " " + sre)

		#排除有除權息日期，但無配發股票股利、現金股利股票
		if cash > "0" or sre > "0":
			#資料庫處理
			sqlstr  = "select count(*) from STOCK_DIVIDEND "
			sqlstr += "where "
			sqlstr += "COMP_ID='" + comp_id + "' and "
			sqlstr += "SETM_YEAR='" + setm_year + "' "

			cursor = conn.execute(sqlstr)
			result = cursor.fetchone()

			if result[0] == 0:
				sqlstr  = "insert into STOCK_DIVIDEND "
				sqlstr += "(COMP_ID,COMP_NAME,SETM_YEAR,"
				sqlstr += "XD_DATE,CASH,XR_DATE,SRE,"
				sqlstr += "DATE_LAST_MAINT,TIME_LAST_MAINT,PROG_LAST_MAINT"
				sqlstr += ") values ("
				sqlstr += "'" + comp_id + "',"
				sqlstr += "'" + comp_name + "',"
				sqlstr += "'" + setm_year + "',"
				sqlstr += "'" + xd_date + "',"
				sqlstr += " " + cash + ","
				sqlstr += "'" + xr_date + "',"
				sqlstr += " " + sre + ","
				sqlstr += "'" + date_last_maint + "',"
				sqlstr += "'" + time_last_maint + "',"
				sqlstr += "'" + prog_last_maint + "' "
				sqlstr += ") "
			else:
				sqlstr  = "update STOCK_DIVIDEND set "

				if cash > "0":
					sqlstr += "XD_DATE='" + xd_date + "',"
					sqlstr += "CASH=" + cash + ","
				if sre > "0":
					sqlstr += "XR_DATE='" + xr_date + "',"
					sqlstr += "SRE=" + sre + ","

				sqlstr += "date_last_maint='" + date_last_maint + "',"
				sqlstr += "time_last_maint='" + time_last_maint + "',"
				sqlstr += "prog_last_maint='" + prog_last_maint + "' "
				sqlstr += "where "
				sqlstr += "COMP_ID='" + comp_id + "' and "
				sqlstr += "SETM_YEAR='" + setm_year + "' "

			try:
				#if comp_id == "2891":
				#	print(sqlstr + "\n")

				cursor = conn.execute(sqlstr)
			except sqlite3.Error as er:
				err_flag = True
				file.write(sqlstr + "\n")
				file.write("DB Err:\n" + er.args[0] + "\n")
				print (sqlstr + "\n")
				print ("DB Err:\n" + er.args[0] + "\n")

			# 關閉DB cursor
			cursor.close()

	#過程中有任何錯誤，進行rollback
	if err_flag == False:
		conn.commit()
	else:
		conn.execute("rollback")


#############################################################################
# Main																		#
#############################################################################
print("Executing STOCK_DIVIDEND...")

# 寫入LOG File
dt = datetime.datetime.now()

print("##############################################")
print("##             公開觀測資訊站               ##")
print("##        股利分派情形彙總表資料讀取        ##")
print("##                                          ##")
print("##   datetime: " + str(dt) +            "   ##")
print("##############################################")

str_date = str(dt)
str_date = parser.parse(str_date).strftime("%Y%m%d")

name = "STOCK_DIVIDEND_LOG_" + str_date + ".txt"
file = open(name, 'a', encoding = 'UTF-8')

global err_flag
err_flag = False

tStart = time.time()#計時開始
file.write("\n\n\n*** LOG datetime  " + str(datetime.datetime.now()) + " ***\n")

# 建立資料庫連線
conn = sqlite3.connect('market_price.sqlite')

try:
	run_mode = sys.argv[1]
	run_mode = run_mode.upper()
except Exception as e:
	run_mode = "C"

print("you choose mode " + run_mode)

if run_mode == "C":
	file.write("mode_c: 自動抓取當季，結轉資料...\n")
	mode_c()
elif run_mode == "H":
	file.write("mode_h: 手動輸入區間，結轉資料...\n")
	mode_h()
elif run_mode == "A":
	print("mode_a 跑特定區間，結轉資料...\n")
	file.write("mode_a: 跑特定區間，結轉資料...\n")
	mode_a()
else:
	file.write("Err: 模式錯誤，結束程式...\n")
	sys.exit("Err: 模式錯誤，結束程式...\n")

tEnd = time.time()#計時結束
file.write ("\n\n\n結轉耗時 %f sec\n" % (tEnd - tStart)) #會自動做進位
file.write("*** End LOG ***\n")

# 資料庫連線關閉
conn.close()

# Close File
file.close()

#print("err_flag=" + str(err_flag))
#若執行過程無錯誤，執行結束後刪除log檔案
#if err_flag == False:
#    os.remove(name)

print ("End of prog...")