# 公開觀測資訊站~資產負債表資料讀取
#
# 讀取公開觀測資訊站
# => 財務報表 => 採IFRSs後 => 合併/個別報表 => 合併/個別報表 => 資產負債表
#
# last_modify: 2016/11/01
#

#import
import sys
import sqlite3
import datetime

import os.path
import time
import re 

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from dateutil import parser
from dateutil.parser import parse

# 自動結轉資料
def mode_c():
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

	# 註：依證券交易法第36條及證券期貨局相關函令規定，財務報告申報期限如下：
	# 1.一般行業申報期限：第一季為5月15日，第二季為8月14日，第三季為11月14日，年度為3月31日。
	# 2.金控業申報期限：第一季為5月30日，第二季為8月31日，第三季為11月29日，年度為3月31日。
	# 3.銀行及票券業申報期限：第一季為5月15日，第二季為8月31日，第三季為11月14日，年度為3月31日。
	# 4.保險業申報期限：第一季為5月15日，第二季為8月31日，第三季為11月14日，年度為3月31日。
	# 5.證券業申報期限：第一季為5月15日，第二季為8月31日，第三季為11月14日，年度為3月31日。

	# 只在以下特定幾天結轉季報資料
	if mmdd == "0405":
		yyy = str(int(yyy) - 1)
		qq = "04"
	elif mmdd == "0605":
		qq = "01"
	elif mmdd == "0905":
		qq = "02"
	elif mmdd == "1205":
	#elif mmdd == "0112":
		qq = "03"
	else:
		file.write("mode_c 未到批次結轉時間，執行結束...\n")
		sys.exit(mmdd + " No need to get data.")

	file.write("mode_c 自動抓取當季 yyyqq=" + yyy + qq + "\n")
	print("mode_c 自動抓取當季 yyyqq=" + yyy + qq)

	MOPS_YQ_2(yyy, qq)



# 手動輸入條件結轉資料
def mode_h():
	yyyy = str(input("輸入抓取資料年分(YYYY):"))
	qq = str(input("輸入抓取資料季別(QQ):"))

	# 轉西元年為民國年
	yyy = str(int(yyyy) - 1911)

	# 寫入LOG File
	file.write("mode_h 手動結轉 yyyqq=" + yyy + qq + "\n")
	print("mode_h 手動結轉 yyyqq=" + yyy + qq)

	MOPS_YQ_2(yyy, qq)



# 跑特定區間，結轉資料(自行修改參數條件)
def mode_a():

	for y in range(2013,2014,1):
		#print("y=" + str(y))
		yyy = str(y - 1911)
		q = 1
		while q <= 4:
			if q == 1:
				qq = "01"
			elif q == 2:
				qq = "02"
			elif q == 3:
				qq = "03"
			else:
				qq = "04"

			file.write("mode_a 特定區間，結轉yyyqq=" + yyy + qq + "\n")
			print("mode_a 特定區間，結轉yyyqq=" + yyy + qq)
			MOPS_YQ_2(yyy, qq)

			q += 1



# 結轉網頁資料
def MOPS_YQ_2(arg_yyy, arg_qq):
	#sys.exit("For testing abort ......")

	# 建立資料庫連線
	conn = sqlite3.connect('market_price.sqlite')

	# 建立網頁讀取
	driver = webdriver.Chrome()
	driver.get("http://mops.twse.com.tw/mops/web/t163sb05")

	# 查詢網頁條件參數
	#sear_yyy = "102"
	#qq = "01"
	sear_yyy = arg_yyy
	qq = arg_qq

	#print("input arg ==>" + str(sear_yyy) + str(qq))
	#sys.exit("For testing abort ......")

	sear_qq= str(int(qq))
	yyyy = str(int(sear_yyy) + 1911)

	#網頁查詢條件輸入，並提交表單
	elem = driver.find_element_by_name("year")
	elem.send_keys(sear_yyy)

	elem = driver.find_element_by_name("season")
	elem.find_element_by_xpath("//select[@name='season']/option[text()='" + sear_qq + "']").click()
	driver.find_element_by_xpath("//input[@type='button'][@value=' 查詢 ']").click()

	cnt = 0
	delay = 10 # seconds

	while True:
		try:
		    element_present = EC.presence_of_element_located((By.NAME, 'fm2'))
		    WebDriverWait(driver, delay).until(element_present)
		    print ("Page is ready!")
		    break
		except TimeoutException:
		    cnt += 1
		    print ("Load cnt=" + str(cnt))
		    if cnt >= 3:
		    	# 關閉瀏覽器視窗
		    	driver.quit();

		    	# 讀取時間太久，直接結束程式
		    	file.write("@@@ 網頁讀取逾時或該網頁無資料. @@@\n")
		    	sys.exit("@@@ 網頁讀取逾時或該網頁無資料. @@@")


	#計算有多少個符合條件特徵的表格個數
	tables = driver.find_elements_by_xpath("//table[@class='hasBorder']")
	tb_cnt = len(tables)
	print("符合條件特徵的table個數=" + str(tb_cnt))

	#網頁中符合條件的table資料都掃過一遍
	i = 1
	#tb_cnt = 1
	while i <= tb_cnt:
		print("i=" + str(i))

		#組合搜尋條件參數
		arg_str = "//table[@class='hasBorder'][" + str(i) + "]/tbody"

		#讀取表格資料內容
		table = elem.find_element_by_xpath(arg_str)
		for row in table.find_elements_by_xpath(".//tr"):
			# 把tr下，所有td的欄位資料讀取到一個list中
			a_list = [td.text for td in row.find_elements_by_xpath(".//td[text()]")]

			# a_list從td tag讀進資料，第一筆資料都是empty list，因此要過濾掉
			if a_list:
				a_list = a_list
				last_elem_idx = len(a_list)
				#print(a_list[0] + "," + a_list[1] + "," + a_list[last_elem_idx-1])

				comp_id = a_list[0]		# 公司股票號碼
				comp_name = a_list[1]	# 公司股票名稱

				bvps = a_list[last_elem_idx-1] # 每股參考淨值
				bvps = re.sub("[^-0-9^.]", "", bvps) # 數字做格式控制

				print(comp_id + "," + comp_name + "," + bvps)

				# 最後維護日期時間
				str_date = str(datetime.datetime.now())
				date_last_maint = parser.parse(str_date).strftime("%Y%m%d")
				time_last_maint = parser.parse(str_date).strftime("%H%M%S")
				prog_last_maint = "MOPS_YQ_2"

				sqlstr = "select count(*) from MOPS_YQ "
				sqlstr = sqlstr + "where "
				sqlstr = sqlstr + "COMP_ID='" + comp_id + "' and "
				sqlstr = sqlstr + "YYYY='" + yyyy + "' and "
				sqlstr = sqlstr + "QQ='" + qq + "' "

				#print(sqlstr)
				cursor = conn.execute(sqlstr)
				result = cursor.fetchone()

				if result[0] == 0:
					sqlstr = "insert into MOPS_YQ values ("
					sqlstr = sqlstr + "'" + comp_id + "',"
					sqlstr = sqlstr + "'" + comp_name + "',"
					sqlstr = sqlstr + "'" + yyyy + "',"
					sqlstr = sqlstr + "'" + qq + "',"
					sqlstr = sqlstr + "0,"
					sqlstr = sqlstr + " " + bvps + ","
					sqlstr = sqlstr + "'" + date_last_maint + "',"
					sqlstr = sqlstr + "'" + time_last_maint + "',"
					sqlstr = sqlstr + "'" + prog_last_maint + "' "
					sqlstr 	= sqlstr + ") "

				else:
					sqlstr = "update MOPS_YQ set "
					sqlstr = sqlstr + "bvps=" + bvps + ","
					sqlstr = sqlstr + "date_last_maint='" + date_last_maint + "',"
					sqlstr = sqlstr + "time_last_maint='" + time_last_maint + "',"
					sqlstr = sqlstr + "prog_last_maint='" + prog_last_maint + "' "
					sqlstr = sqlstr + "where "
					sqlstr = sqlstr + "COMP_ID='" + comp_id + "' and "
					sqlstr = sqlstr + "YYYY='" + yyyy + "' and "
					sqlstr = sqlstr + "QQ='" + qq + "' "

				try:
					cursor = conn.execute(sqlstr)
					conn.commit()
				except sqlite3.Error as er:
					file.write("\n@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n")
					file.write("\n資料 " + comp_id + "," + comp_name + "," + yyyy + qq + "," + bvps + "\n")
					file.write("資料庫錯誤:\n" + er.args[0] + "\n")
					file.write("\n@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n")
					print ("\n@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n")
					print ("\n資料 " + comp_id + "," + comp_name + "," + yyyy + qq + "," + bvps + "\n")
					print ("資料庫錯誤:\n" + er.args[0] + "\n")
					print ("\n@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n")

				# 關閉DB cursor
				cursor.close()
		i += 1

	# 資料庫連線關閉
	conn.close()

	# 關閉瀏覽器視窗
	driver.quit();

	file.write("擷取資料完畢 ...\n")
	print ("擷取資料完畢 ...")



try:
	run_mode = sys.argv[1]
	run_mode = run_mode.upper()
except Exception as e:
	run_mode = "C"

print("you choose mode " + run_mode)

# 寫入LOG File
dt=datetime.datetime.now()

print("##############################################")
print("##    公開觀測資訊站~資產負債表資料讀取     ##")
print("##                                          ##")
print("##                                          ##")
print("##   datetime: " + str(dt) +            "   ##")
print("##############################################")

str_date = str(dt)
str_date = parser.parse(str_date).strftime("%Y%m%d")

name = "MOPS_YQ_2_LOG_" + str_date + ".txt"
file = open(name, 'a', encoding = 'UTF-8')

tStart = time.time()#計時開始
file.write("\n\n\n*** LOG datetime  " + str(datetime.datetime.now()) + " ***\n")

if run_mode == "C":
	file.write("mode_c 自動抓取當季，結轉資料...\n")
	mode_c()
elif run_mode == "H":
	file.write("mode_h 手動輸入區間，結轉資料...\n")
	mode_h()
elif run_mode == "A":
	file.write("mode_a 跑特定區間，結轉資料...\n")
	mode_a()
else:
	file.write("模式錯誤，結束程式...\n")
	sys.exit("模式錯誤，結束程式...\n")

tEnd = time.time()#計時結束
file.write ("\n\n\n結轉耗時 %f sec\n" % (tEnd - tStart)) #會自動做進位
file.write("*** End LOG ***\n")

# Close File
file.close()