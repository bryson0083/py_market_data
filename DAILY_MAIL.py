# -*- coding: utf-8 -*-
"""
每日程式選股檔案郵件發送

@author: Bryson Xue

@Note:
	透過郵件發送每日程式選股結果

@Ref:

"""
import os
import json
import util.Mailer as GMail
import time
import datetime
from datetime import date
from dateutil import parser

def MAIN_DAILY_MAIL():
	global err_flag
	global file
	err_flag = False

	# 寫入LOG File
	dt=datetime.datetime.now()
	str_date = parser.parse(str(dt)).strftime("%Y%m%d")

	print_dt = str(str_date) + (' ' * 22)
	print("##############################################")
	print("##           每日選股結果郵件發送           ##")
	print("##                                          ##")
	print("##                                          ##")
	print("##  datetime: " + print_dt +               "##")
	print("##############################################")
	print("\n\n")

	name = "DAILY_MAIL_" + str_date + ".txt"
	file = open(name, 'a', encoding = 'UTF-8')
	tStart = time.time()#計時開始
	file.write("\n\n\n*** LOG datetime  " + str(datetime.datetime.now()) + " ***\n")
	file.write("Executing " + os.path.basename(__file__) + "...\n")

	#讀取帳密參數檔
	with open('account.json') as data_file:
		data = json.load(data_file)

	#附件清單
	file_list  = ['STOCK_CHIP_ANA_' + str_date + '.xlsx', 'STOCK_SELECT_TYPE09_' + str_date + '.xlsx', 'STOCK_SELECT_TYPE10_' + str_date + '.xlsx']
	file_list2 = ['STOCK_CHIP_ANA_' + str_date + '.xlsx', 'STOCK_SELECT_TYPE09_' + str_date + '.xlsx', 'STOCK_SELECT_TYPE10_' + str_date + '.xlsx', 'STOCK_SELECT_TYPE11_' + str_date + '.xlsx', 'STOCK_SELECT_TYPE12_' + str_date + '.xlsx']

	#發送郵件程序
	m = GMail.Mailer()
	m.send_from = data['gmail']['id']				# 寄件者
	m.gmail_password = data['gmail']['pwd']			# 寄件者 GMAIL 密碼

	try:
		print('@@@ 郵件清單1發送:')
		m.recipients = ['alvan16888@gmail.com', 'yu62493@gmail.com']
		m.subject = '每日程式選股清單' + str_date
		m.message = '你好:\n以下為本日程式選股清單，詳見附件檔案.\n\n\n'
		m.message +='檔案說明\n'
		m.message +='STOCK_CHIP_ANA:\n'
		m.message +='上市櫃股票，三大法人買賣超狀況\n\n'
		m.message +='STOCK_SELECT_TYPE09:\n'
		m.message +='上市櫃股票，3、5、8日均線糾結選股(當天大漲3%以上) + 個股當日三大法人買賣超資訊\n'
		m.message +='買賣超代碼\n'
		m.message +='A類: 三大法人均買超\n'
		m.message +='B類: 外資、投信買超\n'
		m.message +='C類: 三大法人均賣超\n'
		m.message +='D類: 外資、投信賣超\n'
		m.message +='E類: 三大法人行為出現轉折\n'
		m.message +='Z類: 無明顯趨向(盤整)\n\n'
		m.message +='STOCK_SELECT_TYPE10:\n'
		m.message +='上市櫃股票，3、5、8日均線糾結選股(當天小漲3%以下) + 個股當日三大法人買賣超資訊\n\n'
		m.attachments = file_list
		m.send_email()

		print('@@@ 郵件清單2發送:')
		m.recipients = ['tenya.shiue@gmail.com', 'bryson0083@gmail.com']
		m.subject = '每日程式選股清單' + str_date
		m.message = '你好:\n以下為本日程式選股清單，詳見附件檔案.\n\n\n'
		m.message +='檔案說明\n'
		m.message +='STOCK_CHIP_ANA:\n'
		m.message +='上市櫃股票，三大法人買賣超狀況\n\n'
		m.message +='STOCK_SELECT_TYPE09:\n'
		m.message +='上市櫃股票，3、5、8日均線糾結選股(當天大漲3%以上) + 個股當日三大法人買賣超資訊\n'
		m.message +='買賣超代碼\n'
		m.message +='A類: 三大法人均買超\n'
		m.message +='B類: 外資、投信買超\n'
		m.message +='C類: 三大法人均賣超\n'
		m.message +='D類: 外資、投信賣超\n'
		m.message +='E類: 三大法人行為出現轉折\n'
		m.message +='Z類: 無明顯趨向(盤整)\n\n'
		m.message +='STOCK_SELECT_TYPE10:\n'
		m.message +='上市櫃股票，3、5、8日均線糾結選股(當天小漲3%以下) + 個股當日三大法人買賣超資訊\n\n'
		m.message +='STOCK_SELECT_TYPE11:\n'
		m.message +='選股條件，當天收盤價是剛突破20MA第一天，KD都位於低檔(以40為界)，日均量大於500張\n\n'
		m.message +='STOCK_SELECT_TYPE12:\n'
		m.message +='選股條件，KD位於低檔(20以下)，日均量大於500張\n\n'
		m.attachments = file_list2
		m.send_email()

		#print('@@@ 郵件清單3發送:')
		#m.recipients = ['bryson0083@gmail.com']
		#m.subject = '每日程式選股清單' + str_date
		#m.message = '你好:\n以下為本日程式選股清單，詳見附件檔案.\n\n\n'
		#m.attachments = file_list3
		#m.send_email()

	except Exception as e:
		err_flag = True
		print("DAILY_MAIL raise exception:\n" + str(e) + "\n")
		file.write("DAILY_MAIL raise exception:\n" + str(e) + "\n")

	tEnd = time.time()#計時結束
	file.write ("\n\n\n結轉耗時 %f sec\n" % (tEnd - tStart)) #會自動做進位
	file.write("*** End LOG ***\n")

	# Close File
	file.close()

	#若執行過程無錯誤，執行結束後刪除log檔案
	if err_flag == False:
		os.remove(name)

	print("\n\n每日選股結果郵件發送，執行結束...\n\n\n")

if __name__ == '__main__':
	MAIN_DAILY_MAIL()
