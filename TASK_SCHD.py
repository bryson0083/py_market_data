# -*- coding: utf-8 -*-
"""
定時排程管理

@author: Bryson Xue

@Note:
	所有需要執行之排程程序，集中管理執行

@Ref:
	https://schedule.readthedocs.io/en/stable/
	https://stackoverflow.com/questions/1818774/executing-a-subprocess-fails

"""
import sys
import os.path
import schedule
import time
import multiprocessing
import datetime
from dateutil.parser import parse
from dateutil import parser
from subprocess import Popen

########### 以下為非公用程式import ##################################
import GET_DAILY_MTSS as get_mtss
import READ_DAILY_MTSS_CSV as rd_mtss
import GET_DAILY_MTSS_SQ as get_mtss_sq
import READ_DAILY_MTSS_CSV_SQ as rd_mtss_sq
import GET_DAILY_FR_HOLDING_SHARES as get_frhds
import READ_DAILY_FR_HOLDING_SHARES_CSV as rd_frhds
import GET_DAILY_FR_HOLDING_SHARES_SQ as get_frhds_sq
import READ_DAILY_FR_HOLDING_SHARES_CSV_SQ as rd_frhds_sq

import MOPS_YQ_1_V3_2 as yq1
import MOPS_YQ_2_V3_1 as yq2
import STOCK_COMP_LIST_V1_1 as com_ls
import STOCK_COMP_LIST_SQ as com_ls_sq

import STOCK_DIVIDEND as divd
import TSE_QUO_V1_2 as get_tse
import TSE_QUO_CSV_V1_2 as rd_tse
import SQUOTE_V1_1 as get_sqo
import SQUOTE_CSV_V1_1 as rd_sqo
import QUO_MONTH_V1_1 as quo_m
import QUO_WEEK_V1_1 as quo_w

import MOVE_FILE as mv_file
import CK_OPEN as ck_open
import DAILY_MAIL as dmail
import GET_TDCC_STOCK_DISPERSION as tdcc

def job():
	global task_mode

	str_date = str(datetime.datetime.now())
	print("執行job 1: 目前時間:" + str_date + "\n")
	get_mtss.MAIN_GET_DAILY_MTSS(task_mode)
	rd_mtss.MAIN_READ_DAILY_MTSS_CSV()
	get_mtss_sq.MAIN_GET_DAILY_MTSS_SQ(task_mode)
	rd_mtss_sq.MAIN_READ_DAILY_MTSS_CSV_SQ()
	get_frhds.MAIN_GET_DAILY_FR_HOLDING_SHARES(task_mode)
	rd_frhds.MAIN_READ_DAILY_FR_HOLDING_SHARES_CSV()
	get_frhds_sq.MAIN_GET_DAILY_FR_HOLDING_SHARES_SQ(task_mode)
	rd_frhds_sq.MAIN_READ_DAILY_FR_HOLDING_SHARES_CSV_SQ()

def job2():
	str_date = str(datetime.datetime.now())
	print("執行job 2: 目前時間:" + str_date + "\n")
	yq1.MAIN_MOPS_YQ_1('C')
	yq2.MAIN_MOPS_YQ_2('C')
	com_ls.MAIN_STOCK_COMP_LIST()
	com_ls_sq.MAIN_STOCK_COMP_LIST_SQ()

def job3():
	str_date = str(datetime.datetime.now())
	print("執行job 3: 目前時間:" + str_date + "\n")
	divd.MAIN_STOCK_DIVIDEND('C')
	get_tse.MAIN_TSE_QUO()
	rd_tse.MAIN_TSE_QUO_READ_CSV()
	get_sqo.MAIN_SQUOTE()
	rd_sqo.MAIN_SQUOTE_CSV()
	quo_m.MAIN_QUO_MONTH()
	quo_w.MAIN_QUO_WEEK()

def job4():
	global task_mode

	str_date = str(datetime.datetime.now())
	arg_date = parser.parse(str(str_date)).strftime("%Y%m%d")

	print("執行job 4: 目前時間:" + str_date + "\n")
	mv_file.MAIN_MV_FILE() 	#之前分析結果的excel檔，移至歷史檔案資料夾存放

	p = Popen("py_batch_chip_ana_data.bat")
	stdout, stderr = p.communicate()

	p = Popen("py_batch_chip_ana_data_storedb.bat")
	stdout, stderr = p.communicate()

	ck = ck_open.MAIN_CK_OPEN(arg_date) #檢查當天是否有開市
	if ck == True:
		p = Popen("STOCK_SELECT.bat")
		stdout, stderr = p.communicate()

		#發送郵件、選股結果上傳(固定家裡那台做)
		if task_mode == 'A':
			p = Popen("STOCK_SELECT_FB.bat")
			stdout, stderr = p.communicate()

			#dmail.MAIN_DAILY_MAIL()

def job5():
	str_date = str(datetime.datetime.now())
	print("執行job 5: 目前時間:" + str_date + "\n")
	tdcc.MAIN_GET_TDCC_STOCK_DISPERSION('A')

if __name__ == '__main__':
	global task_mode

	print("Executing " + os.path.basename(__file__) + "...")

	#取得目前時間
	dt = datetime.datetime.now()
	str_day = str(dt.day)	#取得當天日期，日的部分
	#print(str_day)

	#依據所選模式，決定執行時間
	try:
		task_mode = sys.argv[1]
		task_mode = task_mode.upper()
	except Exception as e:
		task_mode = "A"

	print("task mode(A:家裡, B:公司): " + task_mode)

	#手動測試單獨執行用
	#ss05.MAIN_STOCK_SELECT_TYPE05()
	#job()

	#上市櫃信用交易統計、融資融券彙總，資料抓取
	#上市櫃外資及陸資投資持股統計，資料抓取
	if task_mode == "B":
		schedule.every().day.at("08:15").do(job)
	else:
		schedule.every().day.at("22:00").do(job)

	#上市櫃公司清單，資料抓取
	#上市櫃綜合損益表資料、資產負債表，資料抓取
	schedule.every().day.at("11:00").do(job2)

	#除權息訊息抓取
	#上市櫃股票收盤報價資料抓取
	#周、月線資料結轉
	schedule.every().day.at("15:00").do(job3)

	#上市櫃三大法人買賣超資料抓取
	#選股程式執行
	#選股結果郵件發送
	schedule.every().day.at("16:15").do(job4)

	#上市櫃，集保中心~集保戶股權分散表查詢，資料抓取(當周資料要周六後才會有)
	schedule.every().saturday.at("17:00").do(job5)
	schedule.every().sunday.at("17:00").do(job5)

	while True:
		schedule.run_pending()
		time.sleep(1)
