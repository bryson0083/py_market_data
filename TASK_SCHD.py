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
import schedule
import time
import multiprocessing
import os.path
import datetime
from dateutil.parser import parse
from dateutil import parser
from subprocess import Popen

#以下為非公用程式import
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

import GET_DAILY_3INSTI_STOCK_V1_1 as get_3insti
import READ_DAILY_3INSTI_STOCK_CSV as rd_3insti
import GET_DAILY_3INSTI_STOCK_SQ as get_3insti_sq
import READ_DAILY_3INSTI_STOCK_CSV_SQ as rd_3insti_sq
import DAILY_MAIL as mail

def job():
	str_date = str(datetime.datetime.now())
	print("執行job 1: 目前時間:" + str_date + "\n")
	get_mtss.MAIN_GET_DAILY_MTSS('B')
	rd_mtss.MAIN_READ_DAILY_MTSS_CSV()
	get_mtss_sq.MAIN_GET_DAILY_MTSS_SQ('B')
	rd_mtss_sq.MAIN_READ_DAILY_MTSS_CSV_SQ()
	get_frhds.MAIN_GET_DAILY_FR_HOLDING_SHARES('B')
	rd_frhds.MAIN_READ_DAILY_FR_HOLDING_SHARES_CSV()
	get_frhds_sq.MAIN_GET_DAILY_FR_HOLDING_SHARES_SQ('B')
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
	str_date = str(datetime.datetime.now())
	print("執行job 4: 目前時間:" + str_date + "\n")
	get_3insti.MAIN_GET_DAILY_3INSTI_STOCK()
	rd_3insti.MAIN_READ_DAILY_3INSTI_STOCK_CSV()
	get_3insti_sq.MAIN_GET_DAILY_3INSTI_STOCK_SQ()
	rd_3insti_sq.MAIN_READ_DAILY_3INSTI_STOCK_CSV_SQ()

	p = Popen("STOCK_SELECT.bat")
	stdout, stderr = p.communicate()

	mail.MAIN_DAILY_MAIL()

if __name__ == '__main__':
	#取得目前時間
	dt = datetime.datetime.now()
	str_day = str(dt.day)	#取得當天日期，日的部分
	#print(str_day)

	#手動測試單獨執行用
	#ss05.MAIN_STOCK_SELECT_TYPE05()
	#job4()

	#上市櫃信用交易統計、融資融券彙總，資料抓取
	#上市櫃外資及陸資投資持股統計，資料抓取
	schedule.every().day.at("08:15").do(job)

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

	#CPS、CRM舊LOG檔案刪除(每個月的第3日執行)
	#if str_day == "3":
	#	schedule.every().day.at("09:10").do(job4)
	
	while True:
	    schedule.run_pending()
	    time.sleep(1)