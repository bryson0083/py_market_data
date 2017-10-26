# -*- coding: utf-8 -*-
"""
每日程式選股檔案郵件發送

@author: Bryson Xue

@Note: 
	1. 透過郵件發送每日程式選股結果

@Ref:
	
"""
import json
import util.Mailer as GMail
import datetime
from datetime import date
from dateutil import parser

############################################################################
# Main                                                                     #
############################################################################
print("Executing DAILY_MAIL...")

#取得當天日期
dt = datetime.datetime.now()
str_date = parser.parse(str(dt)).strftime("%Y%m%d")

#讀取帳密參數檔
with open('account.json') as data_file:
	data = json.load(data_file)

#附件清單
file_list  = ['STOCK_CHIP_ANA_' + str_date + '.xlsx', 'STOCK_SELECT_TYPE09_' + str_date + '.xlsx', 'STOCK_SELECT_TYPE10_' + str_date + '.xlsx']
file_list2 = ['STOCK_CHIP_ANA_' + str_date + '.xlsx', 'STOCK_SELECT_TYPE09_' + str_date + '.xlsx', 'STOCK_SELECT_TYPE10_' + str_date + '.xlsx', 'STOCK_SELECT_TYPE11_' + str_date + '.xlsx', 'STOCK_SELECT_TYPE12_' + str_date + '.xlsx']
file_list3 = ['STOCK_CHIP_ANA_' + str_date + '.xlsx', 'STOCK_SELECT_TYPE06_' + str_date + '.xlsx', 'STOCK_SELECT_TYPE07_' + str_date + '.xlsx', 'STOCK_SELECT_TYPE09_' + str_date + '.xlsx', 'STOCK_SELECT_TYPE10_' + str_date + '.xlsx', 'STOCK_SELECT_TYPE11_' + str_date + '.xlsx', 'STOCK_SELECT_TYPE12_' + str_date + '.xlsx']

#發送郵件程序
m = GMail.Mailer()
m.send_from = data['gmail']['id']				# 寄件者
m.gmail_password = data['gmail']['pwd']			# 寄件者 GMAIL 密碼

print('@@@ 郵件清單1發送:')
m.recipients = ['alvan16888@gmail.com', 'yu62493@gmail.com']	
m.subject = '每日程式選股清單' + str_date
m.message = '你好:\n以下為本日程式選股清單，詳見附件檔案.\n\n\n'
m.attachments = file_list
m.send_email()

print('@@@ 郵件清單2發送:')
m.recipients = ['tenya.shiue@gmail.com']	
m.subject = '每日程式選股清單' + str_date
m.message = '你好:\n以下為本日程式選股清單，詳見附件檔案.\n\n\n'
m.attachments = file_list2
m.send_email()

print('@@@ 郵件清單3發送:')
m.recipients = ['bryson0083@gmail.com']
m.subject = '每日程式選股清單' + str_date
m.message = '你好:\n以下為本日程式選股清單，詳見附件檔案.\n\n\n'
m.attachments = file_list3
m.send_email()

print("End of prog.")