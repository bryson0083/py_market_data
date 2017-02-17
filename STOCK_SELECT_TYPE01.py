import talib
from talib import MA_Type
import sqlite3
import numpy as np
import pandas as pd
import xlsxwriter
import datetime
from dateutil import parser
from dateutil.relativedelta import relativedelta
from firebase import firebase

#檢查list數值是否呈現遞增，回傳Y/N
def CHK_IS_CONTINUE_UP(df):
	i = 0
	flag = "Y"
	for elem in df:
		if i == 0:
			prev_elem = elem
			i += 1
			continue
		else:
			if elem <= prev_elem:
				flag = "N"
				break
			else:
				prev_elem = elem

		i += 1

	return flag


#選股判斷
def JUG_STOCK_QUO(sear_comp_id, str_prev_date, str_today, arg_mode):
	#日線資料讀取
	if arg_mode == "D" :
		strsql  = "select close, quo_date from STOCK_QUO "
		strsql += "where "
		strsql += "SEAR_COMP_ID='" + sear_comp_id + "' and "
		strsql += "QUO_DATE between '" + str_prev_date + "' and '" + str_today + "' "
		strsql += "order by QUO_DATE "

	#周線資料讀取
	elif arg_mode == "W":
		strsql  = "select close, quo_date from STOCK_QUO_WEEKLY "
		strsql += "where "
		strsql += "SEAR_COMP_ID='" + sear_comp_id + "' and "
		strsql += "QUO_DATE between '" + str_prev_date + "' and '" + str_today + "' "
		strsql += "order by QUO_DATE "		

	#月線資料讀取
	elif arg_mode == "M":
		strsql  = "select close, quo_date from STOCK_QUO_MONTH "
		strsql += "where "
		strsql += "SEAR_COMP_ID='" + sear_comp_id + "' and "
		strsql += "QUO_DATE between '" + str_prev_date + "' and '" + str_today + "' "
		strsql += "order by QUO_DATE "

	cursor = conn.execute(strsql)
	result = cursor.fetchall()
	re_len = len(result)

	if re_len > 0:
		close = []
		dt = []
		data = []
		for row in result:
			close.append(row[0])
			dt.append(row[1])
			data.append([row[1], row[0]])

	#print(close)
	#print(dt)
	#print(data)

	df = pd.DataFrame(data, columns = ['日期','收盤價'])

	#LIST轉換為Numpy Array
	close = np.array(close)

	#計算移動平均(ma18)
	ma18 = talib.MA(close, timeperiod=18, matype=0)

	#計算移動平均(ma50)
	ma50 = talib.MA(close, timeperiod=50, matype=0)

	#print(close)
	#print(ma18)
	#print(ma50)

	#均線 18 ma 值導到dataframe
	df['ma18'] = ma18

	#計算均線值與前一天的差(作為變動方向)
	df['ma18_diff_yesterday'] = df['ma18'] - df['ma18'].shift(1)

	#均線 50 ma 值導到dataframe
	df['ma50'] = ma50

	#計算均線值與前一天的差(作為變動方向)
	df['ma50_diff_yesterday'] = df['ma50'] - df['ma50'].shift(1)

	#計算均線間的距離(以百分比表示)
	df['dist_ma_pct'] = abs((df['ma50'] - df['ma18']) / df['ma18'] * 100)

	#檢查最新的三天ma18資料是否連三漲
	ma18_cnt_tot = len(df['ma18'])
	df_a = df['ma18'][ma18_cnt_tot-4:ma18_cnt_tot]
	#df_a = [-3.1,-2.5,0,4,6,7,8]
	#print(df_a)
	ma18_increment_yn = CHK_IS_CONTINUE_UP(df_a)
	#print("ma18_increment_yn=" + ma18_increment_yn)

	#檢查最新的三天ma50資料是否連三漲
	ma50_cnt_tot = len(df['ma50'])
	df_a = df['ma50'][ma50_cnt_tot-4:ma50_cnt_tot]
	#print(df_a)
	ma50_increment_yn = CHK_IS_CONTINUE_UP(df_a)
	#print("ma50_increment_yn=" + ma50_increment_yn)

	#最新一筆ma18大於ma50
	ma18_last = float(df['ma18'].tail(1))
	ma50_last = float(df['ma50'].tail(1))
	
	ma18bt50_yn = "N"
	if ma18_last > ma50_last:
		ma18bt50_yn = "Y"

	"""
	# for test 運算結果寫入EXCEL檔
	if sear_comp_id == "1220.TW":
		print("ma18_increment_yn=" + ma18_increment_yn)
		print("ma50_increment_yn=" + ma50_increment_yn)
		print("ma18bt50_yn=" + ma18bt50_yn)
		file_name = 'ma_sel_' + sear_comp_id + '.xlsx'
		writer = pd.ExcelWriter(file_name, engine='xlsxwriter')
		df.to_excel(writer, sheet_name='stock', index=False)
		writer.save()
	"""

	#最後總評
	if ma18_increment_yn == "Y" and ma50_increment_yn == "Y" and ma18bt50_yn == "Y":
		JUG_STOCK_QUO = "Y"
	else:
		JUG_STOCK_QUO = "N"

	return JUG_STOCK_QUO

	#關閉cursor
	cursor.close()

############################################################################
# Main                                                                     #
############################################################################
#取得當天日期
dt = datetime.datetime.now()
str_today = parser.parse(str(dt)).strftime("%Y%m%d")
print(str_today)

#取得當天往前推180天日期
dt = datetime.datetime.now() + relativedelta(days=-3650)
str_prev_date = str(dt)[0:10]
str_prev_date = parser.parse(str_prev_date).strftime("%Y%m%d")
print(str_prev_date)

#建立資料庫連線
conn = sqlite3.connect("market_price.sqlite")

strsql  = "select distinct SEAR_COMP_ID from STOCK_QUO "
strsql += "where "
#strsql += "QUO_DATE between '" + str_prev_date + "' and '" + str_today + "' "
strsql += "QUO_DATE = '" + str_today + "' "
strsql += "order by SEAR_COMP_ID "
#strsql += "limit 1 "

cursor = conn.execute(strsql)
result = cursor.fetchall()
re_len = len(result)
ls_stk_select = []

if re_len > 0:
	i = 0
	for row in result:
		#print(row[0])

		strsql  = "select COMP_NAME from STOCK_COMP_LIST where SEAR_COMP_ID = '" + row[0] + "'"
		cursor2 = conn.execute(strsql)
		result2 = cursor2.fetchone()

		if result2 is not None:
			comp_name = result2[0]
		else:
			comp_name = "NA"

		#關閉cursor
		cursor2.close()

		#日線等級選股判斷
		#if row[0] == "1220.TW":
		select_d_yn = JUG_STOCK_QUO(row[0], str_prev_date, str_today, "D")
		#	print(select_d_yn)

		#周線等級選股判斷
		#if row[0] == "1220.TW":
		select_w_yn = JUG_STOCK_QUO(row[0], str_prev_date, str_today, "W")
		#	print(select_w_yn)

		#月線等級選股判斷
		#if row[0] == "1220.TW":
		select_m_yn = JUG_STOCK_QUO(row[0], str_prev_date, str_today, "M")
		#	print(select_m_yn)

		#if select_d_yn == "Y":
		if select_d_yn == "Y" and select_w_yn == "Y" and select_m_yn == "Y":
			i += 1
			ls_stk_select.append([row[0],comp_name])
			print("選出股票=" + row[0] + "  " + comp_name)


print("共選出" + str(i) + "檔股票...\n")
#print(ls_stk_select)

#ls_stk_select list拋到pandas
df = pd.DataFrame(ls_stk_select, columns = ['股票編號','公司名稱'])
#print(df)

#運算結果寫入EXCEL檔
file_name = "STOCK_SELECT_TYPE01_" + str_today + ".xlsx"
writer = pd.ExcelWriter(file_name, engine='xlsxwriter')
df.to_excel(writer, sheet_name='LIST', index=False)
writer.save()

#關閉cursor
cursor.close()

#關閉資料庫連線
conn.close()

print("End of prog...")