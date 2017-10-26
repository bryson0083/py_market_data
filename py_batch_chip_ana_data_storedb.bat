cd /d d:\py_market_data
python READ_DAILY_3INSTI_STOCK_CSV.py
python READ_DAILY_3INSTI_STOCK_CSV_SQ.py
python STOCK_CHIP_ANA.py
python STOCK_SELECT_TYPE08.py
call activate py34
python STOCK_SELECT_TYPE09.py
python STOCK_SELECT_TYPE10.py
call deactivate
python DAILY_MAIL.py
exit