cd /d d:\py_market_data
call activate py36
python READ_DAILY_3INSTI_STOCK_CSV.py
python READ_DAILY_3INSTI_STOCK_CSV_SQ.py
python STOCK_CHIP_ANA.py
call deactivate
exit