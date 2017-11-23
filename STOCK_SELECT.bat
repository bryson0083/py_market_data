cd /d d:\py_market_data
python MOVE_FILE.py
python GET_DAILY_3INSTI_STOCK_V1_1.py
python READ_DAILY_3INSTI_STOCK_CSV.py
python GET_DAILY_3INSTI_STOCK_SQ.py
python READ_DAILY_3INSTI_STOCK_CSV_SQ.py
python STOCK_CHIP_ANA.py

call activate py34
chcp 65001
python STOCK_SELECT_TYPE05.py
python STOCK_SELECT_TYPE06.py
python STOCK_SELECT_TYPE09.py
python STOCK_SELECT_TYPE10.py
python STOCK_SELECT_TYPE11.py
python STOCK_SELECT_TYPE12.py
call deactivate

python STOCK_SELECT_TYPE09_FB.py
python STOCK_SELECT_TYPE10_FB.py
python STOCK_SELECT_TYPE12_FB.py
python DAILY_MAIL.py
exit