cd /d d:\py_market_data
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

exit