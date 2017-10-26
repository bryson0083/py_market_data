cd /d d:\py_market_data
python STOCK_GVI_V2.1.py
python STOCK_GVI_FB.py

call activate py34
chcp 65001
python STOCK_SELECT_TYPE06.py
python STOCK_SELECT_TYPE07.py
python STOCK_SELECT_TYPE11.py
python STOCK_SELECT_TYPE12.py
call deactivate

pause