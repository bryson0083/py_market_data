cd /d d:\py_market_data
python STOCK_GVI_V2.1.py
python STOCK_GVI_FB.py

call activate py34
chcp 65001
python STOCK_SELECT_TYPE01.py
python STOCK_SELECT_TYPE02.py
python STOCK_SELECT_TYPE03.py
python STOCK_SELECT_TYPE04_V1.1.py
python STOCK_SELECT_TYPE05.py
python STOCK_SELECT_TYPE06.py
python STOCK_SELECT_TYPE07.py
call deactivate

pause