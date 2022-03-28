import requests
import sqlite3
import json
import sqlalchemy
import pandas as pd
from datetime import datetime



def if_data_valid(dic) -> bool:
	if not dic['success']:
		return False
	return True


ACCESS_KEY = '62752580be819ba497213623997646e9'

URL = 'http://data.fixer.io/api/latest?access_key=' + ACCESS_KEY

r = requests.get(url=URL)
data = r.json()
if not if_data_valid(data):
	raise 'invalid data'
rates = data['rates']
timestamp = data['timestamp']
date = data['date']
new_dict = {'symbol': [], 'rate': [], 'timestamp': []}
time = data['timestamp']

for key, value in rates.items():
	new_dict['symbol'].append(key)
	new_dict['rate'].append(value)
	new_dict['timestamp'].append(datetime.utcfromtimestamp(int(timestamp)))


df = pd.DataFrame(new_dict)
print(df)

	# print(r.text)
	# names = r.json()
	# rates = r.json()['rates']
	# print(rates)
	# del r.json()['rates']
	#
	# data.update(rates)
	# #df = pd.DataFrame(data)
	# print(df)

conn = sqlite3.connect('currencies_stock.db')
c = conn.cursor()
# c.execute("""CREATE TABLE IF NOT EXISTS currencies_to_eur(
# 	currency text,
#
# )""")