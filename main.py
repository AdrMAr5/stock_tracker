import sys

import requests
import sqlite3
import sqlalchemy
import pandas as pd
from datetime import datetime


class InvalidDataException(Exception):
	pass


ACCESS_KEY = '62752580be819ba497213623997646e9'
URL = 'http://data.fixer.io/api/latest?access_key=' + ACCESS_KEY
DATABASE_LOCATION = 'sqlite:///currencies_stock.sqlite'
headers = {
'If-None-Match': "0b9d2094351f5bb30a1cc2a6e8645966",
'If-Modified-Since': 'Tue, 29 Mar 2022 13:38:03 GMT'
}


def if_data_valid(req) -> bool:

	# check if data changed since last request
	with open('last_request.txt', 'r+') as f:
		last_etag = f.readline()
		cur_etag = r.headers['etag']
		print(last_etag)
		print(cur_etag)
		if last_etag.rstrip() == cur_etag:
			print('ddd')
			raise InvalidDataException

		f.truncate()
		f.write(cur_etag + '\n')

	# check if operation was successful
	dic = req.json()
	if not dic['success']:
		raise InvalidDataException
	return True


r = requests.get(url=URL, headers=headers)

try:
	if_data_valid(r)
except InvalidDataException:
	print('dane niepoprawne')
	sys.exit()
data = r.json()
print(data)

rates = data['rates']
timestamp = data['timestamp']
date = data['date']
new_dict = {'symbol': [], 'rate': [], 'timestamp': []}
time = data['timestamp']

for key, value in rates.items():
	new_dict['symbol'].append(key)
	new_dict['rate'].append(value)
	new_dict['timestamp'].append(datetime.utcfromtimestamp(int(timestamp)))


rates_df = pd.DataFrame(new_dict)
print(rates_df)
engine = sqlalchemy.create_engine(DATABASE_LOCATION)


conn = sqlite3.connect('currencies_stock.sqlite')
c = conn.cursor()
c.execute("""CREATE TABLE IF NOT EXISTS currencies_stock(
	symbol text,
	rate text,
	timestamp text
)""")
rates_df.to_sql('currencies_stock', engine, index=False, if_exists='append')

