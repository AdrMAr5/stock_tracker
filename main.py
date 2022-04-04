import sys
import requests
import sqlite3
import sqlalchemy
import pandas as pd
from datetime import datetime, timedelta
from prefect import task, Flow
from prefect.schedules import IntervalSchedule


class InvalidDataException(Exception):
	pass


ACCESS_KEY = '62752580be819ba497213623997646e9'
URL = 'http://data.fixer.io/api/latest?access_key=' + ACCESS_KEY
DATABASE_LOCATION = 'sqlite:///currencies_stock.sqlite'
headers = {
'If-None-Match': "0b9d2094351f5bb30a1cc2a6e8645966",
'If-Modified-Since': 'Tue, 29 Mar 2022 13:38:03 GMT'
}


@task
def send_request():
	r = requests.get(url=URL, headers=headers)
	print(r.text)
	data_valid(r)
	return r


def data_valid(resp):

	try:
		# check if data changed since last request
		with open('last_request.txt', 'a+') as file:
			file.seek(0)
			last_etag = file.readline()
			cur_etag = resp.headers['etag']

			if last_etag.rstrip() == cur_etag:
				raise InvalidDataException

			file.seek(0)
			file.truncate()
			file.write(cur_etag + '\n')

		# check if operation was successful
		dic = resp.json()
		if not dic['success']:
			raise InvalidDataException

	except InvalidDataException:
		print('dane niepoprawne')
		sys.exit()



@task()
def parse_data(r):

	data = r.json()

	rates = data['rates']
	timestamp = data['timestamp']
	date = data['date']
	new_dict = {'symbol': [], 'rate': [], 'timestamp': []}
	time = data['timestamp']

	for key, value in rates.items():
		new_dict['symbol'].append(key)
		new_dict['rate'].append(value)
		new_dict['timestamp'].append(datetime.utcfromtimestamp(int(timestamp)))

	return pd.DataFrame(new_dict)

@task
def append_to_db(df):
	engine = sqlalchemy.create_engine(DATABASE_LOCATION)
	conn = sqlite3.connect('currencies_stock.sqlite')
	c = conn.cursor()
	c.execute("""CREATE TABLE IF NOT EXISTS currencies_stock(
		symbol text,
		rate text,
		timestamp text
	)""")
	df.to_sql('currencies_stock', engine, index=False, if_exists='append')



def build_flow():
	with Flow('stock_tracker_etl') as flow:
		r = send_request()
		data = parse_data(r)
		append_to_db(data)

	return flow




flow = build_flow()
flow.run()




