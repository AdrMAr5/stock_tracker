import requests
import sqlite3
import sqlalchemy
import pandas as pd


def data_validation(dic) -> bool:
	if not dic['success']:
		return False
	return True


ACCESS_KEY = '62752580be819ba497213623997646e9'

URL = 'http://data.fixer.io/api/latest?access_key=' + ACCESS_KEY

r = requests.get(url=URL)
if data_validation(r.json()):
	print(r.json())
	df = pd.DataFrame.from_dict(data=r.json(), orient='index')
	print(df)

conn = sqlite3.connect('currencies_stock.db')
c = conn.cursor()
# c.execute("""CREATE TABLE IF NOT EXISTS currencies_to_eur(
# 	currency text,
#
# )""")