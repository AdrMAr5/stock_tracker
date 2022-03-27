import requests
import sqlite3
import sqlalchemy


def data_validation(dic) -> bool:
	if not dic['success']:
		return False


ACCESS_KEY = '62752580be819ba497213623997646e9'

URL = 'http://data.fixer.io/api/latest?access_key=' + ACCESS_KEY

r = requests.get(url=URL)
if data_validation(r.json()):
	print(r.json())
