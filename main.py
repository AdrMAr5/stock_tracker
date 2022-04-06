from datetime import datetime, timedelta

from prefect import task, Flow
from prefect.schedules import IntervalSchedule

from stock_tasks import *


def build_flow(schedule=None):
	with Flow('stock_tracker_etl', schedule=schedule) as flow:
		r = send_request()
		data = parse_data(r)
		append_to_db(data)

	return flow


schedule = IntervalSchedule(interval=timedelta(hours=12))

flow = build_flow(schedule)
flow.run()




