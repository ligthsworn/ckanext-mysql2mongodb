import json
from pprint import pprint
from datetime import date, datetime


from airflow.api.client.local_client import Client


def convert_data(resource_id, sql_file_name, sql_file_url, package_id):
	try:
		conf = buildConf(resource_id, sql_file_name, sql_file_url, package_id)
		runConvert(conf)
		return True

	except Exception as e:
		pprint(e)
		pprint("Convert fail!")

def buildConf(resource_id, sql_file_name, sql_file_url, package_id):
	conf = {"resource_id" : resource_id}
	conf['sql_file_name'] = sql_file_name
	conf['sql_file_url'] = sql_file_url
	conf['package_id'] = package_id
	return conf

def runConvert(conf):
	c = Client(None, None)
	timestamp = datetime.timestamp(datetime.now())
	c.trigger_dag(dag_id='conversion_flow', run_id='conversion_flow'+str(timestamp), conf=json.dumps(conf))