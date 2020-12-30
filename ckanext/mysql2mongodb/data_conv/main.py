from ckanext.mysql2mongodb.data_conv.schema_conversion import SchemaConversion
from ckanext.mysql2mongodb.data_conv.database_connection import ConvInitOption, ConvOutputOption
from ckanext.mysql2mongodb.data_conv.data_conversion import DataConversion
from ckanext.mysql2mongodb.data_conv.utilities import open_connection_mysql
import urllib, json, re, os, requests
from pprint import pprint
from ckanext.mysql2mongodb.data_conv.dag.flow import create_dag

def convert_data(resource_id, sql_file_name, sql_file_url):
	try:
		pprint("Nguyen Xuan Thanh dang o day!")
		create_dag("flow", resource_id, sql_file_name, sql_file_url)
		return True

	except Exception as e:
		pprint(e)
		pprint("Convert fail!")

def read_package_config(file_url = "package_config.txt"):
	try:
		package_conf = {}
		with open(file_url, "r") as f:
			lines = f.readlines()

		for line in lines:
			look_for_conf = re.search("^package_id", line.strip(), re.IGNORECASE)
			if look_for_conf is not None:
				package_conf["package_id"] = re.split(r'[\s]+=[\s]+', line.strip())[1][1:-1]

			look_for_conf = re.search("^X-CKAN-API-Key", line.strip(), re.IGNORECASE)
			if look_for_conf is not None:
				package_conf["X-CKAN-API-Key"] = re.split(r'[\s]+=[\s]+', line.strip())[1][1:-1]

		return package_conf


	except Exception as e:
		pprint(e)
		pprint("Failed while read package config!")

def read_database_config():
	try:
		db_conf = {}
		file_url = "database_config.txt"
		with open(file_url, "r") as f:
			lines = f.readlines()

		for line in lines:
			look_for_conf = re.search("^mysql_host", line.strip(), re.IGNORECASE)
			if look_for_conf is not None:
				db_conf["mysql_host"] = re.split(r'[\s]+=[\s]+', line.strip())[1][1:-1]

			look_for_conf = re.search("^mysql_port", line.strip(), re.IGNORECASE)
			if look_for_conf is not None:
				db_conf["mysql_port"] = re.split(r'[\s]+=[\s]+', line.strip())[1][1:-1]

			look_for_conf = re.search("^mysql_password", line.strip(), re.IGNORECASE)
			if look_for_conf is not None:
				db_conf["mysql_password"] = re.split(r'[\s]+=[\s]+', line.strip())[1][1:-1]

			look_for_conf = re.search("^mysql_username", line.strip(), re.IGNORECASE)
			if look_for_conf is not None:
				db_conf["mysql_username"] = re.split(r'[\s]+=[\s]+', line.strip())[1][1:-1]

			look_for_conf = re.search("^mongodb_host", line.strip(), re.IGNORECASE)
			if look_for_conf is not None:
				db_conf["mongodb_host"] = re.split(r'[\s]+=[\s]+', line.strip())[1][1:-1]

			look_for_conf = re.search("^mongodb_username", line.strip(), re.IGNORECASE)
			if look_for_conf is not None:
				db_conf["mongodb_username"] = re.split(r'[\s]+=[\s]+', line.strip())[1][1:-1]

			look_for_conf = re.search("^mongodb_port", line.strip(), re.IGNORECASE)
			if look_for_conf is not None:
				db_conf["mongodb_port"] = re.split(r'[\s]+=[\s]+', line.strip())[1][1:-1]

			look_for_conf = re.search("^mongodb_password", line.strip(), re.IGNORECASE)
			if look_for_conf is not None:
				db_conf["mongodb_password"] = re.split(r'[\s]+=[\s]+', line.strip())[1][1:-1]

		return db_conf

	except Exception as e:
		print(e)
		print("Failed while reading database config!")