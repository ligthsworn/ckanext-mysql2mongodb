from ckanext.mysql2mongodb.data_conv.schema_conversion import SchemaConversion
from ckanext.mysql2mongodb.data_conv.database_connection import ConvInitOption, ConvOutputOption
from ckanext.mysql2mongodb.data_conv.data_conversion import DataConversion
import urllib, json, re, os, requests
from pprint import pprint


def convert_data(resource_id, sql_file_name, sql_file_url):
	try:
		pprint("Start conversion!")
		if sql_file_name.split(".")[1] != "sql":
			print("Invalided MySQL backup file extension!")
			raise Exception()

		os.chdir("/usr/lib/ckan/default/src/ckanext-mysql2mongodb/ckanext/mysql2mongodb/data_conv")
		os.system(f"mkdir -p ./downloads/{resource_id}")
		os.system(f"mkdir -p ./blob_and_text_file/{resource_id}")
		os.system(f"curl -o ./downloads/{resource_id}/{sql_file_name} {sql_file_url}")

		db_conf = read_database_config()
		package_conf = read_package_config()

		schema_name = sql_file_name.split(".")[0]

		mysql_host = db_conf["mysql_host"]
		mysql_username = db_conf["mysql_username"]
		mysql_password = db_conf["mysql_password"]
		mysql_port = db_conf["mysql_port"]
		mysql_dbname = schema_name
		
		os.system(f"mysql -u {mysql_username} -p {mysql_password} {schema_name} < ./downloads/{resource_id}/{sql_file_name}")
		

		
		schema_conv_init_option = ConvInitOption(host = mysql_host, username = mysql_username, password = mysql_password, port = mysql_port, dbname = mysql_dbname)

		mongodb_host = db_conf["mongodb_host"]
		mongodb_username = db_conf["mongodb_username"]
		mongodb_password = db_conf["mongodb_password"]
		mongodb_port = db_conf["mongodb_port"]
		mongodb_dbname = schema_name
		schema_conv_output_option = ConvOutputOption(host = mongodb_host, username = mongodb_username, password = mongodb_password, port = mongodb_port, dbname = mongodb_dbname)

		schema_conversion = SchemaConversion()
		schema_conversion.set_config(schema_conv_init_option, schema_conv_output_option)
		schema_conversion.run()

		mysql2mongodb = DataConversion(resource_id)
		mysql2mongodb.set_config(schema_conv_init_option, schema_conv_output_option, schema_conversion)
		mysql2mongodb.run()

		os.system(f"mkdir -p mongodump_files/{resource_id}")
		os.system(f"mongodump --username {mongodb_username} --password {mongodb_password} --authenticationDatabase admin --db {mongodb_dbname} -o mongodump_files/{resource_id}/")
		# os.chdir(f"./mongodump_files")
		os.system(f"mkdir -p uploads/{resource_id}")
		os.system(f"cp -r mongodump_files/{resource_id}/{schema_name} uploads/{resource_id}/{schema_name}")
		os.system(f"cp -r blob_and_text_file/{resource_id} uploads/{resource_id}/blob_and_text_file")
		os.chdir(f"./uploads/{resource_id}")
		os.system(f"zip -r {schema_name}.zip *")

		response = requests.post('http://localhost:5000/api/action/resource_create',
	          data={"package_id":package_conf["package_id"], "name":f"{schema_name}-{resource_id}.zip"},
	          headers={"X-CKAN-API-Key": package_conf["X-CKAN-API-Key"]},
	          files={'upload': open(f"{schema_name}.zip", 'rb')})

		pprint(response.content)

		pprint("Done!")
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