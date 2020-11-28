from ckanext.mysql2mongodb.data_conv.schema_conversion import SchemaConversion
from ckanext.mysql2mongodb.data_conv.database_connection import ConvInitOption, ConvOutputOption
from ckanext.mysql2mongodb.data_conv.data_conversion import DataConversion
import urllib, json, pprint, re, os, requests


def data_conv(resource_id, sql_file_name, sql_file_url):
	os.chdir("/usr/lib/ckan/default/src/ckanext-mysql2mongodb/ckanext/mysql2mongodb/data_conv")
	# os.system(f"mkdir -p ./downloads/{resource_id}")
	# os.system(f"curl -o ./downloads/{resource_id}/{sql_file_name} {sql_file_url}")
	schema_name = sql_file_name.split(".")[0]
	# pprint.pprint("set_config")
	# schema_name = "sakila"
	os.system(f"mysql -u dangsg -p Db@12345678 {schema_name} < ./downloads/{resource_id}/{sql_file_name}")

	mysql_host = 'localhost'
	mysql_username = 'dangsg'
	mysql_password = 'Db@12345678'
	mysql_port = '3306'
	mysql_dbname = schema_name
	schema_conv_init_option = ConvInitOption(host = mysql_host, username = mysql_username, password = mysql_password, port = mysql_port, dbname = mysql_dbname)

	mongodb_host = 'localhost'
	mongodb_username = 'dangsg'
	mongodb_password = 'Db@12345678'
	mongodb_port = '27017'
	mongodb_dbname = schema_name
	schema_conv_output_option = ConvOutputOption(host = mongodb_host, username = mongodb_username, password = mongodb_password, port = mongodb_port, dbname = mongodb_dbname)

	schema_conversion = SchemaConversion()
	schema_conversion.set_config(schema_conv_init_option, schema_conv_output_option)
	schema_conversion.run()

	mysql2mongodb = DataConversion()
	mysql2mongodb.set_config(schema_conv_init_option, schema_conv_output_option, schema_conversion)
	mysql2mongodb.run()

	os.system(f"mkdir -p mongodump_files")
	os.system(f"mongodump --username {mongodb_username} --password {mongodb_password} --authenticationDatabase admin --db {mongodb_dbname} -o mongodump_files/")
	os.chdir("./mongodump_files")
	os.system(f"zip -r {schema_name}.zip {schema_name}/*")

	requests.post('http://localhost:5000/api/action/resource_create',
          data={"package_id":"hoang_dang_data_set", "name":f"{schema_name}.zip"},
          headers={"X-CKAN-API-Key": "589a95e9-89fa-4c71-a186-1ae30f014ed3"},
          files={'upload': open(f"{schema_name}.zip", 'rb')})

	pprint.pprint("Done!")
	return True
	# pass
#!/usr/bin/env python

# def data_conv():
	# pass
	# # Put the details of the dataset we're going to create into a dict.
	# dataset_dict = {
	#     'name': 'my_dataset_name',
	#     'notes': 'A long description of my dataset',
	#     'owner_org': 'org_id_or_name'
	# }

	# # Use the json module to dump the dictionary to a string for posting.
	# data_string = urllib.quote(json.dumps(dataset_dict))

	# # We'll use the package_create function to create a new dataset.
	# request = urllib2.Request(
	#     'http://www.my_ckan_site.com/api/action/package_create')

	# # Creating a dataset requires an authorization header.
	# # Replace *** with your API key, from your user account on the CKAN site
	# # that you're creating the dataset on.
	# request.add_header('Authorization', '***')

	# # Make the HTTP request.
	# response = urllib2.urlopen(request, data_string)
	# assert response.code == 200

	# # Use the json module to load CKAN's response into a dictionary.
	# response_dict = json.loads(response.read())
	# assert response_dict['success'] is True

	# # package_create returns the created package as its result.
	# created_package = response_dict['result']
	# pprint.pprint(created_package)


if __name__ == '__main__':
	data_conv("66d52da5-1b0d-4de7-9890-9fab9e64ddda", "sakila.sql", None)