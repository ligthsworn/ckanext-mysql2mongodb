# utilities.py: Utitilies functions which are used for conversion processes.

import mysql.connector
from pymongo import MongoClient
import json 
import sys
import urllib.parse
 		 
def extract_dict(selected_keys):
	"""
	Extract selected-by-key fields from dict.
	This function is used as iteration of Python map() function.
	"""
	def extract_dict(input_dict):
		output_dict = {}
		for key in selected_keys:
			output_dict[str(key)] = input_dict[str(key)]
		return output_dict
	return extract_dict
  
def import_json_to_mongodb(db_connection, collection_name, dbname, json_filename):
	"""
	Import intermediate JSON file, which was generated and saved at path "./intermediate/<database-name>", into MongoDB.
	Parameter:
	"""
	try:		   
		Collection = db_connection[collection_name]
		with open(f"./intermediate_data/{dbname}/{json_filename}") as file: 
		    file_data = json.load(file) 
		    table_data = file_data
		if isinstance(table_data, list): 
			Collection.insert_many(table_data)   
		else: 
			Collection.insert_one(table_data) 
		return True
		# print(f"Write data from JSON file {json_filename} to MongoDB collection {collection_name} done!") 
	except Exception as e:
		print(f"Error while writing JSON file {json_filename} to MongoDB collection {collection_name}!!!")
		print(e)
		raise e

def store_json_to_mongodb(mongodb_connection, collection_name, json_data):
	"""
	Import data from JSON object (not from JSON file).
	"""
	try:		   
		Collection = mongodb_connection[collection_name]
		if isinstance(json_data, list):
			if len(json_data) > 0: 
			    Collection.insert_many(json_data)   
		else: 
		    Collection.insert_one(json_data)
		# print(f"Write JSON data to MongoDB collection {collection_name} done!") 
		return True
	except Exception as e:
		print(f"Error while writing data to MongoDB collection {collection_name}!!!")
		print(e)
		raise e

def drop_mongodb_database(connection_info_list):
	"""
	Drop MongoDB database.
	Be useful, just use this function at the begining of conversion.
	"""
	host, username, password, port, dbname = connection_info_list
	username = urllib.parse.quote_plus(username)
	password = urllib.parse.quote_plus(password)
	connection_string = f"mongodb://{username}:{password}@{host}:{port}/"
	try:
		mongo_client = MongoClient(connection_string)  
		mongo_client.drop_database(dbname)
		return True
	except Exception as e:
		print(f"Error while dropping MongoDB database {dbname}! Re-check connection or name of database.")
		print(e)
		raise e

def open_connection_mongodb(connection_info_list):
	"""
	Set up a connection to MongoDB database.
	Return a MongoClient object if success.
	mongodb://myDBReader:D1fficultP%40ssw0rd@mongodb0.example.com:27017/?authSource=admin
	"""
	host, username, password, port, dbname = connection_info_list
	username = urllib.parse.quote_plus(username)
	password = urllib.parse.quote_plus(password)
	connection_string = f"mongodb://{username}:{password}@{host}:{port}/"
	try:
		# Making connection 
		mongo_client = MongoClient(connection_string)  
		# Select database  
		db_connection = mongo_client[dbname] 		
		return db_connection
	except Exception as e:
		print(f"Error while connecting to MongoDB database {dbname}! Re-check connection or name of database.")
		print(e)
		raise e

def load_mongodb_collection(connection_info_list):
	"""
	Load all documents from MongoDB collection.
	"""
	host, username, password, port, dbname, collection_name = connection_info_list
	mongodb_connection = open_connection_mongodb(connection_info_list[0:5])
	collection = mongodb_connection[collection_name]
	docs = collection.find()
	res = [doc for doc in docs]
	return res

def open_connection_mysql(connection_info_list, dbname = None):
	"""
	Set up a connection to MySQL database.
	Return a MySQL (connector) connection object if success, otherwise None.
	"""
	try:
		host, username, password = connection_info_list
		db_connection = mysql.connector.connect(
			host = host, 
			user = username, 
			password = password, 
			database = dbname,
			auth_plugin='mysql_native_password'
		)
		if db_connection.is_connected():
			db_info = db_connection.get_server_info()
			return db_connection
		else:
			print("Connect to MySQL server fail!")
			return None
	except Exception as e:
		print(f"Error while connecting to MySQL database {dbname}! Re-check connection or name of database.")
		print(e)
		raise e
