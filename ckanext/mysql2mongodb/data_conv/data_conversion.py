import sys, json, bson, re, time, os
from ckanext.mysql2mongodb.data_conv.schema_conversion import SchemaConversion
from ckanext.mysql2mongodb.data_conv.utilities import open_connection_mysql, open_connection_mongodb, import_json_to_mongodb, extract_dict, store_json_to_mongodb, load_mongodb_collection
# from schema_conversion import SchemaConversion
# from utilities import open_connection_mysql, open_connection_mongodb, import_json_to_mongodb, extract_dict, store_json_to_mongodb, load_mongodb_collection
from bson.decimal128 import Decimal128
from decimal import Decimal
from bson import BSON
from datetime import datetime
from multiprocessing import Pool
from itertools import repeat
import mysql.connector
	
class DataConversion:
	"""
	Data Conversion class is used for:
		- Converting and migrating data from MySQL to MongoDB.
		- Validating if converting is correct, using re-converting method.
	"""

	def __init__(self, resource_id):
		"""
		DataConversion constructor.
		Params:
			- resource_id: Id of uploaded resource to CKAN
		"""
		super(DataConversion, self).__init__()
		self.resource_id = resource_id

	def set_config(self, schema_conv_init_option, schema_conv_output_option, schema):
		"""
		Set up connections from DataConversion to MySQL and MongoDB database.
		Params:
			- schema_conv_init_option: instance of class ConvInitOption, which specified connection to "Input" database (MySQL).
			- schema_conv_output_option: instance of class ConvOutputOption, which specified connection to "Out" database (MongoDB).
			- schema: MySQL schema object which was loaded from MongoDB.
		"""
		self.schema = schema
		self.schema.load_schema()
		self.schema_conv_init_option = schema_conv_init_option
		self.schema_conv_output_option = schema_conv_output_option
		self.validated_dbname = self.schema_conv_init_option.dbname + "_validated"
		self.mongodb_connection_list = self.schema_conv_output_option.get_mongodb_connection_list()
		self.mysql_connection_list = self.schema_conv_init_option.get_mysql_connection_list()


	def run(self):
		"""
		Run all DataConversion operations which include:
			- Migrate data from MySQL to MongoDB.
			- Convert foreign keys from MySQL to document references in MongoDB.
			- Validate if datas from MySQL and MongoDB are matched.
		"""
		tic = time.time()
		self.__migrate_mysql_to_mongodb()
		tac = time.time()
		time_taken = int(tac-tic)
		print(f"Time for migrating MySQL to MongoDB: {time_taken}s")
		
		tic = time.time()
		self.__convert_relations_to_references()
		tac = time.time()
		time_taken = int(tac-tic)
		print(f"Time for converting relations: {time_taken}s")

		tic = time.time()
		self.__validate()
		toc = time.time()
		time_taken = int(toc-tic)
		print(f"Time for validating: {time_taken}s")


	def __migrate_mysql_to_mongodb(self):
		"""
		Migrate data from MySQL to MongoDB.
		"""
		print("Migrating data from MySQL to MongoDB...")
		pool = Pool()
		arg_list = []
		for table in self.schema.get_tables_name_list():
			sql_cmd, step_list = self.__get_info_migrating_table_to_collection(table)
			for step in step_list:
				arg_list = arg_list + [(sql_cmd, table, step)]
		pool.starmap(self.migrate_portion_mysql_to_mongodb, arg_list)
		print("Migrating data from MySQL to MongoDB done!")


	def __get_info_migrating_table_to_collection(self, table_name):
		"""
		Prepare information for parallel migrating data from MySQL table to MongoDB collection.
		"""
		colname_coltype_dict = self.schema.get_table_column_and_data_type()[table_name]
		try:
			db_connection = open_connection_mysql(self.mysql_connection_list, self.schema_conv_init_option.dbname)
			if db_connection.is_connected():

				sql_count = f"SELECT COUNT(*) FROM `{table_name}`;"
				db_cursor = db_connection.cursor()
				db_cursor.execute(sql_count)
				row_count = db_cursor.fetchone()[0]
				db_cursor.close()

				sql_cmd = "SELECT"
				for col_name in colname_coltype_dict.keys():
					dtype = colname_coltype_dict[col_name]
					target_dtype = self.__find_converted_dtype(dtype)

					# Generating SQL for selecting from MySQL Database
					if target_dtype is None:
						raise Exception(f"Data type {dtype} has not been handled!")
					elif target_dtype == "single-geometry":
						sql_cmd = sql_cmd + " ST_AsText(" + col_name + "),"
					else:
						sql_cmd = sql_cmd + " `" + col_name + "`,"

				offset = 0
				max_limit = 150000
				step_list = []
				while offset + 1 < row_count or (offset == 0 and row_count == 1):
					if offset + max_limit + 1 > row_count:
						limit = row_count - offset
					else:
						limit = max_limit
					step_list = step_list + [(offset, limit)]
					offset = offset + limit

				return (sql_cmd, step_list)

			else:
				print("Connect fail!")
		
		except Exception as e:
			print("Error while writing to MongoDB", e)
		
		finally:
			if (db_connection.is_connected()):
				db_connection.close()
				# print("MySQL connection is closed!")

	def migrate_portion_mysql_to_mongodb(self, sql_cmd, table_name, offset_limit):
		"""
		Migrate one portion of data from MySQL table to MongoDB colleciton.
		Params:
			- sql_cmd: SQL for SELECT data from MySQL.
			- offset_limit: offset and limit of migrated portion.
		"""
		offset, limit = offset_limit
		sql_select = sql_cmd[:-1] + f" FROM `{table_name}` LIMIT {offset}, {limit}"
		db_connection = open_connection_mysql(self.mysql_connection_list, self.schema_conv_init_option.dbname)
		db_cursor = db_connection.cursor();
		db_cursor.execute(sql_select)
		fetched_data = db_cursor.fetchall()
		db_cursor.close()
		db_connection.close()
		convert_data_list = self.__convert_mysql_fetched_data_to_mongodb_data(table_name, fetched_data)
		mongodb_connection = open_connection_mongodb(self.mongodb_connection_list)
		store_json_to_mongodb(mongodb_connection, table_name, convert_data_list)
		print(f"Collection {table_name}: {offset} - {offset + limit}")

	def __convert_mysql_fetched_data_to_mongodb_data(self, table_name, fetched_data):
		"""
		Convert data before import to MongoDB
		"""
		colname_coltype_dict = self.schema.get_table_column_and_data_type()[table_name]
		rows = []
		count_blob_text = 1;
		for row in fetched_data:
			data = {}
			col_fetch_seq = list(colname_coltype_dict.keys())
			for i in range(len(col_fetch_seq)):
				col = col_fetch_seq[i]
				dtype = colname_coltype_dict[col]
				target_dtype = self.__find_converted_dtype(dtype)
				#generate SQL
				cell_data = row[i]
				if cell_data != None:
					if dtype in ["INT", "INTEGER", "BIGINT"]:
						converted_data = bson.Int64(cell_data)
					# elif dtype == "BIGINT":
						# print(type(cell_data))
						# return
					elif dtype == "JSON":
							converted_data = json.loads(cell_data)
					elif dtype == "BINARY":
						converted_data = bytes(cell_data)
					elif dtype == "VARBINARY":
						converted_data = bytes(cell_data)
					elif dtype == "DATE":
						converted_data = datetime(cell_data.year, cell_data.month, cell_data.day)#, cell_data.hour, cell_data.minute, cell_data.second)
					elif target_dtype == "decimal":
						converted_data = Decimal128(cell_data)
					elif target_dtype == "object":
						if type(cell_data) is str:
							converted_data = cell_data
						else:
							converted_data = tuple(cell_data)
					elif target_dtype in ["blob", "text"]:
						col_name = list(self.schema.get_table_column_and_data_type()[table_name].keys())[i]
						file_dir_path = f"blob_and_text_file/{self.resource_id}/{table_name}/{col_name}"
						os.system(f"mkdir -p ./{file_dir_path}")
						converted_data = f"{file_dir_path}/{count_blob_text}"
						if target_dtype == "blob":
							# with open(f"./{file_dir_path}/{count_blob_text}", "wb", newline="\n") as out:
							with open(f"./{file_dir_path}/{count_blob_text}", "wb") as out:
								out.write(cell_data)
						else:
							with open(f"./{file_dir_path}/{count_blob_text}", "w", newline="\n") as out:
								out.write(cell_data)

						count_blob_text = count_blob_text + 1
					elif target_dtype == "string":
						if type(cell_data) is bytearray:
							converted_data = cell_data.decode("utf-8")
						else:
							converted_data = str(cell_data)
					else:
						converted_data = cell_data
					data[col_fetch_seq[i]] = converted_data 
			rows.append(data)
		return rows


	def __convert_relations_to_references(self):
		"""
		Convert relations of MySQL table to database references of MongoDB
		"""
		print("Converting relations...")
		tables_relations = self.schema.get_tables_relations()
		relation_list = list(tables_relations.values())

		pool = Pool()
		pool.map(self.convert_one_relation_to_reference, relation_list)		
		print("Converting relations done!")


	def convert_one_relation_to_reference(self, relation_detail):
		"""
		Convert one relation of MySQL table to database reference of MongoDB
		Params:
			- relation_detail: 
				Dict(
					key: <relation uuid>,
					values: Dict(
						"primary_key_table": <primary table name>,
						"foreign_key_table": <foreign table name>,
						"key_columns": List[
							Dict(
								"primary_key_column": <primary column name>,
								"foreign_key_column": <foreign column name>
							)
						]
					)
		"""
		original_collection_name = relation_detail["primary_key_table"] 
		referencing_collection_name = relation_detail["foreign_key_table"]
		key_columns_list = relation_detail["key_columns"]

		db_connection = open_connection_mongodb(self.mongodb_connection_list)
		original_collection_connection = db_connection[original_collection_name]
		finding_field = {}
		for key_pair in key_columns_list:
			finding_field[key_pair["primary_key_column"]] = 1

		original_documents = original_collection_connection.find({}, finding_field)

		new_referenced_key_dict = {}
		for doc in original_documents:
			new_referenced_key_dict[tuple([doc[original_key] for original_key in list(finding_field.keys())])] = doc["_id"]

		ref_keys_list = [key_pair["foreign_key_column"] for key_pair in key_columns_list]
		referencing_key = "_".join(ref_keys_list)
		referencing_key_new_name = "dbref_" + referencing_key
		referencing_documents = db_connection[referencing_collection_name]

		for key in new_referenced_key_dict:
			new_reference = {
				"$ref": original_collection_name,
				"$id": new_referenced_key_dict[key],
				"$db": self.schema_conv_output_option.dbname
			}
			update_query = {}
			for i in range(len(ref_keys_list)):
				update_query[ref_keys_list[i]] = key[i] 
			referencing_documents.update_many(update_query, update={"$set": {referencing_key_new_name: new_reference}})
		print(f"Relation {referencing_key}: done")

	def __validate(self):
		"""
		Convert data from MongoDB back to MySQL, then compare original and validated MySQL databases.
			1 Create validated MySQL database
			2 Define schema for validated database
				2.1 Define tables
				2.2 Define columns of each table
				2.3 Define constraints
			3 Migrate data from MongoDB to MySQL
			4 Define foreign key constraint for validated database
			5 Compare data from original and validated databases, then write result to log file 
		"""
		print("Validating conversion...")
		self.__create_validated_database()
		self.__create_validated_tables()
		self.__migrate_mongodb_to_mysql()
		self.__alter_validated_tables()
		# self.__create_triggers()
		self.__write_validation_log()
		print("Validating conversion done!")

	def __create_validated_database(self):
		"""
		Create validated database.
		"""
		mydb = open_connection_mysql(self.mysql_connection_list)
		mycursor = mydb.cursor()
		mycursor.execute("SHOW DATABASES")
		mysql_table_list = [data[0].decode() if type(data[0]) is bytearray else data[0] for data in mycursor]

		if self.validated_dbname in mysql_table_list:
			mycursor.execute(f"DROP DATABASE {self.validated_dbname}")
		
		mycursor.execute(f"CREATE DATABASE {self.validated_dbname}")
		mycursor.close()
		mydb.close()
		mydb = open_connection_mysql(self.mysql_connection_list, self.validated_dbname)
		print("Creating validated database done!")
		return True

	def __create_validated_tables(self):
		"""
		Create tables of validated database.
		"""
		print("Creating validated tables!")
		table_info_list = self.__get_table_info_list()
		for table_info in table_info_list:
			self.__create_one_table(table_info)
		print("Creating validated tables done!")


	def __create_one_table(self, table_info):
		"""
		Create one table of validated database.
		"""
		columns_info_list = list(filter(lambda column_info: column_info["uuid"] in table_info["columns-uuid-list"], self.__get_columns_info()))
		primary_key_list = list(filter(lambda index_info: index_info["uuid"] == table_info["primary-key-uuid"], self.__get_primary_indexes_info_list())) 
		
		sql_creating_columns_cmd = ",\n".join([self.__generate_sql_creating_column(column_info) for column_info in columns_info_list])

		if len(primary_key_list) > 0:
			primary_key_info = primary_key_list[0]
			sql_creating_key_cmd = self.__generate_sql_creating_key(primary_key_info)
			sql_creating_columns_and_key_cmd = sql_creating_columns_cmd + ",\n" + sql_creating_key_cmd 
		else:
			col_dict = self.schema.get_columns_dict()
			selected_key = table_info["indexes"][0]
			if type(selected_key) is str:
				key_name = col_dict[selected_key]
				sql_creating_key_cmd = f"""KEY `{key_name}` (`{key_name}`)"""
			else:
				col_name_list = [f"`{col_dict[col_uuid]}`" for col_uuid in selected_key["columns"]]
				sql_creating_key_cmd = f"""KEY `{selected_key["name"]}` ({",".join(col_name_list)})"""

		sql_creating_columns_and_key_cmd = sql_creating_columns_cmd + ",\n" + sql_creating_key_cmd 

		sql_creating_table_cmd = f"""CREATE TABLE `{table_info["table-name"]}` (\n{sql_creating_columns_and_key_cmd}\n) ENGINE={table_info["engine"]}"""# DEFAULT CHARSET={table_info["table-collation"]};"""
		# print(sql_creating_table_cmd)
		
		mysql_connection = open_connection_mysql(self.mysql_connection_list, self.validated_dbname)
		mycursor = mysql_connection.cursor()
		mycursor.execute(sql_creating_table_cmd)
		mycursor.close()
		mysql_connection.close()


	def __get_primary_indexes_info_list(self):
		"""
		Get only index on primary keys of tables. 
		Use for defining primary key when creating table.
		Index info:
			Dict(
				key: "uuid", value: <index uuid>,
				key: "table-name", value: <table name>,
				key: "columns-uuid-list", value: <columns uuid list>,
				key: "unique", value: <unique option>,
			)
		"""
		self.schema.load_schema()
		indexes_info_list = []
		for table_schema in self.schema.tables_schema:
			for index in table_schema["indexes"]:
				if type(index) is dict:
					if index["name"] == "PRIMARY":
						index_info = {
							"uuid": index["@uuid"],
							"table-name": index["attributes"]["TABLE_NAME"],
							"columns-uuid-list": index["columns"],
							"unique": index["unique"]
						}
						indexes_info_list.append(index_info)
		return indexes_info_list


	def __generate_sql_creating_key(self, primary_key_info):
		"""
		Generate SQL creating key command from primary key info dict like this:
		Dict(
				key: "uuid", value: <index uuid>,
				key: "table-name", value: <table name>,
				key: "columns-uuid-list", value: <columns uuid list>,
				key: "unique", value: <unique option>,
			)
		"""
		coluuid_colname_dict = self.schema.get_columns_dict()
		columns_in_pk_list = [coluuid_colname_dict[col_uuid] for col_uuid in primary_key_info["columns-uuid-list"]]
		sql_creating_key = f"""PRIMARY KEY ({", ".join(columns_in_pk_list)})"""
		return sql_creating_key


	def __generate_sql_creating_column(self, column_info):
		"""
		Generate SQL command for creating column from this column info dict:
		Dict(
				key: "uuid", value: <column uuid>,
				key: "column-name", value: <column name>,
				key: "table-name", value: <table name>,
				key: "column-type", value: <column type>,
				key: "column-width", value: <column width>,
				key: "nullable", value: <nullable option>,
				key: "default-value", value: <default value>,
				key: "auto-incremented", value: <auto incremented option>,
				key: "character-set-name", value: <character set name>,
				key: "collation-name", value: <collation name>,
			)
		"""
		sql_cmd_list = []
		sql_cmd_list.append(f"""`{column_info["column-name"]}`""")
		# creating_data_type = self.__parse_mysql_data_type(column_info["column-type"], column_info["column-width"])
		sql_cmd_list.append(column_info["column-type"])
		# sql_cmd_list.append(creating_data_type)
		if column_info["character-set-name"] is not None:
			sql_cmd_list.append(f"""CHARACTER SET {column_info["character-set-name"]}""")
		if column_info["collation-name"] is not None:
			sql_cmd_list.append(f"""COLLATE {column_info["collation-name"]}""")
		if column_info["nullable"] is False:
			sql_cmd_list.append("NOT NULL")
		if column_info["default-value"] is not None:
			if column_info["column-type"][:4] == "enum":
				sql_cmd_list.append(f"""default '{column_info["default-value"]}'""")
			else:
				if len(column_info["default-value"]) == 0:
					sql_cmd_list.append(f"""default '{column_info["default-value"]}'""")
				else:
					sql_cmd_list.append(f"""default {column_info["default-value"]}""")
		if column_info["auto-incremented"] is True:
			sql_cmd_list.append("AUTO_INCREMENT")
			# CHARACTER SET utf8mb4 COLLATE utf8mb4_bin
		
		# if column_info["character-set-name"] is not None:
		# 	sql_cmd_list.append(f"""CHARACTER SET {column_info["character-set-name"]}""")
		# if column_info["collation-name"] is not None:
			# sql_cmd_list.append(f"""COLLATE {column_info["collation-name"]}""")

		sql_cmd = " ".join(sql_cmd_list)
		return sql_cmd


	def __migrate_mongodb_to_mysql(self):
		"""
		Migrate data from MongoDB back to MySQL
		"""
		print("Migrating data from MongoDB to MySQL...")
		all_table_relations = list(self.schema.get_tables_relations().values())
		para_args = []
		for collection_name in self.schema.get_tables_name_list()[:]:
			offset_limit, sql, columns_name_list, column_dtype_dict = self.__get_info_migrating_one_collection_to_table(collection_name)
			for step in offset_limit:
				table_relations = list(filter(lambda relation: relation["foreign_key_table"] == collection_name, all_table_relations))
				para_args = para_args + [(step, sql, collection_name, columns_name_list, column_dtype_dict, table_relations)]
		pool = Pool()
		pool.map(self.migrate_portion_mongodb_to_mysql, para_args)
		print("Migrating data from MongoDB to MySQL done!")


	def __get_info_migrating_one_collection_to_table(self, collection_name):
		"""
		Get info of migrating data from one MongoDB collection back to one MySQL table.
		"""
		column_dtype_dict = self.schema.get_table_column_and_data_type()[collection_name]
		col_dict = self.schema.get_columns_dict()
		table_coluuid_list = list(filter(lambda table_schema: table_schema["name"] == collection_name, self.schema.tables_schema))[0]["columns"]
		columns_name_list = [col_dict[col_uuid] for col_uuid in table_coluuid_list]
		columns_num = len(columns_name_list)

		columns_name_sql = ["%s"] * columns_num
		cols_info = list(filter(lambda col_inf: col_inf["table-name"] == collection_name, self.__get_columns_info()))
		for i in range(len(columns_name_sql)):
			for col_info in cols_info:
				if col_info["column-name"] == columns_name_list[i]:
					if col_info["column-type"][:8] == "geometry":
						columns_name_sql[i] = f"ST_GeomFromText({columns_name_sql[i]})"
						break

		sql = f"""INSERT INTO `{collection_name}` ({", ".join([f"`{column_name}`" for column_name in columns_name_list])}) VALUES ({", ".join(columns_name_sql)})"""
		

		mongodb_connection = open_connection_mongodb(self.mongodb_connection_list)
		collection = mongodb_connection[collection_name]

		offset = 0
		max_limit = 200000
		doc_count = collection.find().count()
		offset_limit = []
		while offset + 1 < doc_count or (offset == 0 and doc_count == 1):
			if offset + max_limit + 1 > doc_count:
				limit = doc_count - offset
			else:
				limit = max_limit
			offset_limit = offset_limit + [(offset, limit)]
			offset = offset + limit
		# print(offset_limit)
		return (offset_limit, sql, columns_name_list, column_dtype_dict)

	def migrate_portion_mongodb_to_mysql(self, args):
		"""
		Migrate one portion of data from MongoDB collection to MySQL table.
		"""
		step, sql, collection_name, columns_name_list, column_dtype_dict, table_relations = args
		offset, limit = step
		mongodb_connection = open_connection_mongodb(self.mongodb_connection_list)
		collection = mongodb_connection[collection_name]
		datas = collection.find().skip(offset).limit(limit)

		mysql_fk_set = set()
		referenced_datas = {}
		collection_connection = {}
		dbref_dict = {}
		finding_fields = {}

		for relation in table_relations:
			collection_connection[relation["primary_key_table"]] = mongodb_connection[relation["primary_key_table"]]
			referenced_datas[relation["primary_key_table"]] = {}
			finding_fields[relation["primary_key_table"]] = {"_id": 0}
			for key_pair in relation["key_columns"]:
				mysql_fk_set.add(key_pair["foreign_key_column"])

		for relation in table_relations:
			pk_list = [key_pair["primary_key_column"] for key_pair in relation["key_columns"]]
			fk_list = [key_pair["foreign_key_column"] for key_pair in relation["key_columns"]]
			dbref_field_name = "dbref" + "_" + "_".join(fk_list)
			for key_pair in relation["key_columns"]:
				dbref_dict[key_pair["foreign_key_column"]] = (key_pair["primary_key_column"], dbref_field_name)
				finding_fields[relation["primary_key_table"]][key_pair["primary_key_column"]] = 1

		val = []
		for data in datas:
			row = []
			for key in columns_name_list:
				if key in mysql_fk_set:
					if key in data: ### If data[key] is not null in MongoDB
						primary_key, dbref_name = dbref_dict[key]
							# print(1) 
						dbref = data[dbref_name]
						primary_table = dbref._DBRef__collection
						dbref_id = dbref._DBRef__id
						if dbref_id in referenced_datas[primary_table]:
							cell_data = referenced_datas[primary_table][dbref_id][primary_key]
						else:
							conn = collection_connection[primary_table]
							primary_data = conn.find_one({"_id": dbref_id}, finding_fields[primary_table])
							for pk in primary_data:
								if type(primary_data[pk]) is bson.Int64:
									primary_data[pk] = int(primary_data[pk])
							referenced_datas[primary_table][dbref_id] = primary_data
							cell_data = primary_data[primary_key]
					else:
						cell_data = None
				elif key in data.keys():
					dtype = type(data[key])
					if self.__find_converted_dtype(column_dtype_dict[key]) == "text":
						with open(data[key], newline="\n") as f:
							cell_data = f.read()
					elif self.__find_converted_dtype(column_dtype_dict[key]) == "blob":
						with open(data[key], "rb") as f:
							cell_data = f.read()
							# print(cell_data)
					elif dtype is bson.Int64:
						cell_data = int(data[key])
					elif dtype is Decimal128:
						cell_data = data[key].to_decimal()
					elif dtype is list:
						cell_data = ",".join(data[key])
					elif dtype is dict: #JSON
						cell_data = json.dumps(data[key])
					else:
						cell_data = data[key]
				else:
					cell_data = None
				row.append(cell_data)
			val.append(row)

		mysql_connection = open_connection_mysql(self.mysql_connection_list, self.validated_dbname)
		mycursor = mysql_connection.cursor()
		mycursor.executemany(sql, val)
		mysql_connection.commit()
		mycursor.close()
		mysql_connection.close()
		print(f"Table {collection_name}: {offset} - {offset + limit}")


	def __alter_validated_tables(self):
		"""
		Alter schema of tables in validated database.
		- Add foreign key constraint.
		"""
		for table_info in self.__get_table_info_list():
			self.__alter_one_table(table_info)

	def __alter_one_table(self, table_info):
		"""
		Alter schema of one table in validated database.
		"""
		mysql_connection = open_connection_mysql(self.mysql_connection_list, self.validated_dbname)
		fk_constraints_list = self.__get_table_constraint_info_list(table_info["uuid"])
		sql_creating_fk_cmd = ",\n".join(self.__generate_sql_foreign_keys_list(fk_constraints_list))
		if len(sql_creating_fk_cmd) > 0:
			sql_altering_table_cmd = f"""ALTER TABLE {table_info["table-name"]} {sql_creating_fk_cmd};"""
			mycursor = mysql_connection.cursor()
			mycursor.execute(sql_altering_table_cmd)
			mycursor.close()
		mysql_connection.close()


	def __get_table_constraint_info_list(self, table_uuid):
		"""
		Get constraint info list from schema
		List[<foreign key info>]
		"""
		table_constraints_info = []
		foreign_key_list = self.__get_foreign_keys_list()
		self.schema.load_schema()
		table_schema = list(filter(lambda table_schema: table_schema["@uuid"] == table_uuid, self.schema.tables_schema))[0]
		table_constraints_list = list(filter(lambda tbl_constr: type(tbl_constr) is dict, table_schema["table-constraints"]))
		table_constraint_name_list = list(map(lambda tbl_constr: tbl_constr["name"], table_constraints_list))
		res = list(filter(lambda fk_info: fk_info["name"] in table_constraint_name_list, foreign_key_list))
		return res


	def __get_foreign_keys_list(self):
		"""
		Get list of foreign keys info list
		List[
			Dict(
				"uuid": <fk uuid>,
				"name": <fk name>,
				"column-references": List[
					Dict(
						"fk-uuid": <foreign key column uuid>,
						"pk-uuid": <primary key column uuid>,
					)],
				"delete-rule": <delete rule>,
				"update-rule": <update rule>
			)
		]
		"""
		self.schema.load_schema()
		fk_keys_list = []
		for table_schema in self.schema.tables_schema:
			for fk_schema in table_schema["foreign-keys"]:
				if type(fk_schema) is dict:
					col_refs = []
					for col_ref in fk_schema["column-references"]:
						col_refs.append({
							"fk-uuid": col_ref["foreign-key-column"],
							"pk-uuid": col_ref["primary-key-column"],
							})
					fk_info = {
						# "uuid": fk_schema["@uuid"],
						"name": fk_schema["name"],
						"delete-rule": fk_schema["delete-rule"],
						"update-rule": fk_schema["update-rule"],
						"column-references": col_refs
					}
					fk_keys_list.append(fk_info)
		return fk_keys_list


	def __write_validation_log(self):
		"""
		Write validation log to file.
		Logs contain:
			Dict(
				<key>: <table name>,
				<value>: Dict(
					schema: <schema log>,
					data: <data log>
				)
			)
		"""
		print("Writing log...")
		
		writing_data = {}

		mongodb_conn = open_connection_mongodb(self.mongodb_connection_list)
		all_database_count = {}
		all_database_count.update({f"mysql_{self.schema_conv_init_option.dbname}": self.__mysql_count_row(self.schema_conv_init_option.dbname)})
		all_database_count.update({f"mysql_{self.validated_dbname}": self.__mysql_count_row(self.validated_dbname)})

		mongo_count = {}
		for table_name in self.schema.get_tables_name_list():
			collection = mongodb_conn[table_name]
			mongo_count.update({table_name: collection.find().count()})
		all_database_count.update({f"mongo_{self.schema_conv_output_option.dbname}": mongo_count})
		writing_data.update({"row-count": all_database_count})
		
		conversion_log = {}
		pool = Pool()
		log_list = pool.map(self.prepare_one_table_log, self.schema.get_tables_name_list())

		for log_item in log_list:
			conversion_log.update(log_item)

		writing_data.update({"conversion-log": conversion_log})
		with open(f"./conversion_log/{self.resource_id}/log.json", 'w') as f:
			json.dump(writing_data, f, indent=4)

		print("Writing log done!")


	def prepare_one_table_log(self, table_name):
		"""
		Prepare log data for writing log.
		"""
		table_columns_list = self.schema.get_table_column_and_data_type()
		mysql_conn = open_connection_mysql(self.mysql_connection_list)
		mysql_cur = mysql_conn.cursor()

		schema_validating_sql = f"""
			SELECT column_name,ordinal_position,data_type,column_type FROM
			(
			    SELECT
			        column_name,ordinal_position,
			        data_type,column_type,COUNT(1) rowcount
			    FROM information_schema.columns
			    WHERE
			    (
			        (table_schema='{self.schema_conv_init_option.dbname}' AND table_name='{table_name}') OR
			        (table_schema='{self.validated_dbname}' AND table_name='{table_name}')
			    )
			    GROUP BY
			        column_name,ordinal_position,
			        data_type,column_type
			    HAVING COUNT(1)=1
			) A;
		"""

		log_data = {}

		mysql_cur.execute(schema_validating_sql)
		schema_validating_data = mysql_cur.fetchall()
		res_schema = [tuple(str(ele) for ele in tup) for tup in schema_validating_data]
		log_data["schema"] = res_schema
		key_list = self.schema.get_primary_key_dict()[table_name]
		sql_order = ",".join(key_list)

		columns_of_table = list(table_columns_list[table_name])
		columns_sql = ",".join([f"`{col_name}`" for col_name in columns_of_table])

		dtype_dict = self.schema.get_table_column_and_data_type()
		sql_selected_columns = ",".join([f"CAST(`{col_name}` AS CHAR) AS `{col_name}`" if dtype_dict[table_name][col_name] == "FLOAT" else f"`{col_name}`" for col_name in columns_of_table])

		offset = 0
		max_limit = 200000
		row_count = self.__mysql_count_row(self.schema_conv_init_option.dbname)[table_name]
		log_data["data"] = []
		while offset + 1 < row_count or (offset == 0 and row_count == 1):
			if offset + max_limit + 1 > row_count:
				limit = row_count - offset
			else:
				limit = max_limit
			
			data_validating_sql = f"""
				select {columns_sql}
				from
				(
					(SELECT {sql_selected_columns} FROM {self.schema_conv_init_option.dbname}.`{table_name}` ORDER BY {sql_order} LIMIT {offset}, {limit})
					union all
					(SELECT {sql_selected_columns} FROM {self.validated_dbname}.`{table_name}` ORDER BY {sql_order} LIMIT {offset}, {limit})
				) as C
				group by {columns_sql}
				having count(*) = 1
			"""
			mysql_cur.execute(data_validating_sql)
			data_validating_data = mysql_cur.fetchall()
			res_data = [tuple(str(ele) for ele in tup) for tup in data_validating_data]
			log_data["data"] = log_data["data"] + res_data

			offset = offset + limit
		
		mysql_cur.close()
		mysql_conn.close()
		
		return {table_name: log_data}


	def __get_columns_info(self):
		"""
		List[
			Dict(
				key: "uuid", value: <column uuid>,
				key: "column-name", value: <column name>,
				key: "table-name", value: <table name>,
				key: "column-type", value: <column type>,
				key: "auto-incremented", value: <auto incremented option>,
				key: "nullable", value: <nullable option>,
				key: "default-value", value: <default value>,
				key: "column-width", value: <column width>,
				key: "character-set-name", value: <character set name>,
				key: "collation-name", value: <collation name>,
			)
		]
		"""
		columns_info_list = []
		for column_schema in self.schema.all_table_columns:
			table_name= column_schema["short-name"].split(".")[0]
			column_info = {
				"uuid": column_schema["@uuid"],
				"column-name": column_schema["name"],
				"table-name": table_name,
				"column-type": column_schema["attributes"]["COLUMN_TYPE"],
				"character-set-name": column_schema["attributes"]["CHARACTER_SET_NAME"],
				"collation-name": column_schema["attributes"]["COLLATION_NAME"],
				"auto-incremented": column_schema["auto-incremented"],
				"nullable": column_schema["nullable"],
				"default-value": column_schema["default-value"],
				"column-width": column_schema["width"],
			}
			columns_info_list.append(column_info)
		return columns_info_list


	def __generate_sql_foreign_keys_list(self, fk_info_list):
		"""
		Return list of SQL command for creating foreign key constraints
		Foreign key info like this:
		Dict(
				"name": <fk name>,
				"column-references": List[
					Dict(
						"fk-uuid": <foreign key column uuid>,
						"pk-uuid": <primary key column uuid>,
					)],
				"delete-rule": <delete rule>,
				"update-rule": <update rule>
			)
		"""
		coluuid_colname_dict = self.schema.get_columns_dict()
		coluuid_tablename_dict = self.schema.get_tables_dict()
		sql_fk_cmd_list = []
		for fk_info in fk_info_list:
			col_ref = fk_info["column-references"][0]
			fk_col = coluuid_colname_dict[col_ref["fk-uuid"]]
			pk_col = coluuid_colname_dict[col_ref["pk-uuid"]]
			pk_tabl = coluuid_tablename_dict[col_ref["pk-uuid"]]
			sql_fk_cmd_list.append(f"""ADD CONSTRAINT {fk_info["name"]} FOREIGN KEY ({fk_col}) REFERENCES {pk_tabl} ({pk_col}) ON DELETE {fk_info["delete-rule"]} ON UPDATE {fk_info["update-rule"]}""")
		return sql_fk_cmd_list

	def __parse_mysql_data_type(self, dtype, width):
		"""
		Generate MySQL data type for creating column SQL command.
		Params:
			-dtype: MySQL data type and unsigned option
			-width: length of column
		"""
		mysql_type_list = [
			"ascii", 
			"bigint",
			"binary", 
			"bit", 
			"blob", 
			"boolean",
			"bool", 
			"character", 
			"charset", 
			"char", 
			"datetime", 
			"date", 
			"decimal", 
			"dec", 
			"double", 
			"enum", 
			"fixed",
			"float", 
			"geometrycollection",
			"geometry", 
			"integer", 
			"int", 
			"json"
			"linestring", 
			"longblob",
			"longtext",
			"mediumblob", 
			"mediumint", 
			"mediumtext", 
			"multilinestring", 
			"multipoint", 
			"multipolygon", 
			"point", 
			"polygon",
			"real",
			"set",
			"smallint", 
			"text", 
			"timestamp", 
			"time",
			"tinyblob", 
			"tinyint", 
			"tinytext", 
			"unicode", 
			"varbinary",
			"varchar", 
			"year",
		]
		for mysql_type in mysql_type_list:
			if re.search(f"^{mysql_type}", dtype):
				# Handle enum data type
				if mysql_type == "enum":
					return dtype
				elif mysql_type == "set":
					return dtype
				elif mysql_type == "json":
					return dtype
				else:
					if bool(re.search(f"unsigned$", dtype)):
						unsigned = " unsigned"
					else:
						unsigned = ""
					res = mysql_type + width + unsigned
					return res
		print(dtype, width)
		return None

	def __get_table_info_list(self):
		"""
		List[
			Dict(
				key: "uuid", value: <table uuid>,
				key: "table-name", value: <table name>,
				key: "engine", value: <engine>,
				key: "table-collation", value: <table collation>,
				key: "columns-uuid-list", value: <List of columns uuid>,
				key: "primary-key-uuid", value: <primary key uuid>,

			)
		]
		Not be handled yet. Will be handled in next phase:
				key: "foreign key", value: ???,
				key: "table-constraints", value: ???,
				key: "indexes", value: ???, ### may be neccesary or not, because primary key is auto indexed
		"""
		table_info_list = []
		for table_schema in self.schema.tables_schema:
			if self.__get_table_type(table_schema["table-type"]) == "TABLE":
				table_info = {
					"uuid": table_schema["@uuid"],
					"table-name": table_schema["name"],
					"engine": table_schema["attributes"]["ENGINE"],
					# "table-collation": table_schema["attributes"]["TABLE_COLLATION"].split("_")[0],
					"columns-uuid-list": table_schema["columns"],
					"primary-key-uuid": table_schema["primary-key"],
					"indexes": table_schema["indexes"]
				}
				table_info_list.append(table_info)
		return table_info_list

	def __get_table_type(self, table_type):
		"""
		Define table type is TABLE or VIEW.
		Parameter:
			-table_type: table type which was get from schema, either be object or string
		"""
		# Dict(key: <table type uuid>, value: <table type>)
		if type(table_type) is dict:
			return table_type["table-type"]
		else:
			table_type_dict = {}
			self.schema.load_schema()
			for table_schema in self.schema.tables_schema:
				if type(table_schema["table-type"]) is dict:
					table_type_dict[table_schema["table-type"]["@uuid"]] = table_schema["table-type"]["table-type"]
			return table_type_dict[table_type]


	def __mysql_count_row(self, dbname):
		"""
		Get row count number of tables in MySQL database.
		"""
		mysql_conn = open_connection_mysql(self.mysql_connection_list, dbname)
		row_count = {}
		mysql_cur = mysql_conn.cursor()
		for table_name in self.schema.get_tables_name_list():
			mysql_cur.execute(f"SELECT COUNT(*) FROM `{table_name}`")
			num_of_rows = mysql_cur.fetchone()[0]
			row_count[table_name] = num_of_rows
		mysql_cur.close()
		mysql_conn.close()
		return row_count	
		

	def __find_converted_dtype(self, mysql_dtype):
		"""
		Mapping data type from MySQL to MongoDB.
		Just use this function for migrate_mysql_to_mongodb function
		"""
		mongodb_dtype = {
			"integer": "integer",
			"decimal": "decimal",
			"double": "double",
			"boolean": "boolean",
			"date": "date",
			"timestamp": "timestamp",
			"binary": "binary",
			"blob": "blob",
			"text": "text",
			"string": "string",
			"object": "object",
			"single-geometry": "single-geometry",
			"multiple-geometry": "multiple-geometry",
		}
		
		dtype_dict = {
			mongodb_dtype["integer"]: ["TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT"],
			mongodb_dtype["decimal"]: ["DECIMAL", "DEC", "FIXED"],
			mongodb_dtype["double"]: ["FLOAT", "DOUBLE", "REAL"],
			mongodb_dtype["boolean"]: ["BOOL", "BOOLEAN"],
			mongodb_dtype["date"]: ["DATE", "YEAR"],
			mongodb_dtype["timestamp"]: ["DATETIME", "TIMESTAMP", "TIME"],
			mongodb_dtype["binary"]: ["BIT", "BINARY", "VARBINARY"],
			mongodb_dtype["blob"]: ["TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB"],
			mongodb_dtype["text"]: ["TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT"],
			mongodb_dtype["string"]: ["CHARACTER", "CHARSET", "ASCII", "UNICODE", "CHAR", "VARCHAR"],
			mongodb_dtype["object"]: ["ENUM", "SET", "JSON"],
			mongodb_dtype["single-geometry"]: ["GEOMETRY", "POINT", "LINESTRING", "POLYGON"],
			mongodb_dtype["multiple-geometry"]: ["MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION"]
		}

		for target_dtype in dtype_dict.keys():
			if(mysql_dtype) in dtype_dict[target_dtype]:
				return mongodb_dtype[target_dtype]
		return None 