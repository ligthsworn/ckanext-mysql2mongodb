import json, os, re
# from collections import OrderedDict
from ckanext.mysql2mongodb.data_conv.utilities import extract_dict, import_json_to_mongodb, open_connection_mongodb, open_connection_mysql, drop_mongodb_database, load_mongodb_collection, store_json_to_mongodb
from utilities import extract_dict, import_json_to_mongodb, open_connection_mongodb, open_connection_mysql, drop_mongodb_database, load_mongodb_collection, store_json_to_mongodb
	
class SchemaConversion:
	"""
	Schema Conversion class is used for:
		- Extracting schema from MySQL database and store in MongoDB. 
		- Converting MySQL schema into readable one and store in MongoDB.  
		- Creating MongoDB schema validators based on MySQL schema.
	All above processes are belong to phase "Schema Conversion".
	The schema operation utilities was also defined in this class.
	"""

	def __init__(self):
		"""
		SchemaConversion constructor.
		"""
		super(SchemaConversion, self).__init__()
		# Define a name for schema file which will be place at intermediate directory for later uses.
		self.schema_filename = "schema.json"

	def set_config(self, schema_conv_init_option, schema_conv_output_option):
		"""
		Set up connections from SchemaConversion to MySQL and MongoDB database.
		Parameters:
			- schema_conv_init_option:_ instance of class ConvInitOption, which specified connection to "Input" database (MySQL).
			- schema_conv_output_option: instance of class ConvOutputOption, which specified connection to "Output" database (MongoDB).
		"""
		self.schema_conv_init_option = schema_conv_init_option
		self.schema_conv_output_option = schema_conv_output_option
		self.mongodb_connection_list = self.schema_conv_output_option.get_mongodb_connection_list()
		self.mysql_connection_list = self.schema_conv_init_option.get_mysql_connection_list()
		print("Set up database connections config done!")

	def load_schema(self):
		"""
		Load schema from MongoDB into object "self".
		"""
		if not hasattr(self, "db_schema"):
			db_schema = load_mongodb_collection(self.mongodb_connection_list + ["schema"])
			# Store the most used variables into self 
			self.db_schema = db_schema[0]
			self.all_table_columns = self.db_schema["all-table-columns"]
			self.tables_schema = [] 
			for table_schema in self.db_schema["catalog"]["tables"]:
				if type(table_schema) is dict:
					self.tables_schema.append(table_schema)
					if "table-usage" in table_schema:
						for table_usage in table_schema["table-usage"]:
							if type(table_usage) is dict:
								self.tables_schema.append(table_usage)
		return True

	def run(self):
		"""
		Run all SchemaConversion operations which include:
			- Extract MySQL schema (to JSON file)
			- Store MySQL schema (JSON file) in collection "schema" of MongoDB
			- Convert schema into readable one and store in collection "schema_view" of MongoDB 
			- Create MongoDB Schema Validators
		"""
		self.__drop_mongodb()
		self.__extract_mysql_schema()
		self.__save_schema()
		self.load_schema()
		self.__save_schema_view()
		self.__create_mongo_schema_validators()
		self.__create_mongo_indexes()
		self.__drop_view()
		print("SchemaConversion done!")
		return True

	def __drop_mongodb(self):
		"""
		Drop a MongoDB database if existed.
		"""
		drop_mongodb_database(self.mongodb_connection_list)

	def __extract_mysql_schema(self):
		"""
		Extract MySQL schema using SchemaCrawler, then save as JSON file at intermediate directory.
		"""
		command_create_intermediate_dir = f"mkdir -p ./intermediate_data/{self.schema_conv_init_option.dbname}"
		os.system(command_create_intermediate_dir)

		command_generate_mysql_schema = f"_schemacrawler/schemacrawler.sh \
		--server=mysql \
		--host={self.schema_conv_init_option.host} \
		--port={self.schema_conv_init_option.port} \
		--database={self.schema_conv_init_option.dbname} \
		--schemas={self.schema_conv_init_option.dbname} \
		--user={self.schema_conv_init_option.username} \
		--password={self.schema_conv_init_option.password} \
		--info-level=maximum \
		--command=serialize\
		--output-file=./intermediate_data/{self.schema_conv_init_option.dbname}/{self.schema_filename}"
		os.system(command_generate_mysql_schema)

		print(f"Generate MySQL database schema done!")


	def __save_schema(self):
		"""
		Save MySQL schema which was generate by SchemaCrawler to MongoDB database
		"""
		db_connection = open_connection_mongodb(self.mongodb_connection_list)	
		import_json_to_mongodb(db_connection, collection_name="schema", dbname=self.schema_conv_output_option.dbname, json_filename=self.schema_filename)
		print(f"Save schema in MongoDB done!")


	def __save_schema_view(self):
		"""
		Store a MySQL converted schema in MongoDB. 
		This schema will be used for generated detail schema in future by end-user.
		Converted schema structure:
			Dict(
				"Converted schema": Dict(
					"Database type": "MySQL",
					"Schema": <Database name>,
					"Tables": List[
						Dict(
							"Table name": <table name>,
							"Columns": List[
								"Column name": <column name>
							]
						)
					],
					"Procedures": [],
					"Functions": [],
					"Triggers":	[]
				)
			)
		"""
		print("Start convert schema!")
		converted_schema = {}
		catalog_schema = self.db_schema["catalog"]
		converted_schema["database-name"] = catalog_schema["database-info"]["product-name"]
		converted_schema["database-version"] = catalog_schema["database-info"]["product-version"]
		converted_schema["schema"] = catalog_schema["database-info"]["server-info"][0]["value"]
		converted_schema["tables"] = []
		converted_schema["foreign-keys"] = []

		triggers_dict = self.__get_triggers_dict()
		col_dict = self.get_columns_dict()

		for table_schema in self.tables_schema:
			table_info = {}
			table_info["name"] = table_schema["name"]
			table_info["engine"] = table_schema["attributes"]["ENGINE"]
			table_info["table-collation"] = table_schema["attributes"]["TABLE_COLLATION"]
			table_info["remarks"] = table_schema["remarks"]

			table_info["constraints"] = []
			for table_schema_constraint in table_schema["table-constraints"]:
				if type(table_schema_constraint) is dict:
					table_constraint = {
						"name": table_schema_constraint["name"],
						"type": table_schema_constraint["constraint-type"],
						"definition": table_schema_constraint["definition"]
					}
					table_info["constraints"].append(table_constraint)

			table_info["triggers"] = []
			if table_info["name"] in triggers_dict:
				table_info["triggers"] = triggers_dict[table_info["name"]]
			
			columns_schema = self.db_schema["all-table-columns"]
			table_info["columns"] = []
			for column_schema in columns_schema:
				if column_schema["@uuid"] in table_schema["columns"]:
					column_info = {
						"name": column_schema["name"],
						"character-set-name": column_schema["attributes"]["CHARACTER_SET_NAME"],
						"collation-name": column_schema["attributes"]["COLLATION_NAME"],
						"column-type": column_schema["attributes"]["COLUMN_TYPE"],
						"nullable": column_schema["attributes"]["IS_NULLABLE"],
						"auto-incremented": column_schema["auto-incremented"],
						"nullable": column_schema["nullable"],
						"default-value" : column_schema["default-value"],
					}
					table_info["columns"].append(column_info)
			table_info["indexes"] = []
			for index_schema in table_schema["indexes"]:
				if type(index_schema) is dict:
					index_column_list = list(map(lambda col_sche: col_sche["name"], list(filter(lambda col_sche: col_sche["@uuid"] in index_schema["columns"], columns_schema))))
					index_info = {
						"name": index_schema["name"],
						"unique": index_schema["unique"],
						"columns": index_column_list
					}
				else:
					index_info = {
						"name": col_dict[index_schema],
						"unique": False,
						"columns": [col_dict[index_schema]]	
					}
				table_info["indexes"].append(index_info)

			converted_schema["tables"].append(table_info)

			table_dict = self.get_tables_dict()
			cols_dict = self.get_columns_dict()
			for foreign_key_schema in table_schema["foreign-keys"]:
				if type(foreign_key_schema) is dict:
					col_refs = list(map(lambda fk_sche: {
							"key-sequence": fk_sche["key-sequence"],
							"foreign-key-column": cols_dict[fk_sche["foreign-key-column"]], 
							"foreign-key-table": table_dict[fk_sche["foreign-key-column"]],
							"primary-key-column": cols_dict[fk_sche["primary-key-column"]], 
							"primary-key-table": table_dict[fk_sche["primary-key-column"]],
						}, 
						foreign_key_schema["column-references"]))
					foreign_key_info = {
						"name": foreign_key_schema["name"],
						"column-references": col_refs,
						"delete-rule": foreign_key_schema["delete-rule"],
						"update-rule": foreign_key_schema["update-rule"],
					}
					converted_schema["foreign-keys"].append(foreign_key_info)

		converted_schema["procedures"] = self.__get_procedures_list()
		converted_schema["functions"] = self.__get_functions_list()
		
		mongodb_connection = open_connection_mongodb(self.mongodb_connection_list)
		store_json_to_mongodb(mongodb_connection, "schema_view", converted_schema)
		print(f"Convert and save schema view in MongoDB done!")


	def __create_mongo_schema_validators(self):
		"""
		Specify MongoDB schema validator for all tables.
		"""
		table_view_column_dtype = self.get_table_column_and_data_type()
		table_list = self.get_tables_name_list()
		uuid_col_dict = self.get_columns_dict()
		
		table_column_dtype = {}
		for table in table_list:
			table_column_dtype[table] = table_view_column_dtype[table]
		
		table_cols_uuid = {}
		for table in self.tables_schema:
			table_name = table["name"]
			if table_name in table_list:
				table_cols_uuid[table_name] = table["columns"]
		
		enum_col_dict = {}
		for col in self.all_table_columns:
			if col["attributes"]["COLUMN_TYPE"][:4] == "enum":
				data = {}
				table_name, col_name = col["short-name"].split(".")[:2]
				if table_name in table_list:
					data = list(map(lambda ele: ele[1:-1], col["attributes"]["COLUMN_TYPE"][5:-1].split(",")))
					sub_dict = {}
					sub_dict[col_name] = data
					enum_col_dict[table_name] = sub_dict
		
		db_connection = open_connection_mongodb(self.mongodb_connection_list)
		for table in self.__get_tables_and_views_list():
			db_connection.create_collection(table)
		
		for table in table_cols_uuid:
			props = {}
			for col_uuid in table_cols_uuid[table]:
				col_name = uuid_col_dict[col_uuid]
				mysql_dtype = table_column_dtype[table][col_name]
				if mysql_dtype == "ENUM":
					data = {
						"enum": enum_col_dict[table][col_name],
						"description": "can only be one of the enum values"
					}
				else:
					data = {
						"bsonType": self.__data_type_schema_mapping(mysql_dtype)
					}
				props[col_name] = data
				json_schema = {
					"bsonType": "object",
					"properties": props
				}
				vexpr = {"$jsonSchema": json_schema}
				cmd = OrderedDict([('collMod', table), ('validator', vexpr)])
				db_connection.command(cmd)

		print("Create schema validators successfully!")


	def __create_mongo_indexes(self):
		"""
		Add index to MongoDB collection.
		Just use for running time. Need to remove indexes before exporting MongoDB database.
		"""
		fk_uuid_list = []
		for table_schema in self.tables_schema:
			for fk_schema in table_schema["foreign-keys"]:
				if type(fk_schema) is dict:
					fk_uuid_list = fk_uuid_list + [fk["foreign-key-column"] for fk in fk_schema["column-references"]]

		mongodb_connection = open_connection_mongodb(self.mongodb_connection_list)
		for column_schema in self.all_table_columns:
			if column_schema["@uuid"] in fk_uuid_list:
				collection_name = column_schema["short-name"].split(".")[0]
				column_name = column_schema["name"]
				collection_conn = mongodb_connection[collection_name]
				collection_conn.create_index(column_name, unique=False)
		print("Create indexes done!")


	def __drop_view(self):
		"""
		Drop views of MySQL from MongoDB.
		"""
		mongodb_connection = open_connection_mongodb(self.mongodb_connection_list)
		view_set = set(self.__get_tables_and_views_list()) - set(self.get_tables_name_list())
		for view in list(view_set):
			mycol = mongodb_connection[view]
			mycol.drop() 
		return True


	### DEFINE UTILITIES

	def get_columns_dict(self):
		"""
		Extract column uuid and name from database schema
		Return:
			Dict(key: <column uuid>, value: <column name>)
		"""
		all_table_columns = self.db_schema["all-table-columns"]
		col_dict = {}
		for col in all_table_columns:
			col_dict[col["@uuid"]] = col["name"]
		return col_dict

	def get_tables_dict(self):
		"""
		Extract column uuid and its table name from database schema
		Return:
			Dict(key: <column uuid>, value: <name of table has that column>)
		"""
		table_dict = {}
		for table in self.tables_schema:
			if type(table) is dict:
				for col in table["columns"]:
					table_dict[str(col)] = table["name"]
		return table_dict


	def get_tables_relations(self):
		"""
		Get relations between MySQL tables from database schema.
		Result will be a dictionary which has uuids (of relation, defined by SchemaCrawler) as keys, and values including:
		- source: Name of table which holds primary key of relation
		- dest: Name of table which holds foreign key of relation
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

		)
		"""
		col_dict = self.get_columns_dict()
		table_dict = self.get_tables_dict()

		relations_dict = {}
		for table in self.tables_schema:
			for foreign_key in table["foreign-keys"]:
				if(isinstance(foreign_key, dict)):
					relation_uuid = foreign_key["@uuid"]
					col_refs = foreign_key["column-references"]
					relations_dict[relation_uuid] = {
						"primary_key_table": table_dict[col_refs[0]["primary-key-column"]],
						"foreign_key_table": table_dict[col_refs[0]["foreign-key-column"]],
						"key_columns": list(map(lambda ele: {
							"primary_key_column": col_dict[ele["primary-key-column"]],
							"foreign_key_column": col_dict[ele["foreign-key-column"]]
							}, col_refs))
					}
		return relations_dict

	def get_tables_name_list(self):
		"""
		Get list of name of all tables.
		"""
		table_name_list = list(map(lambda table: table["name"], list(filter(lambda table: table["remarks"] == "", self.tables_schema))))
		return table_name_list

	def __get_tables_and_views_list(self):
		"""
		Get list of name of all tables and views.
		"""
		table_and_view_name_list = list(map(lambda table: table["name"], self.tables_schema))
		return table_and_view_name_list

	def get_table_column_and_data_type(self):
		"""
		Get dict of tables, columns name and columns data type.
		Dict(
			key: <table name>
			value: Dict(
				key: <column name>
				value: <MySQL column data type>
			)
		)
		"""
		table_dict = self.get_tables_dict()
		all_columns = self.all_table_columns
		schema_type_dict = {}
		for col in all_columns:
			dtype = col["column-data-type"]
			if type(dtype) is dict:
				schema_type_dict[dtype["@uuid"]] = dtype["name"].split()[0]
		table_list = self.__get_tables_and_views_list()
		res = {}
		for table_name in table_list:
			res[table_name] = {}
		for col in all_columns:
			dtype = col["column-data-type"]
			if type(dtype) is dict:
				res[table_dict[col["@uuid"]]][col["name"]] = schema_type_dict[dtype["@uuid"]]
			else:
				res[table_dict[col["@uuid"]]][col["name"]] = schema_type_dict[dtype]
		return res
		

	def __data_type_schema_mapping(self, mysql_type):
		"""
		Mapping data type from MySQL to MongoDB.
		Input: MySQL data type.
		Output: MongoDB data type.
		"""
		dtype_dict = {
			"int": ["BIT", "TINYINT", "SMALLINT", "MEDIUMINT", "YEAR", "BOOL", "BOOLEAN"],
			"long": ["INT", "INTEGER", "BIGINT"],
			"decimal": ["DECIMAL", "DEC", "FIXED"],
			"double": ["FLOAT", "DOUBLE", "REAL"],
			"date": ["DATE", "DATETIME", "TIMESTAMP", "TIME"],
			"binData": ["BINARY", "VARBINARY"],
			"string": [
				"CHARACTER", "CHARSET", "ASCII", "UNICODE", "CHAR", "VARCHAR", 
				"TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT", 
				"TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB",
				"GEOMETRY", "POINT", "LINESTRING", "POLYGON", 
				"MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION"
				],
			"object": ["JSON", "ENUM"],
			"array": ["SET"]
		}

		for mongodb_type in dtype_dict.keys():
			if mysql_type in dtype_dict[mongodb_type]:
				return mongodb_type
		print(f"MySQL data type {mysql_type} has not been handled!")
		return None


	def __get_coluuid(self, table_name, col_name):
		"""
		Get column uuid:
		Input: Table name and column name.
		Output: Column uuid
		"""
		for col in self.all_table_columns():
			if f"{table_name}.{col_name}" == col["short-name"]:
				return col["@uuid"]
		print(f"Can not find column {col_name} from table {table_name}!")
		return None

	def __get_col_type_from_schema_attribute(self, table_name, col_name):
		"""
		Get MySQL column data type from schema.
		Input: Table name and column name.
		Output: MySQL data type of column.
		"""
		for col in self.all_table_columns:
			if f"{table_name}.{col_name}" == col["short-name"]:
				return col["attributes"]["COLUMN_TYPE"]
		print(f"Can not find column {col_name} from table {table_name}!")
		return None

	def __get_triggers_dict(self):
		"""
		Get triggers dict of database.
		Dict(
			<table name>: List[
				Dict(
					"trigger-name": <trigger name>,
					"event": <event>,
					"statement": <statement>,
					"timing": <timing>,
				)
			]
		)
		"""
		mysql_connection = open_connection_mysql(self.mysql_connection_list, self.schema_conv_init_option.dbname)
		mysql_cursor = mysql_connection.cursor()
		mysql_cursor.execute("SHOW TRIGGERS;")
		# triggers_list = [fetched_data for fetched_data in mysql_cursor]
		triggers_list = mysql_cursor.fetchall()
		# print(triggers_list)
		mysql_cursor.close()
		mysql_connection.close()
		triggers_dict = {}
		for trigger in triggers_list:
			# triggers_dict[trigger[2].decode("utf-8")] = []
			triggers_dict[trigger[2] if type(trigger[2]) is str else trigger[2].decode("utf-8")] = []
		for trigger in triggers_list:
			trigger_info = {
				"trigger-name": trigger[0],
				"envent": trigger[1] if type(trigger[1]) is str else trigger[1].decode("utf-8"),
				"statement": trigger[3] if type(trigger[3]) is str else trigger[3].decode("utf-8"),
				"timing": trigger[4] if type(trigger[4]) is str else trigger[4].decode("utf-8")
			}
			triggers_dict[trigger[2] if type(trigger[2]) is str else trigger[2].decode("utf-8")].append(trigger_info)
		return triggers_dict

	def __get_procedures_list(self):
		"""
		Get list of procedures of database
		"""
		mysql_connection = open_connection_mysql(self.mysql_connection_list, self.schema_conv_init_option.dbname)
		mysql_cursor = mysql_connection.cursor()
		mysql_cursor.execute("SHOW PROCEDURE STATUS WHERE db=%s;", (self.schema_conv_init_option.dbname,))
		# print(mysql_cursor.fetchall())
		procedure_name_list = list(map(lambda proc: proc[1], mysql_cursor.fetchall()))
		# print(procedure_name_list)
		procedures_list = []
		for procedure_name in procedure_name_list:
			# procedure_name = "film_in_stock; create schema injection"
			mysql_cursor.execute(f"SHOW CREATE PROCEDURE {procedure_name};")
			procedures_list.append({
				"procedure-name": procedure_name,
				"procedure-creation": mysql_cursor.fetchone()[2]
				})

		return procedures_list


	def __get_functions_list(self):
		"""
		Get list of functions of database
		"""
		mysql_connection = open_connection_mysql(self.mysql_connection_list, self.schema_conv_init_option.dbname)
		mysql_cursor = mysql_connection.cursor()
		mysql_cursor.execute("SHOW FUNCTION STATUS WHERE db=%s;", (self.schema_conv_init_option.dbname,))
		functions_name_list = list(map(lambda func: func[1], mysql_cursor.fetchall()))
		functions_list = []
		for function_name in functions_name_list:
			mysql_cursor.execute(f"SHOW CREATE FUNCTION {function_name};")
			functions_list.append({
				"function-name": function_name,
				"function-creation": mysql_cursor.fetchone()[2]
				})

		return functions_list
	
	def get_primary_key_dict(self):
		"""
		Get dict of primary keys of tables.
		Return:
			Dict(<table name>: List[<primary key column name>])
		"""
		schema_view = load_mongodb_collection(self.mongodb_connection_list + ["schema_view"])
		tables_schema = schema_view[0]["tables"]
		pk_dict = {}
		for table_schema in tables_schema:
			pk_list = list(filter(lambda index: index["name"] == "PRIMARY", table_schema["indexes"]))
			if len(pk_list) > 0:
				pk = pk_list[0]
				pk_dict[table_schema["name"]] = pk["columns"]
			else:
				fk_list = []
				for index in table_schema["indexes"]:
					fk_list = fk_list + index["columns"]
				pk_dict[table_schema["name"]] = fk_list
		return pk_dict

	### END UTILITIES