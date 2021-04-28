import json
import os
import re
import subprocess
from collections import OrderedDict
import mysql.connector
from pymongo import GEO2D, TEXT


from ckanext.mysql2mongodb.data_conv.core.utilities import open_connection_mongodb, load_mongodb_collection
from ckanext.mysql2mongodb.data_conv.core.helper import store_collection_to_DS


from ckanext.mysql2mongodb.data_conv.core.interfaces.AbstractSchemaConversion import AbstractSchemaConversion


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


def open_connection_mysql(host, username, password, dbname=None):
    """
    Set up a connection to MySQL database.
    Return a MySQL (connector) connection object if success, otherwise None.
    """
    try:
        db_connection = mysql.connector.connect(
            host=host,
            user=username,
            password=password,
            database=dbname,
            auth_plugin='mysql_native_password'
        )
        if db_connection.is_connected():
            db_info = db_connection.get_server_info()
            print("Connected to MySQL Server version ", db_info)

            return db_connection
        else:
            print("Connect fail!")
            return None
    except Exception as e:
        print(
            f"Error while connecting to MySQL database {dbname}! Re-check connection or name of database.")
        print(e)
        raise e


class MySQLSchemaImportConversion(AbstractSchemaConversion):
    """
    MySQL Database Schema class.
    This class is used for
            - Extracting schema from MySQL database.
            - Exporting MySQL schema as JSON.
            - Storing MySQL schema as a collection in MongoDB database.
            - Loading MySQL schema, which was stored in MongoDB before, for another processes.
            - Defining a MongoDB schema validator based on MySQL schema.
            - Creating MongoDB secondary indexes based on MySQL schema.
    All above processes are belong to phase "Schema Conversion".
    """

    def __init__(self):
        super(MySQLSchemaImportConversion, self).__init__()
        # Define a name for schema file, which will be place at intermediate folder.
        self.schema_filename = "schema.json"

    def set_config(self, schema_conv_init_option, schema_conv_output_option):
        """
        To set up connections, you need to provide:
                - schema_conv_init_option:_ instance of class ConvInitOption, which specified connection to "Input" database (MySQL).
                - schema_conv_output_option: instance of class ConvOutputOption, which specified connection to "Out" database (MongoDB).
        """
        self.schema_conv_init_option = schema_conv_init_option
        self.schema_conv_output_option = schema_conv_output_option

    def get(self):
        self.load_schema()
        return self.db_schema

    def run(self):
        self.__generate_mysql_schema()
        return True

    def save(self):
        """
        Save MySQL schema which was generate by SchemaCrawler to MongoDB database
        """
        db_connection = open_connection_mongodb(self.schema_conv_output_option)
        # print("Ready
        with open(f"./intermediate_data/{self.schema_conv_output_option.dbname}/{self.schema_filename}") as file:
            file_data = json.load(file)
            collections = [('schema', file_data)]
            store_collection_to_DS(
                collections, self.schema_conv_output_option.dbname)

        print(
            f"Save schema from {self.schema_conv_output_option.dbname} database to MongoDB successfully!")
        return True

    def load_schema(self):
        """
        Load schema from JSON file.
        *Need to be edited for loading from MongoDB instead.
        """
        if not hasattr(self, "db_schema"):
            db_schema = load_mongodb_collection(
                self.schema_conv_output_option, "schema")
            self.db_schema = db_schema[0]
            # Most used variable
            self.all_table_columns = self.db_schema["all-table-columns"]
            self.tables_schema = self.db_schema["catalog"]["tables"]
            self.extracted_tables_schema = self.extract_tables_schema()

    def __generate_mysql_schema(self, info_level="maximum"):
        """
        Generate MySQL schema using SchemaCrawler, then save as JSON file at intermediate directory.
        """
        subprocess.run(
            [f"mkdir -p ./intermediate_data/{self.schema_conv_init_option.dbname}"], check=True, shell=True)

        command = f"./core/_schemacrawler/schemacrawler.sh \
		--server=mysql \
		--host={self.schema_conv_init_option.host} \
		--port={self.schema_conv_init_option.port} \
		--database={self.schema_conv_init_option.dbname} \
		--schemas={self.schema_conv_init_option.dbname} \
		--user={self.schema_conv_init_option.username} \
		--password={self.schema_conv_init_option.password} \
		--info-level={info_level} \
		--command=serialize\
		--output-file=./intermediate_data/{self.schema_conv_init_option.dbname}/{self.schema_filename}"

        print('command')
        print(os.getcwd())
        subprocess.run([command], check=True, shell=True)
        print(
            f"Generate MySQL database {self.schema_conv_init_option.dbname} successfully!")
        return True

    def extract_tables_schema(self, extracted_keys_list=["@uuid", "name", "columns", "foreign-keys"]):
        """
        Extract only specific fields from tables schema.
        Params:
                extracted_keys_list: List of specific keys. 
        """
        ite_func = extract_dict(extracted_keys_list)
        return list(map(ite_func, self.tables_schema))

    def get_columns_dict(self):
        """
        Extract column uuid and name from database schema
        Return a dictionary with @uuid as key and column name as value
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
        Return a dictionary with @uuid as key and table name as value
        Dict(key: <column uuid>, value: <name of table has that column>)
        """
        table_dict = {}
        for table in self.tables_schema:
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
                        "primary_key_column": <primary column name>,
                        "foreign_key_column": <foreign column name>
                )

        )
        """
        col_dict = self.get_columns_dict()
        table_dict = self.get_tables_dict()

        relations_dict = {}
        for table in self.extracted_tables_schema:
            for foreign_key in table["foreign-keys"]:
                if(isinstance(foreign_key, dict)):
                    relation_uuid = foreign_key["@uuid"]
                    foreign_key_uuid = foreign_key["column-references"][0]["foreign-key-column"]
                    primary_key_uuid = foreign_key["column-references"][0]["primary-key-column"]
                    relations_dict[relation_uuid] = {}
                    relations_dict[relation_uuid]["primary_key_table"] = table_dict[primary_key_uuid]
                    relations_dict[relation_uuid]["foreign_key_table"] = table_dict[foreign_key_uuid]
                    relations_dict[relation_uuid]["primary_key_column"] = col_dict[primary_key_uuid]
                    relations_dict[relation_uuid]["foreign_key_column"] = col_dict[foreign_key_uuid]
        return relations_dict

    def get_tables_name_list(self):
        """
        Get list of name of all tables.
        """
        self.load_schema()
        table_name_list = list(map(lambda table: table["name"], list(
            filter(lambda table: table["remarks"] == "", self.tables_schema))))
        return table_name_list

    def get_tables_and_views_list(self):
        """
        Get list of name of all tables and views.
        """
        self.load_schema()
        table_and_view_name_list = list(
            map(lambda table: table["name"], self.extracted_tables_schema))
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
        self.load_schema()
        table_dict = self.get_tables_dict()
        all_columns = self.db_schema["all-table-columns"]
        schema_type_dict = {}
        for col in all_columns:
            dtype = col["column-data-type"]
            if type(dtype) is dict:
                schema_type_dict[dtype["@uuid"]] = dtype["name"].split()[0]
        table_list = self.get_tables_and_views_list()
        res = {}
        for table_name in table_list:
            res[table_name] = {}
        for col in all_columns:
            dtype = col["column-data-type"]
            if type(dtype) is dict:
                res[table_dict[col["@uuid"]]][col["name"]
                                              ] = schema_type_dict[dtype["@uuid"]]
            else:
                res[table_dict[col["@uuid"]]][col["name"]
                                              ] = schema_type_dict[dtype]
        return res

    def get_schema_standardlized(self):
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
                                ]
                        )
                )
        """
        self.load_schema()
        converted_schema = {}
        catalog_schema = self.db_schema["catalog"]

        converted_schema["database-name"] = catalog_schema["database-info"]["product-name"]
        converted_schema["database-version"] = catalog_schema["database-info"]["product-version"]
        converted_schema["schema"] = catalog_schema["name"]
        converted_schema["tables"] = []
        converted_schema["foreign-keys"] = []

        tables_schema = catalog_schema["tables"]
        for table_schema in tables_schema:
            table_info = {}
            table_info["name"] = table_schema["name"]
            table_info["engine"] = table_schema["attributes"]["ENGINE"]
            table_info["table-collation"] = table_schema["attributes"]["TABLE_COLLATION"]

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
            for table_schema_trigger in table_schema["triggers"]:
                if type(table_schema_trigger) is dict:
                    table_trigger = {
                        "name": table_schema_trigger["name"],
                        "action-condition": table_schema_trigger["action-condition"],
                        "action-order": table_schema_trigger["action-order"],
                        "action-orientation": table_schema_trigger["action-orientation"],
                        "action-statement": table_schema_trigger["action-statement"],
                        "condition-timing": table_schema_trigger["condition-timing"],
                        "event-manipulation-type": table_schema_trigger["event-manipulation-type"],
                    }
                    table_info["triggers"].append(table_trigger)

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
                        "default-value": column_schema["default-value"],
                    }
                    table_info["columns"].append(column_info)

            table_info["indexes"] = []
            for index_schema in table_schema["indexes"]:
                if type(index_schema) is dict:
                    index_column_list = list(map(lambda col_sche: {"name": col_sche["name"], "table": col_sche["short-name"].split(
                        ".")[0]}, list(filter(lambda col_sche: col_sche["@uuid"] in index_schema["columns"], columns_schema))))
                    index_info = {
                        "name": index_schema["name"],
                        "unique": index_schema["unique"],
                        "columns": index_column_list
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

            print(
                f"Save schema view from {self.schema_conv_output_option.dbname} database to MongoDB successfully!")
        return converted_schema
