import sys
import json
import bson
import re
import time
import pprint
import logging

from bson.decimal128 import Decimal128
from decimal import Decimal
from bson import BSON
from datetime import datetime
from multiprocessing import Pool
from itertools import repeat

import mysql.connector
from pymongo import MongoClient

from ckanext.mysql2mongodb.data_conv.core.interfaces.AbstractDataConversion import AbstractDataConversion
from ckanext.mysql2mongodb.data_conv.core.helper import store_collection_to_DS


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


def store_json_to_mongodb(mongodb_connection, collection_name, json_data):
    """
    Import data from JSON object (not from JSON file).
    """
    try:
        # Created or switched to collection
        Collection = mongodb_connection[collection_name]
        if isinstance(json_data, list):
            Collection.insert_many(json_data)
            # Collection.insert_many(json_data, ordered=False)
        else:
            Collection.insert_one(json_data)
        print(
            f"Write JSON data to MongoDB collection {collection_name} successfully!")
        return True
    except Exception as e:
        print(
            f"Error while writing data to MongoDB collection {collection_name}.")
        print(e)
        raise e


def open_connection_mongodb(schema_conv_output_option):
    """
    Set up a connection to MongoDB database.
    Return a MongoClient object if success.
    """
    connection_string = f"mongodb://{schema_conv_output_option.host}:{schema_conv_output_option.port}/"
    try:
        # Making connection
        mongo_client = MongoClient(
            connection_string, username=schema_conv_output_option.username, password=schema_conv_output_option.password)
        # Select database
        db_connection = mongo_client[schema_conv_output_option.dbname]
        return db_connection
    except Exception as e:
        print(
            f"Error while connecting to MongoDB database {schema_conv_output_option.dbname}! Re-check connection or name of database.")
        print(e)
        raise e


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

    # def write_json_to_file(self, json_data, filename):
    # 	"""Write json data to file"""
    # 	with open(f"./intermediate_data/{self.schema_conv_init_option.dbname}/{filename}", 'w') as outfile:
    # 		json.dump(json_data, outfile, default=str)


class MysqlDataImportConversion (AbstractDataConversion):
    """
    DataConversion Database data class.
    This class is used for:
            - Converting and migrating data from MySQL to MongoDB.
            - Validating if converting is correct, using re-converting method.
    """

    def __init__(self):
        super(MysqlDataImportConversion, self).__init__()

    def set_config(self, schema_conv_init_option, schema_conv_output_option, schema):
        """
        To set config, you need to provide:
                - schema_conv_init_option: instance of class ConvInitOption, which specified connection to "Input" database (MySQL).
                - schema_conv_output_option: instance of class ConvOutputOption, which specified connection to "Out" database (MongoDB).
                - schema: MySQL schema object which was loaded from MongoDB.
        """
        self.schema = schema
        # set config
        self.schema_conv_init_option = schema_conv_init_option
        self.schema_conv_output_option = schema_conv_output_option

    def run(self):
        self.__save()

    def __save(self):
        tic = time.time()
        self.migrate_mysql_to_mongodb()
        toc = time.time()
        time_taken = round((toc-tic)*1000, 1)
        print(f"Time for migrating MySQL to MongoDB: {time_taken}")
        self.convert_relations_to_references()

    def find_converted_dtype(self, mysql_dtype):
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
            "string": "string",
            "object": "object",
            "single-geometry": "single-geometry",
            "multiple-geometry": "multiple-geometry",
        }

        dtype_dict = {}
        dtype_dict[mongodb_dtype["integer"]] = ["TINYINT",
                                                "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT"]
        dtype_dict[mongodb_dtype["decimal"]] = ["DECIMAL", "DEC", "FIXED"]
        dtype_dict[mongodb_dtype["double"]] = ["FLOAT", "DOUBLE", "REAL"]
        dtype_dict[mongodb_dtype["boolean"]] = ["BOOL", "BOOLEAN"]
        dtype_dict[mongodb_dtype["date"]] = ["DATE", "YEAR"]
        dtype_dict[mongodb_dtype["timestamp"]] = [
            "DATETIME", "TIMESTAMP", "TIME"]
        dtype_dict[mongodb_dtype["binary"]] = ["BIT", "BINARY", "VARBINARY"]
        dtype_dict[mongodb_dtype["blob"]] = [
            "TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB"]
        dtype_dict[mongodb_dtype["string"]] = ["CHARACTER", "CHARSET", "ASCII",
                                               "UNICODE", "CHAR", "VARCHAR", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT"]
        dtype_dict[mongodb_dtype["object"]] = ["ENUM", "SET", "JSON"]
        dtype_dict[mongodb_dtype["single-geometry"]
                   ] = ["GEOMETRY", "POINT", "LINESTRING", "POLYGON"]
        dtype_dict[mongodb_dtype["multiple-geometry"]] = ["MULTIPOINT",
                                                          "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION"]

        for target_dtype in dtype_dict.keys():
            if(mysql_dtype) in dtype_dict[target_dtype]:
                return mongodb_dtype[target_dtype]
        return None

    def migrate_mysql_to_mongodb(self):
        """
        Migrate data from MySQL to MongoDB.
        """
        migrate_data = []
        for table in self.schema.get_tables_name_list():
            collection = self.migrate_one_table_to_collection(table)
            element = (table, collection)
            migrate_data.append(element)
        store_collection_to_DS(
            migrate_data, self.schema_conv_output_option.dbname)

    def migrate_one_table_to_collection(self, table_name):
        try:
            fetched_data_list = self.get_fetched_data_list(table_name)
            convert_data_list = self.store_fetched_data_to_mongodb(
                table_name, fetched_data_list)

            return convert_data_list
        except Exception as e:
            raise e

    def get_fetched_data_list(self, table_name):
        colname_coltype_dict = self.schema.get_table_column_and_data_type()[
            table_name]
        try:
            db_connection = open_connection_mysql(
                self.schema_conv_init_option.host,
                self.schema_conv_init_option.username,
                self.schema_conv_init_option.password,
                self.schema_conv_init_option.dbname,
            )
            if db_connection.is_connected():
                # col_fetch_seq = []
                sql_cmd = "SELECT"
                for col_name in colname_coltype_dict.keys():
                    # col_fetch_seq.append(col_name)
                    dtype = colname_coltype_dict[col_name]
                    target_dtype = self.find_converted_dtype(dtype)

                    # Generating SQL for selecting from MySQL Database
                    if target_dtype is None:
                        raise Exception(
                            f"Data type {dtype} has not been handled!")
                    elif target_dtype == "single-geometry":
                        sql_cmd = sql_cmd + " ST_AsText(" + col_name + "),"
                    else:
                        sql_cmd = sql_cmd + " `" + col_name + "`,"
                # join sql
                sql_cmd = sql_cmd[:-1] + " FROM " + "`" + table_name + "`"

                db_cursor = db_connection.cursor()
                # execute sql
                db_cursor.execute(sql_cmd)
                # fetch data and convert
                fetched_data = db_cursor.fetchall()
                db_cursor.close()
                return fetched_data
            else:
                raise Exception("Connect fail!")

        except Exception as e:
            raise e

        finally:
            if (db_connection.is_connected()):
                db_connection.close()
                print("MySQL connection is closed!")

    def store_fetched_data_to_mongodb(self, table_name, fetched_data):
        """
        Parallel
        """
        colname_coltype_dict = self.schema.get_table_column_and_data_type()[
            table_name]
        rows = []
        # Parallel start from here
        for row in fetched_data:
            data = {}
            col_fetch_seq = list(colname_coltype_dict.keys())
            for i in range(len(col_fetch_seq)):
                col = col_fetch_seq[i]
                dtype = colname_coltype_dict[col]
                target_dtype = self.find_converted_dtype(dtype)
                # generate SQL
                cell_data = row[i]
                if cell_data != None:
                    # if dtype == "GEOMETRY":
                    # 	geodata = [float(num) for num in cell_data[6:-1].split()]
                    # 	geo_x, geo_y = geodata[:2]
                    # 	if geo_x > 180 or geo_x < -180:
                    # 		geo_x = 0
                    # 	if geo_y > 90 or geo_y < -90:
                    # 		geo_y = 0
                    # 	converted_data = {
                    # 		"type": "Point",
                    # 		"coordinates": [geo_x, geo_y]
                    # 	}
                    if dtype == "GEOMETRY":
                        converted_data = cell_data
                    if dtype == "VARBINARY":
                        # print(type(cell_data), str(cell_data))
                        converted_data = bytes(cell_data)
                        # print(type(converted_data), converted_data)
                        # return
                    elif dtype == "VARCHAR":
                        # print(str[cell_data], type(cell_data))
                        # return
                        converted_data = str(cell_data)
                    elif dtype == "BIT":
                        # get col type from schema attribute
                        # mysql_col_type = self.schema.get_col_type_from_schema_attribute(table, col)
                        # if mysql_col_type == "tinyint(1)":
                        # 	binary_num = cell_data
                        # 	converted_data = binary_num.to_bytes(len(str(binary_num)), byteorder="big")
                        # else:
                        # 	converted_data = cell_data
                        converted_data = cell_data
                    # elif dtype == "YEAR":
                        # print(cell_data, type(cell_data))
                    elif dtype == "DATE":
                        # print(cell_data, type(cell_data))
                        # , cell_data.hour, cell_data.minute, cell_data.second)
                        converted_data = datetime(
                            cell_data.year, cell_data.month, cell_data.day)
                    # elif dtype == "JSON":
                        # print(type(cell_data), cell_data)
                        # return
                    # elif dtype == "BLOB":
                        # print(cell_data, type(cell_data))
                        # return
                    elif target_dtype == "decimal":
                        converted_data = Decimal128(cell_data)
                    elif target_dtype == "object":
                        if type(cell_data) is str:
                            converted_data = cell_data
                        else:
                            converted_data = tuple(cell_data)
                    else:
                        converted_data = cell_data
                    data[col_fetch_seq[i]] = converted_data
            rows.append(data)
        # Parallel end here

        # assign to obj
        # store to mongodb
        # print("Start migrating table ", table)
        return rows

    def convert_relations_to_references(self):
        """
        Convert relations of MySQL table to database references of MongoDB
        """
        tables_name_list = self.schema.get_tables_name_list()
        # db_connection = open_connection_mongodb(mongodb_connection_info)
        tables_relations = self.schema.get_tables_relations()
        # converting_tables_order = specify_sequence_of_migrating_tables(schema_file)
        edited_table_relations_dict = {}
        original_tables_set = set(
            [tables_relations[key]["primary_key_table"] for key in tables_relations])

        # Edit relations of table dictionary
        for original_table in original_tables_set:
            for key in tables_relations:
                if tables_relations[key]["primary_key_table"] == original_table:
                    if original_table not in edited_table_relations_dict.keys():
                        edited_table_relations_dict[original_table] = []
                    edited_table_relations_dict[original_table] = edited_table_relations_dict[original_table] + [
                        extract_dict(["primary_key_column", "foreign_key_table", "foreign_key_column"])(tables_relations[key])]
        # Convert each relation of each table
        for original_collection_name in tables_name_list:
            if original_collection_name in original_tables_set:
                for relation_detail in edited_table_relations_dict[original_collection_name]:
                    referencing_collection_name = relation_detail["foreign_key_table"]
                    original_key = relation_detail["primary_key_column"]
                    referencing_key = relation_detail["foreign_key_column"]
                    self.convert_one_relation_to_reference(
                        original_collection_name, referencing_collection_name, original_key, referencing_key)
        print("Convert relations successfully!")

    def convert_one_relation_to_reference(self, original_collection_name, referencing_collection_name, original_key, referencing_key):
        """
        Convert one relation of MySQL table to database reference of MongoDB
        """
        db_connection = open_connection_mongodb(
            self.schema_conv_output_option)
        original_collection_connection = db_connection[original_collection_name]
        original_documents = original_collection_connection.find()
        new_referenced_key_dict = {}
        for doc in original_documents:
            new_referenced_key_dict[doc[original_key]] = doc["_id"]

        referencing_documents = db_connection[referencing_collection_name]
        for key in new_referenced_key_dict:
            new_reference = {}
            new_reference["$ref"] = original_collection_name
            new_reference["$id"] = new_referenced_key_dict[key]
            new_reference["$db"] = self.schema_conv_output_option.dbname
            referencing_key_new_name = "db_ref_" + referencing_key
            referencing_documents.update_many({referencing_key: key}, update={
                                              "$set": {referencing_key_new_name: new_reference}})
