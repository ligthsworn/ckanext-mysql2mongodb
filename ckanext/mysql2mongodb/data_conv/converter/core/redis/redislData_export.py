import sys
import json
import bson
import re
import time
import pprint
import logging
import redis

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


def open_connection_redis(host, username, password, dbname=None):
    try:
        db_connection = redis.Redis(host=host, port=6379,
                                    password=password, db=0)
        if db_connection.is_connected():
            db_info = db_connection.get_server_info()
            print("Connected to Redis", host)

            return db_connection
        else:
            print("Connect fail!")
            return None
    except Exception as e:
        print(
            f"Error while connecting to Redis on {host}! Please check again.")
        print(e)
        raise e


def open_connection_mongodb(schema_conv_init_option):
    """
    Set up a connection to MongoDB database.
    Return a MongoClient object if success.
    """
    connection_string = f"mongodb://{schema_conv_init_option.host}:{schema_conv_init_option.port}/"
    try:
        # Making connection
        mongo_client = MongoClient(
            connection_string, username=schema_conv_init_option.username, password=schema_conv_init_option.password)
        # Select database
        db_connection = mongo_client[schema_conv_init_option.dbname]
        return db_connection
    except Exception as e:
        print(
            f"Error while connecting to MongoDB database {schema_conv_init_option.dbname}! Re-check connection or name of database.")
        print(e)
        raise e


class RedisDataImportConversion (AbstractDataConversion):
    """
    DataConversion Database data class.
    This class is used for:
            - Converting and migrating data from MySQL to MongoDB.
            - Validating if converting is correct, using re-converting method.
    """

    def __init__(self):
        super(RedisDataImportConversion, self).__init__()

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
        dbJson = list()
        redis = open_connection_redis(self.schema_conv_output_option.host, self.schema_conv_output_option.username,
                                      self.schema_conv_output_option.password, self.schema_conv_output_option.dbname)

        mongoConnection = open_connection_mongodb(self.schema_conv_init_option)

        # TODO: Map nhe lai cai nay thanh check schema roi loop tren kia.
        for dataAndSchema in mongoConnection.list_collection_names():
            data = dataAndSchema["data"]
            if dataAndSchema["schema"]["collection"] == "string":
                for item in data:
                    redis.set(item["key"], item["value"])
            elif dataAndSchema["schema"]["collection"] == "list":
                for item in data:
                    redis.rpush(item["key"], *item["value"])
            elif dataAndSchema["schema"]["collection"] == "set":
                for item in data:
                    redis.sadd(item["key"], *item["value"])
            elif dataAndSchema["schema"]["collection"] == "hash":
                for item in data:
                    redis.hset(item["key"], mapping=item["value"])
            elif dataAndSchema["schema"]["collection"] == "sortedSet":
                for item in data:
                    redis.zadd(item["key"], item["value"])
            else:
                for item in data:
                    # TODO: bitmap hyperlog
                    redis.set(dataAndSchema["schema"]["collection"] +
                              "_" + item["key"], item["value"])

    def __save(self):
        tic = time.time()
        self.migrate_mysql_to_mongodb()
        toc = time.time()
        time_taken = round((toc-tic)*1000, 1)
        print(f"Time for migrating MySQL to MongoDB: {time_taken}")
        self.convert_relations_to_references()
