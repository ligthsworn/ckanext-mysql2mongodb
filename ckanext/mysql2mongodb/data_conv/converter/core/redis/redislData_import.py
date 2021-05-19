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


class RedisDataExportConversion (AbstractDataConversion):
    """
    DataConversion Database data class.
    This class is used for:
            - Converting and migrating data from Redis to MongoDB.
    """

    def __init__(self):
        super(RedisDataExportConversion, self).__init__()

    def set_config(self, schema_conv_init_option, schema_conv_output_option, schema):
        """
        To set config, you need to provide:
                - schema_conv_init_option: instance of class ConvInitOption, which specified connection to "Input" database.
                - schema_conv_output_option: instance of class ConvOutputOption, which specified connection to "Out" database.
                - schema: schema object acquired from Schema Converter.
        """
        self.schema = schema
        # set config
        self.schema_conv_init_option = schema_conv_init_option
        self.schema_conv_output_option = schema_conv_output_option

    def run(self):
        dbJson = list()
        redis_con = open_connection_redis(self.schema_conv_init_option.host, self.schema_conv_init_option.username,
                                          self.schema_conv_init_option.password, self.schema_conv_init_option.dbname)
        stringData = list()
        listData = list()
        setData = list()
        hashData = list()
        stringData = list()
        sortedSetData = list()
        for key in redis_con.keys():
            keyString = key.decode("utf-8")
            valueType = redis_con.type(key)
            if valueType == b"string":
                stringData.append({
                    "key": keyString,
                    "value": redis_con.get(key).decode("utf-8")
                })
            if valueType == b"list":
                listData.append({
                    "key": keyString,
                    "value": list(map(lambda x: x.decode("utf-8"), redis_con.lrange(key, 0, redis_con.llen(key))))
                })
            if valueType == b"set":
                setData.append({
                    "key": keyString,
                    "value": list(map(lambda x: x.decode("utf-8"), redis_con.smembers(key)))
                })
            if valueType == b"hash":
                allKeysAndValues = redis_con.hgetall(key)
                valueObject = {}
                for item in allKeysAndValues:
                    valueObject[item.decode(
                        "utf-8")] = allKeysAndValues[item].decode("utf-8")
                hashData.append({
                    "key": keyString,
                    "value": valueObject
                })
            if valueType == b"zset":
                sortedValues = redis_con.zrange(key, 0, redis_con.zcount(
                    key, -sys.maxsize-1, sys.maxsize), False, True)
                valueObject = {}
                for member, score in sortedValues:
                    valueObject[member.decode("utf-8")] = score
                sortedSetData.append({
                    "key": keyString,
                    "value": valueObject
                })
        dbJson.append(stringData)
        dbJson.append(listData)
        dbJson.append(setData)
        dbJson.append(hashData)
        dbJson.append(sortedSetData)

        store_collection_to_DS(
            dbJson, self.schema_conv_output_option.dbname)

    def __save(self):
        pass
