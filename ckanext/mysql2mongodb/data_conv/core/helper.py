import urllib
import json
import re
import os
import requests
from pprint import pprint
import xmltodict
import logging

from pymongo import MongoClient
import json


FILE_URL = "./core/config.xml"


def getConfig():
    with open(FILE_URL, "r") as f:
        lines = f.readlines()
    temp = ''
    for line in lines:
        temp += line
    config = xmltodict.parse(temp)
    return config


def read_package_config():
    package_conf = {}
    config = getConfig()
    package_conf["package_id"] = config['dataconv']['package_id']
    package_conf["X-CKAN-API-Key"] = config['dataconv']['X-CKAN-API-KEY']

    return package_conf


def read_database_config():
    db_conf = {}

    config = getConfig()

    db_conf["mysql_host"] = config['dataconv']['mysql']['host']
    db_conf["mysql_port"] = config['dataconv']['mysql']['port']
    db_conf["mysql_password"] = config['dataconv']['mysql']['password']
    db_conf["mysql_username"] = config['dataconv']['mysql']['username']

    db_conf["datastore_host"] = config['dataconv']['datastore']['host']
    db_conf["datastore_port"] = config['dataconv']['datastore']['port']
    db_conf["datastore_username"] = config['dataconv']['datastore']['username']
    db_conf["datastore_password"] = config['dataconv']['datastore']['password']

    db_conf["redis_host"] = config['dataconv']['redis']['host']
    db_conf["redis_port"] = config['dataconv']['redis']['port']
    db_conf["redis_username"] = config['dataconv']['redis']['username']
    db_conf["redis_password"] = config['dataconv']['redis']['password']


    return db_conf


def getDSClient():
    cfg = read_database_config()

    connection_string = f"mongodb://{cfg['datastore_host']}:{cfg['datastore_port']}/"
    try:
        # Making connection
        mongo_client = MongoClient(
            connection_string, username=cfg['datastore_username'], password=cfg['datastore_password'])
        return mongo_client
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.error(
            "Error while connecting to MongoDB database! Re-check connection or name of database.")
        logger.error(
            "------------------------------------------------------------------------------------")
        logger.error("Connection string:" + str(connection_string))
        logger.error(
            "------------------------------------------------------------------------------------")
        raise e


def store_collection_to_DS(collections, dbs_name):
    '''
    Use to store mutiple collections of data to Datastore.
    Require:
        - collections: collections of collection. Each collection is a tuple, first element is collection name, second collection is a list of JSON objects/documents(or collection)
        - dbs_name: Name of the database or the overall name of all the collections
    '''
    try:
        client = getDSClient()
        ds_connection = client[dbs_name]

        for collection in collections:
            collection_name = collection[0]
            collection_data = collection[1]

            if isinstance(collection_data, list):
                ds_connection[collection_name].insert_many(collection_data)
                # Collection.insert_many(json_data, ordered=False)
            else:
                ds_connection[collection_name].insert_one(collection_data)
                print(
                    f"Write JSON data to Datastore collection {collection_name} successfully!")
        return True
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.error(
            "------------------------------------------------------------------------------------")
        logger.error(
            "Error while storing collections to Datastore.")
        raise e
