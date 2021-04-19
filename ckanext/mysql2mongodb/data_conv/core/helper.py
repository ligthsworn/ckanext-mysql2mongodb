import urllib
import json
import re
import os
import requests
from pprint import pprint
import xmltodict


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

    db_conf["mongodb_host"] = config['dataconv']['mongodb']['host']
    db_conf["mongodb_port"] = config['dataconv']['mongodb']['port']
    db_conf["mongodb_username"] = config['dataconv']['mongodb']['username']
    db_conf["mongodb_password"] = config['dataconv']['mongodb']['password']

    return db_conf
