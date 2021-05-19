from .core.database_function import DatabaseFunctionsOptions
from pprint import pprint
import requests
import os
import re
import redis
import sys
import json
import time
from .factory import getDatabaseFuntions


def read_package_config(file_url="package_config.txt"):
    try:
        package_conf = {}
        with open(file_url, "r") as f:
            lines = f.readlines()

        for line in lines:
            look_for_conf = re.search(
                "^package_id", line.strip(), re.IGNORECASE)
            if look_for_conf is not None:
                package_conf["package_id"] = re.split(
                    r'[\s]+=[\s]+', line.strip())[1][1:-1]

            look_for_conf = re.search(
                "^X-CKAN-API-Key", line.strip(), re.IGNORECASE)
            if look_for_conf is not None:
                package_conf["X-CKAN-API-Key"] = re.split(
                    r'[\s]+=[\s]+', line.strip())[1][1:-1]

        return package_conf

    except Exception as e:
        pprint(e)
        pprint("Failed while read package config!")


def read_database_config():
    try:
        db_conf = {}
        file_url = "database_config.txt"
        with open(file_url, "r") as f:
            lines = f.readlines()

        for line in lines:
            look_for_conf = re.search(
                "^mysql_host", line.strip(), re.IGNORECASE)
            if look_for_conf is not None:
                db_conf["mysql_host"] = re.split(
                    r'[\s]+=[\s]+', line.strip())[1][1:-1]

            look_for_conf = re.search(
                "^mysql_port", line.strip(), re.IGNORECASE)
            if look_for_conf is not None:
                db_conf["mysql_port"] = re.split(
                    r'[\s]+=[\s]+', line.strip())[1][1:-1]

            look_for_conf = re.search(
                "^mysql_password", line.strip(), re.IGNORECASE)
            if look_for_conf is not None:
                db_conf["mysql_password"] = re.split(
                    r'[\s]+=[\s]+', line.strip())[1][1:-1]

            look_for_conf = re.search(
                "^mysql_username", line.strip(), re.IGNORECASE)
            if look_for_conf is not None:
                db_conf["mysql_username"] = re.split(
                    r'[\s]+=[\s]+', line.strip())[1][1:-1]

            look_for_conf = re.search(
                "^mongodb_host", line.strip(), re.IGNORECASE)
            if look_for_conf is not None:
                db_conf["mongodb_host"] = re.split(
                    r'[\s]+=[\s]+', line.strip())[1][1:-1]

            look_for_conf = re.search(
                "^mongodb_username", line.strip(), re.IGNORECASE)
            if look_for_conf is not None:
                db_conf["mongodb_username"] = re.split(
                    r'[\s]+=[\s]+', line.strip())[1][1:-1]

            look_for_conf = re.search(
                "^mongodb_port", line.strip(), re.IGNORECASE)
            if look_for_conf is not None:
                db_conf["mongodb_port"] = re.split(
                    r'[\s]+=[\s]+', line.strip())[1][1:-1]

            look_for_conf = re.search(
                "^mongodb_password", line.strip(), re.IGNORECASE)
            if look_for_conf is not None:
                db_conf["mongodb_password"] = re.split(
                    r'[\s]+=[\s]+', line.strip())[1][1:-1]

            look_for_conf = re.search(
                "^redis_host", line.strip(), re.IGNORECASE)
            if look_for_conf is not None:
                db_conf["redis_host"] = re.split(
                    r'[\s]+=[\s]+', line.strip())[1][1:-1]

            look_for_conf = re.search(
                "^redis_username", line.strip(), re.IGNORECASE)
            if look_for_conf is not None:
                db_conf["redis_username"] = re.split(
                    r'[\s]+=[\s]+', line.strip())[1][1:-1]

            look_for_conf = re.search(
                "^redis_port", line.strip(), re.IGNORECASE)
            if look_for_conf is not None:
                db_conf["redis_port"] = re.split(
                    r'[\s]+=[\s]+', line.strip())[1][1:-1]

            look_for_conf = re.search(
                "^redis_password", line.strip(), re.IGNORECASE)
            if look_for_conf is not None:
                db_conf["redis_password"] = re.split(
                    r'[\s]+=[\s]+', line.strip())[1][1:-1]

        return db_conf

    except Exception as e:
        print(e)
        print("Failed while reading database config!")


try:
    pprint("Start conversion!")
    sql_file_name = "dump.rdb"
    # if sql_file_name.split(".")[1] != "sql":
    #     print("Invalided MySQL backup file extension!")
    #     raise Exception()
    # os.system("whoami")
    os.chdir("./data_conv")
    # # os.system("ll")
    # os.system(f"mkdir -p ./downloads/{resource_id}")
    # os.system(f"curl -o ./downloads/{resource_id}/{sql_file_name} {sql_file_url}")

    db_conf = read_database_config()
    package_conf = read_package_config()

    schema_name = sql_file_name.split(".")[0]

    redis_host = db_conf["redis_host"]
    redis_username = db_conf["redis_username"]
    redis_password = db_conf["redis_password"]
    redis_port = db_conf["redis_port"]
    redis_dbname = schema_name

    source_database_funtions = getDatabaseFuntions(type="REDIS", options=DatabaseFunctionsOptions(
        host=redis_host, username=redis_username, password=redis_password, port=redis_port, dbname=redis_dbname))

    source_database_funtions.restore(f"./downloads/{sql_file_name}")

    time.sleep(2)

    r = redis.Redis(host="localhost", port=redis_port,
                    password=redis_password, db=0)
    dbJson = list()
    stringData = {
        "schema": {
            "collection": "string",
            "columns": {
                "key": "string",
                "value": "string"
            }
        },
        "data": list()
    }
    listData = {
        "schema": {
            "collection": "list",
            "columns": {
                "key": "string",
                "value": "array"
            }
        },
        "data": list()
    }
    setData = {
        "schema": {
            "collection": "set",
            "columns": {
                "key": "string",
                "value": "array"
            }
        },
        "data": list()
    }
    hashData = {
        "schema": {
            "collection": "hash",
            "columns": {
                "key": "string",
                "value": "object"
            }
        },
        "data": list()
    }
    sortedSetData = {
        "schema": {
            "collection": "sortedSet",
            "columns": {
                "key": "string",
                "value": "object"
            }
        },
        "data": list()
    }
    for key in r.keys():
        keyString = key.decode("utf-8")
        valueType = r.type(key)
        if valueType == b"string":
            stringData["data"].append({
                "key": keyString,
                "value": r.get(key).decode("utf-8")
            })
        if valueType == b"list":
            listData["data"].append({
                "key": keyString,
                "value": list(map(lambda x: x.decode("utf-8"), r.lrange(key, 0, r.llen(key))))
            })
        if valueType == b"set":
            setData["data"].append({
                "key": keyString,
                "value": list(map(lambda x: x.decode("utf-8"), r.smembers(key)))
            })
        if valueType == b"hash":
            allKeysAndValues = r.hgetall(key)
            valueObject = {}
            for item in allKeysAndValues:
                valueObject[item.decode(
                    "utf-8")] = allKeysAndValues[item].decode("utf-8")
            hashData["data"].append({
                "key": keyString,
                "value": valueObject
            })
        if valueType == b"zset":
            sortedValues = r.zrange(key, 0, r.zcount(
                key, -sys.maxsize-1, sys.maxsize), False, True)
            valueObject = {}
            for member, score in sortedValues:
                valueObject[member.decode("utf-8")] = score
            sortedSetData["data"].append({
                "key": keyString,
                "value": valueObject
            })
    dbJson.append(stringData)
    dbJson.append(listData)
    dbJson.append(setData)
    dbJson.append(hashData)
    dbJson.append(sortedSetData)

    print(json.dumps(dbJson))

    pprint("Done!")

except Exception as e:
    pprint(e)
    pprint("Convert fail!")
