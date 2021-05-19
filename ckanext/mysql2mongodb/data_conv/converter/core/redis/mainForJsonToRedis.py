from .core.database_function import DatabaseFunctionsOptions
from pprint import pprint
import requests
import os
import re
import redis
import sys
import json
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

    # source_database_funtions.restore(f"./downloads/{sql_file_name}")
    r = redis.Redis(host="localhost", port=redis_port,
                    password=redis_password, db=0)

    dataStringJson = """[{"schema": {"collection": "string", "columns": {"key": "string", "value": "string"}}, "data": [{"key": "thanh", "value": "hello"}, {"key": "test", "value": "hello"}]}, {"schema": {"collection": "list", "columns": {"key": "string", "value": "array"}}, "data": [{"key": "list1", "value": ["thien", "thanh", "thien", "thanh"]}, {"key": "mylist", "value": ["a", "a"]}, {"key": "list2", "value": ["2", "1", "2", "1"]}]}, {"schema": {"collection": "set", "columns": {"key": "string", "value": "array"}}, "data": [{"key": "set1", "value": ["1", "test", "thanh", "hello"]}]}, {"schema": {"collection": "hash", "columns": {"key": "string", "value": "object"}}, "data": [{"key": "user:1000", "value": {"username": "test", "password": "123", "age": "10"}}]}, {"schema": {"collection": "sortedSet", "columns": {"key": "string", "value": "object"}}, "data": [{"key": "myzset", "value": {"one": 1.0, "uno": 1.0, "two": 2.0, "three": 3.0}}]}]"""
    dbJson = json.loads(dataStringJson)

    for dataAndSchema in dbJson:
        data = dataAndSchema["data"]
        if dataAndSchema["schema"]["collection"] == "string":
            for item in data:
                # TODO: bitmap hyperlog
                r.set(item["key"], item["value"])
        if dataAndSchema["schema"]["collection"] == "list":
            for item in data:
                r.rpush(item["key"], *item["value"])
        if dataAndSchema["schema"]["collection"] == "set":
            for item in data:
                r.sadd(item["key"], *item["value"])
        if dataAndSchema["schema"]["collection"] == "hash":
            for item in data:
                r.hset(item["key"], mapping=item["value"])
        if dataAndSchema["schema"]["collection"] == "sortedSet":
            for item in data:
                r.zadd(item["key"], item["value"])

    # print(dbJson)

    source_database_funtions = getDatabaseFuntions(type="REDIS", options=DatabaseFunctionsOptions(
        host=redis_host, username=redis_username, password=redis_password, port=redis_port, dbname=redis_dbname))

    source_database_funtions.backup(f"./downloads/{sql_file_name}")

    pprint("Done!")

except Exception as e:
    pprint(e)
    pprint("Convert fail!")
