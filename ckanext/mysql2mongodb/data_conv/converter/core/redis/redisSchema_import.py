import json
import os
import re
import subprocess
from collections import OrderedDict
import mysql.connector
from pymongo import GEO2D, TEXT
import redis


from ckanext.mysql2mongodb.data_conv.core.utilities import open_connection_mongodb, load_mongodb_collection
from ckanext.mysql2mongodb.data_conv.core.helper import store_collection_to_DS


from ckanext.mysql2mongodb.data_conv.core.interfaces.AbstractSchemaConversion import AbstractSchemaConversion
from ckanext.mysql2mongodb.data_conv.core.helper import store_collection_to_DS


class RedisSchemaImportConversion(AbstractSchemaConversion):
    def __init__(self):
        super(RedisSchemaImportConversion, self).__init__()
        # Define a name for schema file, which will be place at intermediate folder.

    def set_config(self, schema_conv_init_option, schema_conv_output_option):
        self.schema_conv_init_option = schema_conv_init_option
        self.schema_conv_output_option = schema_conv_output_option

    def get(self):
        return None

    def run(self):
        redisSchema = {}
        redisSchema['database-name'] = self.schema_conv_init_option.dbname
        redisSchema["database-version"] = ''
        redisSchema["schema"] = ''
        redisSchema["tables"] = []
        redisSchema["foreign-keys"] = []

        table_info = {}
        table_info["name"] = "string"
        table_info["constraints"] = []
        table_info["triggers"] = []
        table_info["columns"] = [
            {
                "name": "key",
                "column-type": "string"
            },
            {
                "name": "value",
                "column-type": "string"
            }
        ]
        table_info["indexes"] = []
        redisSchema["tables"].append(table_info)

        table_info = {}
        table_info["name"] = "list"
        table_info["constraints"] = []
        table_info["triggers"] = []
        table_info["columns"] = [
            {
                "name": "key",
                "column-type": "string"
            },
            {
                "name": "value",
                "column-type": "array"
            }
        ]
        table_info["indexes"] = []
        redisSchema["tables"].append(table_info)

        table_info = {}
        table_info["name"] = "set"
        table_info["constraints"] = []
        table_info["triggers"] = []
        table_info["columns"] = [
            {
                "name": "key",
                "column-type": "string"
            },
            {
                "name": "value",
                "column-type": "array"
            }
        ]
        table_info["indexes"] = []
        redisSchema["tables"].append(table_info)

        table_info = {}
        table_info["name"] = "hash"
        table_info["constraints"] = []
        table_info["triggers"] = []
        table_info["columns"] = [
            {
                "name": "key",
                "column-type": "string"
            },
            {
                "name": "value",
                "column-type": "object"
            }
        ]
        table_info["indexes"] = []
        redisSchema["tables"].append(table_info)

        table_info = {}
        table_info["name"] = "sortedSet"
        table_info["constraints"] = []
        table_info["triggers"] = []
        table_info["columns"] = [
            {
                "name": "key",
                "column-type": "string"
            },
            {
                "name": "value",
                "column-type": "object"
            }
        ]
        table_info["indexes"] = []
        redisSchema["tables"].append(table_info)
        self.schema = redisSchema
        return True

    def save(self):
        store_collection_to_DS(
            self.schema, self.schema_conv_output_option.dbname)
        return True
