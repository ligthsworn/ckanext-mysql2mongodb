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


class RedisSchemaExportConversion(AbstractSchemaConversion):
    def __init__(self):
        super(RedisSchemaExportConversion, self).__init__()
        # Define a name for schema file, which will be place at intermediate folder.

    def set_config(self, schema_conv_init_option, schema_conv_output_option):
        self.schema_conv_init_option = schema_conv_init_option
        self.schema_conv_output_option = schema_conv_output_option

    def get(self):
        return None

    def run(self):
        return True

    def save(self):
        return True
