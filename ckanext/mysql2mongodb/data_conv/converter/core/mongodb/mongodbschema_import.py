import json
import os
import re
import subprocess
from collections import OrderedDict
import mysql.connector
from pymongo import GEO2D, TEXT


from pymongo_schema.compare import compare_schemas_bases
from pymongo_schema.export import transform_data_to_file
from pymongo_schema.extract import extract_pymongo_client_schema
from pymongo_schema.filter import filter_mongo_schema_namespaces, init_filtered_schema
from pymongo_schema.tosql import mongo_schema_to_mapping


from ckanext.mysql2mongodb.data_conv.core.interfaces.AbstractSchemaConversion import AbstractSchemaConversion
from ckanext.mysql2mongodb.data_conv.core.utilities import open_connection_mongodb, load_mongodb_collection

from ckanext.mysql2mongodb.data_conv.core.database_connection import ConvInitOption, ConvOutputOption
from ckanext.mysql2mongodb.data_conv.core.helper import read_package_config, read_database_config, getDSClient


class MongoSchemaImportConversion(AbstractSchemaConversion):

    def __init__(self):
        super(MongoSchemaImportConversion, self).__init__()
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
        self.convertSchema()
        return True

    def extractSchema(self):
        # extract schema from MongoDB
        schema = extract_pymongo_client_schema(getDSClient())
        return schema

    def convertSchema(self):
        schema = self.extractSchema()

        mapping = mongo_schema_to_mapping(schema)

        print(mapping)

        # save mapping to file
        transform_data_to_file(
            mapping, ['json', 'md'], output='mapping', category='mapping')


if __name__ == '__main__':
    db_conf = read_database_config()
    package_conf = read_package_config()
    CKAN_API_KEY = package_conf["X-CKAN-API-Key"]

    schemaImporter = MongoSchemaImportConversion()

    output_options = ConvOutputOption(
        host=db_conf['mongodb_host'], username=db_conf['mongodb_username'], password=db_conf['mongodb_password'], port=db_conf['mongodb_port'], dbname='sakila')

    init_options = ConvInitOption(
        host=db_conf['mongodb_host'], username=db_conf['mongodb_username'], password=db_conf['mongodb_password'], port=db_conf['mongodb_port'], dbname='sakila')

    schemaImporter.set_config(init_options, output_options)
    schemaImporter.run()
