#!/usr/bin/env python3

# database_config.py: Classes which are used for construct database connection
import os
import subprocess
from ..database_function import DatabaseFunctions, DatabaseFunctionsOptions
import pprint


class MysqlDatabaseFunctions(DatabaseFunctions):
    """
    Class Conversion Initialized Connection Option.
    This class is usually used for MySQL connection.
    """

    def __init__(self, options: DatabaseFunctionsOptions):
        super(MysqlDatabaseFunctions, self).__init__(options)

    def restore(self, filePath):
        # if environment has installed mysql

        subprocess.run(
            [f"mysql -h {self.options.host} --user={self.options.username} --password={self.options.password} -e \"CREATE DATABASE IF NOT EXISTS {self.options.dbname}\""], check=True, shell=True)
        subprocess.run(
            [f"mysql -h {self.options.host} --user={self.options.username} --password={self.options.password} {self.options.dbname} < {filePath}"], check=True, shell=True)

    def backup(self, filePath):
        pass
