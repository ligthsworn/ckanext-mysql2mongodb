#!/usr/bin/env python3

# database_config.py: Classes which are used for construct database connection
import os
import subprocess
from ..database_function import DatabaseFunctions, DatabaseFunctionsOptions
from pathlib import Path


class MongoDatabaseFunctions(DatabaseFunctions):
    """
    Class Conversion Initialized Connection Option.
    This class is usually used for Mongodb connection.
    """

    def __init__(self, options: DatabaseFunctionsOptions):
        super(MongoDatabaseFunctions, self).__init__(options)

    def restore(self, filePath):
        bak_Path = Path(filePath)
        parent_dir = bak_Path.parent.absolute()
        file_name = bak_Path.stem
        subprocess.run(
            [f"mkdir -p {parent_dir}/{file_name}"], check=True, shell=True)

        subprocess.run(
            [f"unzip {filePath} -d {parent_dir}/{file_name}"], check=True, shell=True)
        subprocess.run(
            [f"mongorestore --username {self.options.username} --password {self.options.password} --host {self.options.host} --port {self.options.port} --db {file_name} --authenticationDatabase admin --dir {parent_dir}/{file_name}"], check=True, shell=True)

    def backup(self, filePath):
        subprocess.run(
            [f"mongodump --username {self.options.username} --password {self.options.password} --host {self.options.host} --port {self.options.port} --authenticationDatabase admin --db {self.options.dbname} --forceTableScan -o {filePath}"], check=True, shell=True)
