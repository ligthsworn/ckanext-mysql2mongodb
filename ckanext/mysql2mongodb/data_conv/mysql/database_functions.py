#!/usr/bin/env python3

# database_config.py: Classes which are used for construct database connection
import os
from ..core.database_function import DatabaseFunctions, DatabaseFunctionsOptions


class MysqlDatabaseFunctions(DatabaseFunctions):
    """
    Class Conversion Initialized Connection Option.
    This class is usually used for MySQL connection.
    """

    def __init__(self, options: DatabaseFunctionsOptions):
        super(MysqlDatabaseFunctions, self).__init__(options)

    def restore(self, filePath):

        # if environment has installed mysql
        os.system(
            f"mysql -h {self.options.host} -u {self.options.username} --password={self.options.password} -e \"CREATE DATABASE IF NOT EXISTS {self.options.dbname}\"")
        os.system(
            f"mysql -h {self.options.host} -u {self.options.username} --password={self.options.password} {self.options.dbname} < {filePath}")

        # if environment hasn installed mysql in docker container
        # os.system(
        #     f"docker exec -it {self.options.host} mysql -u {self.options.username} --password={self.options.password} -e \"CREATE DATABASE IF NOT EXISTS {self.options.dbname}\"")
        # os.system(
        #     f"docker cp {filePath} {self.options.host}:/{self.options.dbname}.sql")
        # os.system(
        #     f"docker exec -it {self.options.host} sh -c \" mysql -u {self.options.username} --password={self.options.password} {self.options.dbname} < /{self.options.dbname}.sql \"")

    def backup(self, filePath):
        pass
