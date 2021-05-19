#!/usr/bin/env python3

# database_config.py: Classes which are used for construct database connection
import os
from ..database_function import DatabaseFunctions, DatabaseFunctionsOptions


class RedisDatabaseFunctions(DatabaseFunctions):
    """
    Class Conversion Initialized Connection Option.
    This class is usually used for Redis connection.
    """

    def __init__(self, options: DatabaseFunctionsOptions):
        super(RedisDatabaseFunctions, self).__init__(options)

    def restore(self, filePath):
        os.system(
            f"docker stop {self.options.host}")
        os.system(
            f"docker cp {filePath} {self.options.host}:/data/dump.rdb")
        os.system(
            f"docker start {self.options.host}")

    def backup(self, filePath):
        os.system(
            f"docker exec -it {self.options.host} redis-cli -a {self.options.password} -p {self.options.port} save")
        os.system(
            f"docker cp {self.options.host}:/data/dump.rdb {filePath}")
