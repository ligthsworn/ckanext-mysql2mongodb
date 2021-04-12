from .database_function import DatabaseFunctions, DatabaseFunctionsOptions
from .mysql.database_functions import MysqlDatabaseFunctions
from .mongo.database_functions import MongoDatabaseFunctions


def getDatabaseFuntions(type: str, options: DatabaseFunctionsOptions) -> DatabaseFunctions:
    if type == "MYSQL":
        return MysqlDatabaseFunctions(options=options)
    if type == "MONGO":
        return MongoDatabaseFunctions(options=options)

    return DatabaseFunctions(options=options)
