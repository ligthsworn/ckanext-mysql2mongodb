from .core.mysql.mysqlSchema_import import MySQLSchemaImportConversion
from .core.mysql.mysqlData_import import MysqlDataImportConversion
from ..core.interfaces.AbstractSchemaConversion import AbstractSchemaConversion
from ..core.interfaces.AbstractDataConversion import AbstractDataConversion
from ..core.mongodb.mongodbschema_import import MongoSchemaImportConversion


class ConverterFactory:
    def getSchemaImporter(self, type: str) -> AbstractSchemaConversion:
        if type == "MYSQL":
            return MySQLSchemaImportConversion()
        if type == "MONGO":
            return MongoSchemaImportConversion()

        else:
            raise Exception('Unknown Importer')

    def getDataImporter(self, type: str) -> AbstractDataConversion:
        if type == "MYSQL":
            return MysqlDataImportConversion()
        else:
            raise Exception('Unknown Importer')
