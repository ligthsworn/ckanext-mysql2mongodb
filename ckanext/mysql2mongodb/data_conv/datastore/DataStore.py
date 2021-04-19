import pymongo
import dns
from DatabaseOptions import DatabaseOptions
from bson.json_util import dumps


class DataStore:

    def __init__(self, connectionString):
        self.client = pymongo.MongoClient(connectionString)

    def exportJSON(self, dbOptions: DatabaseOptions):
        dbName = dbOptions.dbName
        db = self.client[dbName]

        collections = db.list_collection_names()
        list_collections = []

        for collection in collections:
            data = list(db[collection].find({}, {'_id': 0}))
            list_collections.append({collection: data})

        return list_collections

    def exportJSONFile(self, dbOptions: DatabaseOptions):
        data = self.exportJSON(dbOptions)

        with open(f'{dbOptions.dbName}.json', 'w') as file:
            file.write(dumps(data))

    def saveSchema(self, dbOptions: DatabaseOptions, schema):
        dbName = dbOptions.dbName
        db = self.client[dbName]

        mycol = db["schema_view"]
        mycol.insert_one(schema)
