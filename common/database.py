import pymongo
#import srv as srv


class Database(object):
    ##URI = "mongodb://127.0.0.1:27017"
    URI = "mongodb+srv://admin:emnisnehuI17N05m@cluster1.lqmj32j.mongodb.net/?retryWrites=true&w=majority"
    DATABASE = None

    @staticmethod
    def initialize():
        client = pymongo.MongoClient(Database.URI)
        Database.DATABASE = client["rototargets"]

    @staticmethod
    def insert(collection, data):
        Database.DATABASE[collection].insert(data)

    @staticmethod
    def insert_many(collection, data):
        Database.DATABASE[collection].insert_many(data)

    @staticmethod
    def find(collection, query):
        return Database.DATABASE[collection].find(query)

    @staticmethod
    def find_one(collection, query):
        return Database.DATABASE[collection].find_one(query)

    @staticmethod
    def update_one(collection, query, new):
        return Database.DATABASE[collection].update_one(query, new, upsert=True)

    @staticmethod
    def update_many(collection, query, new):
        return Database.DATABASE[collection].update_many(query, new)

    @staticmethod
    def delete_one(collection, query):
        return Database.DATABASE[collection].delete_one(query)

    @staticmethod
    def delete_many(collection, query):
        return Database.DATABASE[collection].delete_many(query)
