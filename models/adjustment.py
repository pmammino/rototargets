import datetime
import uuid

from flask import session

from common.database import Database

class Adjust(object):
    def __init__(self, username, adjustment_name, _id=None):
        self.username = username
        self.adjustment_name = adjustment_name
        self._id = uuid.uuid4().hex if _id is None else _id

    @classmethod
    def get_by_user(cls, username):
        data = Database.find("adjustment", {"username": username})
        if data is not None:
            return [cls(**dat) for dat in data]

    @classmethod
    def get_by_id(cls, _id):
        data = Database.find_one("adjustment", {"_id": _id})
        if data is not None:
            return cls(**data)

    @staticmethod
    def save_adjustments(data):
        Database.insert_many("adjustment", data.to_dict('records'))

    @classmethod
    def get_by_user_name(cls, username,adjustment_name):
        data = Database.find_one("adjustment", {"$and":[{"username": username},
                                                    {"adjustment_name": adjustment_name}]})
        if data is not None:
            return cls(**data)


    def json(self):
        return {
            "username": self.username,
            "adjustment_name": self.adjustment_name,
            "_id": self._id
        }

    def save_to_mongo(self):
        Database.insert("adjustment", self.json())
