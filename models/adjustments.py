import datetime
import uuid

from flask import session

from common.database import Database


class Adjustment(object):
    def __init__(self, username, adjustment_name, mlbamid, adjustment,adjustment_id, _id=None):
        self.username = username
        self.adjustment_name = adjustment_name
        self.mlbamid = mlbamid
        self.adjustment = adjustment
        self.adjustment_id = adjustment_id
        self._id = uuid.uuid4().hex if _id is None else _id

    @classmethod
    def get_by_user(cls, username):
        data = Database.find("adjustments", {"username": username})
        if data is not None:
            return [cls(**dat) for dat in data]

    @classmethod
    def get_by_id(cls, _id):
        data = Database.find_one("adjustments", {"_id": _id})
        if data is not None:
            return cls(**data)

    @classmethod
    def get_by_adjustment_id(cls, adjustment_id):
        data = Database.find("adjustments", {"adjustment_id": adjustment_id})
        if data is not None:
            return [cls(**dat) for dat in data]

    @staticmethod
    def save_adjustments(data):
        Database.insert_many("adjustments", data.to_dict('records'))


    def json(self):
        return {
            "username": self.username,
            "adjustment_name": self.adjustment_name,
            "mlbamid": self.mlbamid,
            "adjustment": self.adjustment,
            "adjustment_id": self.adjustment_id,
            "_id": self._id
        }

    def save_to_mongo(self):
        Database.insert("adjustments", self.json())
