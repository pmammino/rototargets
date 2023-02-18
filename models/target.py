import datetime
import uuid
from bson import ObjectId

from flask import session

from common.database import Database

class Target(object):
    def __init__(self, username, target_name,HR,AVG,R,RBI,SB,W,WHIP,ERA,SO,SV,hitters,pitchers,_id=None):
        self.username = username
        self.target_name = target_name
        self.HR = HR,
        self.AVG = AVG,
        self.R = R,
        self.RBI = RBI,
        self.SB = SB,
        self.W = W,
        self.WHIP = WHIP,
        self.ERA = ERA,
        self.SO = SO,
        self.SV = SV,
        self.hitters = hitters,
        self.pitchers = pitchers,
        self._id = uuid.uuid4().hex if _id is None else _id

    @classmethod
    def get_by_user(cls, username):
        data = Database.find("targets", {"$or":[{"username": username},
                                                 {"username":"default"}]})
        if data is not None:
            return [cls(**dat) for dat in data]

    @classmethod
    def get_by_id(cls, _id):
        data = Database.find_one("targets", {"$or":[{"_id": _id},
                                                 {"_id":ObjectId(_id)}]})
        if data is not None:
            return cls(**data)

    @staticmethod
    def save_targets(data):
        Database.insert_many("targets", data.to_dict('records'))

    @classmethod
    def get_by_user_name(cls, username,target_name):
        data = Database.find_one("targets", {"$and":[{"username": username},
                                                    {"target_name": target_name}]})
        if data is not None:
            return cls(**data)


    def json(self):
        return {
            "username": self.username,
            "target_name": self.target_name,
            "HR":self.HR,
            "AVG":self.AVG,
            "R":self.R,
            "RBI":self.RBI,
            "SB":self.SB,
            "W":self.W,
            "WHIP":self.WHIP,
            "ERA":self.ERA,
            "SO":self.SO,
            "SV":self.SV,
            "hitters":self.hitters,
            "pitchers":self.pitchers,
            "_id": self._id
        }

    def save_to_mongo(self):
        Database.insert("targets", self.json())
