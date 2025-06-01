import os
import sys
import json
import certifi
import pymongo
import numpy as np
import pandas as pd

from dotenv import load_dotenv
from networksecurity.logging.logger import logging
from networksecurity.exception.exception import NetworkSecurityException

load_dotenv()

MONGO_DB_URL = os.getenv("MONGO_DB_URL")

ca = certifi.where()


class NetworkDataExtract:
    def __init__(self) -> None:
        try:
            pass
        except Exception as e:
            raise NetworkSecurityException(e, sys)  # type: ignore

    def csv_to_json_converter(self, file_path: str) -> list:
        try:
            data = pd.read_csv(file_path)
            data.reset_index(drop=True, inplace=True)
            records = list(json.loads(data.T.to_json()).values())
            return records
        except Exception as e:
            raise NetworkSecurityException(e, sys)  # type: ignore

    def insert_data_to_mongo(self, records, database, collection):
        try:
            self.database = database
            self.collection = collection
            self.records = records

            self.mongo_client = pymongo.MongoClient(MONGO_DB_URL)
            self.database = self.mongo_client[self.database]

            self.collection = self.database[self.collection]
            self.collection.insert_many(self.records)
            return len(self.records)
        except Exception as e:
            raise NetworkSecurityException(e, sys)  # type: ignore


if __name__ == "__main__":
    FILE_PATH = "network_data\\phisingData.csv"
    DATABSE = "data_science"
    COLLECTION = "network_data"

    networkETLobj = NetworkDataExtract()
    records = networkETLobj.csv_to_json_converter(FILE_PATH)
    print(records)
    no_of_records = networkETLobj.insert_data_to_mongo(records, DATABSE, COLLECTION)
    print(no_of_records)
