from networksecurity.logging.logger import logging
from networksecurity.entity.config import DataIngestionConfig
from networksecurity.entity.artifact import DataIngestionArtifact
from networksecurity.exception.exception import NetworkSecurityException

import os
import sys
import pymongo
import numpy as np
import pandas as pd
from typing import List
from dotenv import load_dotenv
from sklearn.model_selection import train_test_split

## Configuration of the data ingestion config

load_dotenv()

MONGO_DB_URL = os.getenv("MONGO_DB_URL")


class DataIngestion:
    def __init__(self, data_ingestion_config: DataIngestionConfig) -> None:
        try:
            self.data_ingestion_config = data_ingestion_config
        except Exception as e:
            raise NetworkSecurityException(e, sys)  # type: ignore

    def export_collection_as_dataframe(self):
        """
        Read data from MongoDB collection and convert it to a pandas DataFrame.
        """
        try:
            database_name = self.data_ingestion_config.database_name
            collection_name = self.data_ingestion_config.collection_name
            self.mongo_client = pymongo.MongoClient(MONGO_DB_URL)
            collection = self.mongo_client[database_name][collection_name]
            dataframe = pd.DataFrame(list(collection.find()))
            if "_id" in dataframe.columns.to_list():
                dataframe.drop(columns=["_id"], inplace=True)
            dataframe.replace({"na": np.nan}, inplace=True)
            return dataframe
        except Exception as e:
            raise NetworkSecurityException(e, sys)  # type: ignore

    def export_data_to_feature_store(self, dataframe: pd.DataFrame):
        """
        Save the DataFrame to a CSV file in the feature store directory.
        """
        try:
            feature_store_file_path = self.data_ingestion_config.feature_store_file_path
            os.makedirs(os.path.dirname(feature_store_file_path), exist_ok=True)
            dataframe.to_csv(feature_store_file_path, index=False, header=True)
            return dataframe
        except Exception as e:
            raise NetworkSecurityException(e, sys)  # type: ignore

    def split_data_as_train_test(self, dataframe: pd.DataFrame):
        """
        Split the DataFrame into training and testing sets.
        """
        try:
            logging.info("Performed train test split on the data")
            train_set, test_set = train_test_split(
                dataframe,
                test_size=self.data_ingestion_config.train_test_split_ratio,
                random_state=42,
            )
            logging.info("Exited split_data_as_train_test method")
            train_file_path = self.data_ingestion_config.training_file_path
            test_file_path = self.data_ingestion_config.testing_file_path

            logging.info("Exporting train and test data to CSV files")
            os.makedirs(os.path.dirname(train_file_path), exist_ok=True)
            os.makedirs(os.path.dirname(test_file_path), exist_ok=True)
            train_set.to_csv(train_file_path, index=False, header=True)
            test_set.to_csv(test_file_path, index=False, header=True)
            logging.info("Exported train and test data to CSV files")
        except Exception as e:
            raise NetworkSecurityException(e, sys)  # type: ignore

    def initiate_data_ingestion(self):
        try:
            dataframe = self.export_collection_as_dataframe()
            dataframe = self.export_data_to_feature_store(dataframe)
            self.split_data_as_train_test(dataframe)
            data_ingestion_artifact = DataIngestionArtifact(
                train_file_path=self.data_ingestion_config.training_file_path,
                test_file_path=self.data_ingestion_config.testing_file_path
            )
            return data_ingestion_artifact
        except Exception as e:
            raise NetworkSecurityException(e, sys)  # type: ignore
