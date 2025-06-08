from networksecurity.entity.artifact import (
    DataIngestionArtifact,
    DataValidationArtifact,
)
from networksecurity.entity.config import DataValidationConfig
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.utils.main_utils.utils import read_yaml_file, write_yaml_file
from networksecurity.logging.logger import logging
from networksecurity.constants.training_pipeline import SCHEMA_FILE_PATH
from scipy.stats import ks_2samp
import pandas as pd
import os, sys


class DataValidation:
    def __init__(
        self,
        data_ingestion_artifact: DataIngestionArtifact,
        data_validation_config: DataValidationConfig,
    ) -> None:
        try:
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_validation_config = data_validation_config
            self._schema_file_path = read_yaml_file(SCHEMA_FILE_PATH)
        except Exception as e:
            raise NetworkSecurityException(e, sys)  # type: ignore

    @staticmethod
    def read_data(file_path: str) -> pd.DataFrame:
        """
        Reads a CSV file and returns a DataFrame.
        """
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise NetworkSecurityException(e, sys)  # type: ignore

    def validate_number_of_columns(self, dataframe: pd.DataFrame) -> bool:
        """
        Validates the number of columns in the DataFrame against the schema.
        """
        try:
            expected_columns = self._schema_file_path["columns"]
            actual_columns = dataframe.columns.tolist()
            logging.info(f"Expected columns: {len(expected_columns)}")
            logging.info(f"Actual columns: {len(actual_columns)}")
            if len(actual_columns) == len(expected_columns):
                return True
            return False
        except Exception as e:
            raise NetworkSecurityException(e, sys)  # type: ignore

    def validate_numerical_columns(self, dataframe: pd.DataFrame) -> bool:
        """
        Validates the numerical columns in the DataFrame against the schema.
        """
        try:
            expected_numerical_columns = self._schema_file_path["numerical_columns"]
            actual_numerical_columns = dataframe.select_dtypes(
                include=["number"]
            ).columns.tolist()
            logging.info(f"Expected numerical columns: {expected_numerical_columns}")
            logging.info(f"Actual numerical columns: {actual_numerical_columns}")
            if set(actual_numerical_columns) == set(expected_numerical_columns):
                return True
            return False
        except Exception as e:
            raise NetworkSecurityException(e, sys)  # type: ignore

    def detect_data_drift(
        self,
        base_dataframe: pd.DataFrame,
        current_dataframe: pd.DataFrame,
        threshold=0.05,
    ) -> bool:
        """
        Detect data drift between the base and current DataFrames using the Kolmogorov-Smirnov test.
        """
        try:
            drift_detected = True
            report = {}
            for column in base_dataframe.columns:
                base_data = base_dataframe[column]
                current_data = current_dataframe[column]
                _, pvalue = ks_2samp(base_data, current_data)
                if threshold <= float(pvalue): # type: ignore
                    is_found = False
                else:
                    is_found = True
                    drift_detected = False
                report.update(
                    {
                        column: {
                            "p_value": float(pvalue),  # type: ignore
                            "drift_detected": is_found,
                        }
                    }
                )
            drift_report_file_path = self.data_validation_config.drift_report_file_path
            os.makedirs(os.path.dirname(drift_report_file_path), exist_ok=True)
            write_yaml_file(
                file_path=drift_report_file_path, content=report, replace=True
            )
            return drift_detected
        except Exception as e:
            raise NetworkSecurityException(e, sys)  # type: ignore

    def initiate_data_validation(self) -> DataValidationArtifact:
        try:
            train_file_path = self.data_ingestion_artifact.train_file_path
            test_file_path = self.data_ingestion_artifact.test_file_path

            ## Read the data from train and test files
            train_dataframe = DataValidation.read_data(train_file_path)
            test_dataframe = DataValidation.read_data(test_file_path)

            ## Validate the number of columns
            status = self.validate_number_of_columns(train_dataframe)
            if not status:
                error_message = (
                    f"Train dataframe does not have the expected number of columns.\n"
                )
            status = self.validate_number_of_columns(test_dataframe)
            if not status:
                error_message = (
                    f"Test dataframe does not have the expected number of columns.\n"
                )

            ## Validate the numerical columns
            status = self.validate_numerical_columns(train_dataframe)
            if not status:
                error_message = (
                    f"Train dataframe does not have the expected numerical columns.\n"
                )

            ## let's check data drift
            drift_status = self.detect_data_drift(
                base_dataframe=train_dataframe, current_dataframe=test_dataframe
            )
            dir_path = os.path.dirname(
                self.data_validation_config.valid_train_file_path
            )
            os.makedirs(dir_path, exist_ok=True)

            train_dataframe.to_csv(
                self.data_validation_config.valid_train_file_path,
                index=False,
                header=True,
            )
            test_dataframe.to_csv(
                self.data_validation_config.valid_test_file_path,
                index=False,
                header=True,
            )

            return DataValidationArtifact(
                validation_status=drift_status,
                valid_train_file_path=self.data_ingestion_artifact.train_file_path,
                valid_test_file_path=self.data_ingestion_artifact.test_file_path,
                invalid_train_file_path=None,
                invalid_test_file_path=None,
                drift_report_file_path=self.data_validation_config.drift_report_file_path,
            )
        except Exception as e:
            raise NetworkSecurityException(e, sys)  # type: ignore
