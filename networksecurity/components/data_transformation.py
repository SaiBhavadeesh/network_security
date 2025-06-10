import sys, os
import numpy as np
import pandas as pd
from sklearn.impute import KNNImputer
from sklearn.pipeline import Pipeline
from networksecurity.constants.training_pipeline import TARGET_COLUMN
from networksecurity.constants.training_pipeline import (
    DATA_TRANSFORMATION_IMPUTER_PARAMS,
)
from networksecurity.entity.artifact import (
    DataTransformationArtifact,
    DataValidationArtifact,
)
from networksecurity.entity.config import DataTransformationConfig
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
from networksecurity.utils.main_utils.utils import save_numpy_array_data, save_object


class DataTransformation:
    def __init__(
        self,
        data_validation_artifact: DataValidationArtifact,
        data_transformation_config: DataTransformationConfig,
    ):
        try:
            self.data_validation_artifact = data_validation_artifact
            self.data_transformation_config = data_transformation_config
        except Exception as e:
            raise NetworkSecurityException(e, sys)  # type: ignore

    @staticmethod
    def read_data(file_path: str) -> pd.DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise NetworkSecurityException(e, sys)  # type: ignore

    def get_data_transformer_object(self) -> Pipeline:
        """
        It initialises KNNImputer object with the parameters specified in the training_pipeline.py file
        and returns a Pipeline object with the KNNImputer object as the first step.
        
        Returns:
            A pipeline object
        """
        logging.info(
            f"Entered get_data_transformer_object method of Transformation class"
        )
        try:
            imputer = KNNImputer(**DATA_TRANSFORMATION_IMPUTER_PARAMS)
            logging.info(
                f"Initialise KNNImputer with {DATA_TRANSFORMATION_IMPUTER_PARAMS}"
            )
            processor = Pipeline([("imputer", imputer)])
            return processor
        except Exception as e:
            raise NetworkSecurityException(e, sys)  # type: ignore

    def initiate_data_transformation(self) -> DataTransformationArtifact:
        try:
            logging.info(f"Data Transformation started.")
            train_dataframe = DataTransformation.read_data(
                self.data_validation_artifact.valid_train_file_path
            )
            test_dataframe = DataTransformation.read_data(
                self.data_validation_artifact.valid_test_file_path
            )

            ## training dataframe
            input_feature_train_dataframe = train_dataframe.drop(
                columns=[TARGET_COLUMN]
            )
            target_feature_train_dataframe = train_dataframe[TARGET_COLUMN]
            target_feature_train_dataframe = target_feature_train_dataframe.replace(
                -1, 0
            )
            ## testing dataframe
            input_feature_test_dataframe = test_dataframe.drop(columns=[TARGET_COLUMN])
            target_feature_test_dataframe = test_dataframe[TARGET_COLUMN]
            target_feature_test_dataframe = target_feature_test_dataframe.replace(-1, 0)

            preprocessor = self.get_data_transformer_object()
            preprocessor_obj = preprocessor.fit(input_feature_train_dataframe)
            transformed_input_train_feature = preprocessor_obj.transform(
                input_feature_train_dataframe
            )
            transformed_input_test_feature = preprocessor_obj.transform(
                input_feature_test_dataframe
            )

            train_arr = np.c_[
                transformed_input_train_feature,
                np.array(target_feature_train_dataframe),
            ]
            test_arr = np.c_[
                transformed_input_test_feature, np.array(target_feature_test_dataframe)
            ]

            ## Save numpy array data
            save_numpy_array_data(
                self.data_transformation_config.transformed_train_file_path,
                array=train_arr,
            )
            save_numpy_array_data(
                self.data_transformation_config.transformed_test_file_path,
                array=test_arr,
            )
            save_object(
                self.data_transformation_config.transformed_object_file_path,
                obj=preprocessor_obj,
            )

            ## Preparing artifcat
            return DataTransformationArtifact(
                transformed_object_file_path=self.data_transformation_config.transformed_object_file_path,
                transformed_train_file_path=self.data_transformation_config.transformed_train_file_path,
                transformed_test_file_path=self.data_transformation_config.transformed_test_file_path,
            )
        except Exception as e:
            raise NetworkSecurityException(e, sys)  # type: ignore
