import os, sys

from networksecurity.cloud.s3_syncer import S3Sync
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
from networksecurity.components.data_ingestion import DataIngestion
from networksecurity.components.data_validation import DataValidation
from networksecurity.components.data_transformation import DataTransformation
from networksecurity.components.model_trainer import ModelTrainer
from networksecurity.constants.training_pipeline import TRAINING_BUCKET_NAME
from networksecurity.entity.config import (
    TrainingPipelineConfig,
    DataIngestionConfig,
    DataTransformationConfig,
    DataValidationConfig,
    ModelTrainerConfig,
)
from networksecurity.entity.artifact import (
    DataIngestionArtifact,
    DataValidationArtifact,
    DataTransformationArtifact,
    ModelTrainerArtifact,
)


class Training:
    def __init__(self) -> None:
        self.training_pipeline_config = TrainingPipelineConfig()
        self.s3_sync = S3Sync()

    def start_data_ingestion(self):
        try:
            self.data_ingestion_config = DataIngestionConfig(
                self.training_pipeline_config
            )
            logging.info("Start data ingestion")
            data_ingestion = DataIngestion(self.data_ingestion_config)
            data_ingestion_artifact = data_ingestion.initiate_data_ingestion()
            logging.info(f"Data ingestion completed - {data_ingestion_artifact}")
            return data_ingestion_artifact
        except Exception as e:
            raise NetworkSecurityException(e, sys)  # type: ignore

    def start_data_validation(self, data_ingestion_artifact: DataIngestionArtifact):
        try:
            self.data_validation_config = DataValidationConfig(
                self.training_pipeline_config
            )
            logging.info("Start data validation")
            data_validation = DataValidation(
                data_ingestion_artifact, self.data_validation_config
            )
            data_validation_artifact = data_validation.initiate_data_validation()
            logging.info(f"Data validation completed - {data_validation_artifact}")
            return data_validation_artifact
        except Exception as e:
            raise NetworkSecurityException(e, sys)  # type: ignore

    def start_data_transformation(
        self, data_validation_artifact: DataValidationArtifact
    ):
        try:
            self.data_transformation_config = DataTransformationConfig(
                self.training_pipeline_config
            )
            logging.info("Start data transformation")
            data_transformation = DataTransformation(
                data_validation_artifact, self.data_transformation_config
            )
            data_transformation_artifact = (
                data_transformation.initiate_data_transformation()
            )
            logging.info(
                f"Data transformation completed - {data_transformation_artifact}"
            )
            return data_transformation_artifact
        except Exception as e:
            raise NetworkSecurityException(e, sys)  # type: ignore

    def start_model_trainer(
        self, data_transformation_artifact: DataTransformationArtifact
    ):
        try:
            self.model_trainer_config = ModelTrainerConfig(
                self.training_pipeline_config
            )
            logging.info("Start model trainer")
            model_trainer = ModelTrainer(
                data_transformation_artifact, self.model_trainer_config
            )
            model_trainer_artifact: ModelTrainerArtifact = (
                model_trainer.initiate_model_trainer()
            )
            logging.info(f"Model trainer completed - {model_trainer_artifact}")
            return model_trainer_artifact
        except Exception as e:
            raise NetworkSecurityException(e, sys)  # type: ignore

    def sync_artifact_dir_to_s3(self):
        try:
            aws_bucket_url = f"s3://{TRAINING_BUCKET_NAME}/artifact/{self.training_pipeline_config.timestamp}"
            self.s3_sync.sync_folder_to_s3(
                folder=self.training_pipeline_config.artifact_dir,
                aws_bucket_url=aws_bucket_url,
            )
        except Exception as e:
            raise NetworkSecurityException(e, sys)  # type: ignore

    def sync_saved_model_dir_to_s3(self):
        try:
            aws_bucket_url = f"s3://{TRAINING_BUCKET_NAME}/models/{self.training_pipeline_config.timestamp}"
            self.s3_sync.sync_folder_to_s3(
                folder=self.training_pipeline_config.model_dir,
                aws_bucket_url=aws_bucket_url,
            )
        except Exception as e:
            raise NetworkSecurityException(e, sys)  # type: ignore

    def run_pipeline(self):
        try:
            data_ingestion_artifact = self.start_data_ingestion()
            data_validation_artifact = self.start_data_validation(
                data_ingestion_artifact
            )
            data_transformation_artifact = self.start_data_transformation(
                data_validation_artifact
            )
            model_trainer_artifact = self.start_model_trainer(
                data_transformation_artifact
            )

            self.sync_artifact_dir_to_s3()
            self.sync_saved_model_dir_to_s3()
            
            return model_trainer_artifact
        except Exception as e:
            raise NetworkSecurityException(e, sys)  # type: ignore
