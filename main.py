import sys
from networksecurity.logging.logger import logging
from networksecurity.entity.config import (
    DataIngestionConfig,
    DataValidationConfig,
    DataTransformationConfig,
    TrainingPipelineConfig,
)
from networksecurity.components.data_ingestion import DataIngestion
from networksecurity.components.data_validation import DataValidation
from networksecurity.components.data_transformation import DataTransformation
from networksecurity.exception.exception import NetworkSecurityException

if __name__ == "__main__":
    try:
        logging.info("Starting data ingestion process...")
        training_pipeline_config = TrainingPipelineConfig()
        data_ingestion_config = DataIngestionConfig(
            training_pipeline_config=training_pipeline_config
        )
        data_validation_config = DataValidationConfig(
            training_pipeline_config=training_pipeline_config
        )
        data_transformation_config = DataTransformationConfig(
            training_pipeline_config=training_pipeline_config
        )
        data_ingestion = DataIngestion(data_ingestion_config=data_ingestion_config)
        logging.info("Initiate data ingestion...")
        artifact = data_ingestion.initiate_data_ingestion()
        logging.info("Data ingestion process completed successfully.")
        print(artifact)

        data_validation = DataValidation(
            data_ingestion_artifact=artifact,
            data_validation_config=data_validation_config,
        )
        logging.info("Initiating data validation...")
        artifact = data_validation.initiate_data_validation()
        logging.info("Data validation process completed successfully.")
        print(artifact)

        data_transformation = DataTransformation(
            data_transformation_config=data_transformation_config,
            data_validation_artifact=artifact,
        )
        logging.info(f"Initiate data transformation...")
        artifact = data_transformation.initiate_data_transformation()
        logging.info(f"Data transformation process completed successfully.")
        print(artifact)

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise NetworkSecurityException(e, sys)  # type: ignore
