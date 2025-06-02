from networksecurity.logging.logger import logging
from networksecurity.entity.config import DataIngestionConfig
from networksecurity.entity.config import TrainingPipelineConfig
from networksecurity.components.data_ingestion import DataIngestion
from networksecurity.exception.exception import NetworkSecurityException

if __name__ == "__main__":
    try:
        logging.info("Starting data ingestion process...")
        training_pipeline_config = TrainingPipelineConfig()
        data_ingestion_config = DataIngestionConfig(training_pipeline_config=training_pipeline_config)
        data_ingestion = DataIngestion(data_ingestion_config=data_ingestion_config)
        
        artifact = data_ingestion.initiate_data_ingestion()
        print(artifact)
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise NetworkSecurityException(e, sys) # type: ignore