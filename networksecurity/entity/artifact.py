from dataclasses import dataclass

@dataclass
class DataIngestionArtifact:
    """
    Data Ingestion Artifact class to hold the artifacts related to data ingestion.
    """
    train_file_path: str
    test_file_path: str

    def __post_init__(self):
        # You can add any additional initialization logic here if needed
        pass

@dataclass
class DataValidationArtifact:
    """
    Data Validation Artifact class to hold the artifacts related to data validation.
    """
    validation_status: bool
    valid_train_file_path: str
    valid_test_file_path: str
    invalid_train_file_path: str | None
    invalid_test_file_path: str | None
    drift_report_file_path: str

    def __post_init__(self):
        # You can add any additional initialization logic here if needed
        pass
