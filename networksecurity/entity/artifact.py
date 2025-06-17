from dataclasses import dataclass


@dataclass
class DataIngestionArtifact:
    """
    Data Ingestion Artifact class to hold the artifacts related to data ingestion.
    """

    train_file_path: str
    test_file_path: str


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


@dataclass
class DataTransformationArtifact:
    """
    Data Transformation Artifact class to hold the artifacts related to data transformation.
    """

    transformed_train_file_path: str
    transformed_test_file_path: str
    transformed_object_file_path: str


@dataclass
class ClassificationMetricsArtifact:
    f1_score: float
    precision_score: float
    recall_score: float


@dataclass
class ModelTrainerArtifact:
    trained_model_file_path: str
    train_metric_artifact: ClassificationMetricsArtifact
    test_metric_artifact: ClassificationMetricsArtifact
