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