import sys
from networksecurity.entity.artifact import ClassificationMetricsArtifact
from networksecurity.exception.exception import NetworkSecurityException
from sklearn.metrics import f1_score, precision_score, recall_score


def get_classification_score(y_true, y_pred) -> ClassificationMetricsArtifact:
    try:
        model_f1_score = f1_score(y_true, y_pred)
        model_precision_score = precision_score(y_true, y_pred)
        model_recall_score = recall_score(y_true, y_pred)

        return ClassificationMetricsArtifact(
            f1_score=model_f1_score,  # type: ignore
            precision_score=model_precision_score,  # type: ignore
            recall_score=model_recall_score,  # type: ignore
        )
    except Exception as e:
        raise NetworkSecurityException(e, sys)  # type: ignore
