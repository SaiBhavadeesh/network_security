import yaml
import pickle
import os, sys
import numpy as np
from sklearn.metrics import r2_score
from sklearn.model_selection import GridSearchCV
from networksecurity.logging.logger import logging
from networksecurity.exception.exception import NetworkSecurityException


def read_yaml_file(file_path: str) -> dict:
    """
    Reads a YAML file and returns its content as a dictionary.
    """
    try:
        with open(file_path, "rb") as file:
            return yaml.safe_load(file)
    except Exception as e:
        raise NetworkSecurityException(e, sys)  # type: ignore


def write_yaml_file(file_path: str, content: object, replace: bool = False) -> None:
    """
    Writes content to a YAML file. If replace is True, it overwrites the file.
    """
    try:
        if replace:
            if os.path.exists(file_path):
                os.remove(file_path)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w") as file:
            yaml.dump(content, file)
    except Exception as e:
        raise NetworkSecurityException(e, sys)  # type: ignore


def save_numpy_array_data(file_path: str, array: np.ndarray) -> None:
    """
    Saves a NumPy array to a file.
    """
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "wb") as file_obj:
            np.save(file_obj, array)
    except Exception as e:
        raise NetworkSecurityException(e, sys)  # type: ignore


def load_numpy_array_data(file_path: str) -> np.ndarray:
    """
    Saves a NumPy array from a file.
    """
    try:
        if not os.path.exists(file_path):
            raise Exception(f"The file: {file_path} is not exist")
        with open(file_path, "rb") as file_obj:
            return np.load(file_obj)
    except Exception as e:
        raise NetworkSecurityException(e, sys)  # type: ignore


def save_object(file_path: str, obj: object) -> None:
    """
    Saves an object to a file using pickle.
    """
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "wb") as file_obj:
            pickle.dump(obj, file_obj)
    except Exception as e:
        raise NetworkSecurityException(e, sys)  # type: ignore


def load_object(file_path: str) -> object:
    """
    Loads an object to a file using pickle.
    """
    try:
        if not os.path.exists(file_path):
            raise Exception(f"The file: {file_path} is not exist")
        with open(file_path, "rb") as file_obj:
            print(file_obj)
            return pickle.load(file_obj)
    except Exception as e:
        raise NetworkSecurityException(e, sys)  # type: ignore


def evaluate_models(X_train, y_train, X_test, y_test, models: dict, params: dict):
    try:
        report = {}
        for i in range(len(list(models))):
            model = list(models.values())[i]
            param = params[list(models.keys())[i]]
            gs = GridSearchCV(model, param, cv=3)
            gs.fit(X_train, y_train)
            
            model.set_params(**gs.best_params_)
            model.fit(X_train, y_train)
            y_train_pred = model.predict(X_train)
            y_test_pred = model.predict(X_test)
            train_model_score = r2_score(y_train, y_train_pred)
            logging.info(
                f"{list(models.keys())[i]} Train Model Score - {train_model_score}"
            )
            test_model_score = r2_score(y_test, y_test_pred)
            report[list(models.keys())[i]] = test_model_score
        return report
    except Exception as e:
        raise NetworkSecurityException(e, sys)  # type: ignore
