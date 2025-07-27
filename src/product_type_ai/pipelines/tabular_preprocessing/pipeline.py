"""
This is a boilerplate pipeline 'tabular_preprocessing'
generated using Kedro 1.0.0
"""

from kedro.pipeline import Pipeline, Node  # noqa
from .nodes import get_raw_data, preprocess_missing_data


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline([
        Node(
            func=get_raw_data,
            inputs={},
            outputs=["train_raw", "test_raw"],
            name="read_local_raw_data_or_download",
        ),
        Node(
            func=preprocess_missing_data,
            inputs=["train_raw", "test_raw"],
            outputs=["train_missing_values_preprocessed", "test_missing_values_preprocessed"],
            name="preprocess_missing_values",
        ),
        # Node(
        #     func=get_raw_data,
        #     inputs=["train_raw", "test_raw"],
        #     outputs=["train_missing_values_preprocessed", "test_missing_values_preprocessed"],
        #     name="encode_categorical_data",
        # ),
        # Node(
        #     func=get_raw_data,
        #     inputs=["train_raw", "test_raw"],
        #     outputs=["train_missing_values_preprocessed", "test_missing_values_preprocessed"],
        #     name="encode_categorical_data",
        # ),
    ])
