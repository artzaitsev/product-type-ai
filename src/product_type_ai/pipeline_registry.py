"""Project pipelines."""
from __future__ import annotations

from kedro.framework.project import find_pipelines
from kedro.pipeline import Pipeline

from .pipelines import (
    tabular_preprocessing,
    tabular_model,
    image_model,
    fusion_model,
)
from kedro.pipeline import pipeline

PIPELINE_REGISTRY = {
    "tabular_preprocessing": tabular_preprocessing.create_pipeline(),
    "tabular_model": tabular_model.create_pipeline(),
    "image_model": image_model.create_pipeline(),
    "fusion_model": fusion_model.create_pipeline(),
}

def register_pipelines() -> dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from pipeline names to ``Pipeline`` objects.
    """
    return {
        "__default__": pipeline(
            [
                tabular_preprocessing.create_pipeline(),
                tabular_model.create_pipeline(),
                image_model.create_pipeline(),
                fusion_model.create_pipeline(),
            ]
        ),
        "tabular_preprocessing": tabular_preprocessing.create_pipeline(),
        "tabular_model": tabular_model.create_pipeline(),
        "image_model": image_model.create_pipeline(),
        "fusion_model": fusion_model.create_pipeline(),
    }
