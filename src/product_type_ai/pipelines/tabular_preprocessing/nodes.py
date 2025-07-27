"""
This is a boilerplate pipeline 'tabular_preprocessing'
generated using Kedro 1.0.0
"""
import os
import pandas as pd
from minio import Minio
from pathlib import Path
from kedro.config import OmegaConfigLoader
from kedro.framework.project import settings
import logging

logger = logging.getLogger(__name__)

def _display_basic_info_about_dataset(df: pd.DataFrame, name: str = ''):
    # Display basic info about the dataset
    df_info = {
        "shape": df.shape,
        "columns": df.columns.tolist(),
        "dtypes": df.dtypes.astype(str).to_dict(),
        "missing_values": df.isnull().sum().to_dict()
    }

    logger.info(
        f"{name}: \n"
        f"{df_info}",
    )

def _get_conf_source():
    project_root = Path.cwd()
    return project_root / settings.CONF_SOURCE

def _get_raw_data_source():
    project_root = Path.cwd()
    return project_root / "data" / "01_raw"

conf = None
def _get_config():
    if conf is not None:
        return conf

    return OmegaConfigLoader(str(_get_conf_source()))

def get_raw_data() -> (pd.DataFrame, pd.DataFrame):
    conf_source = _get_conf_source()
    data_source = _get_raw_data_source()

    data_source_config = _get_config()["parameters"]["data_source"]

    train_data_name = data_source_config['train']['name']
    test_data_name = data_source_config['test']['name']

    train_data = str(data_source / train_data_name)
    test_data = str(data_source / test_data_name)

    train_data_exists = os.path.exists(train_data)
    test_data_exists = os.path.exists(test_data)

    if train_data_exists and test_data_exists:
        logger.info(f"[✓] {train_data_name} and {test_data_name} already exists locally. No download will be performed.")

        train = pd.read_parquet(train_data)
        test = pd.read_parquet(test_data)

        return train, test

    train_data_s3_path = data_source_config['train']['s3_path']
    test_data_s3_path = data_source_config['test']['s3_path']

    s3_access_key = os.environ.get("S3_ACCESS_KEY")
    s3_secret_access_key = os.environ.get("S3_SECRET_KEY")
    s3_endpoint_url = os.environ.get("S3_ENDPOINT")
    s3_region_name = os.environ.get("S3_REGION")
    s3_bucket = os.environ.get("S3_BUCKET")

    s3 = Minio(
        endpoint=s3_endpoint_url,
        access_key=s3_access_key,
        secret_key=s3_secret_access_key,
        region=s3_region_name,
    )

    if not s3.bucket_exists(s3_bucket):
        raise ConnectionError(f"Bucket {s3_bucket} does not exist")

    os.makedirs(os.path.dirname(data_source), exist_ok=True)

    train_data_s3_full_path = f"{train_data_s3_path}/{train_data_name}"
    test_data_s3_full_path = f"{test_data_s3_path}/{test_data_name}"

    if not train_data_exists:
        logger.info(f"[↓] Downloading  s3://{s3_bucket}/{train_data_s3_full_path} → {train_data}")
        s3.fget_object(s3_bucket, train_data_s3_full_path, train_data)

    if not test_data_exists:
        logger.info(f"[↓] Downloading  s3://{s3_bucket}/{test_data_s3_full_path} → {test_data}")
        s3.fget_object(s3_bucket, test_data_s3_full_path, test_data)

    train = pd.read_parquet(train_data)
    test = pd.read_parquet(test_data)

    return train, test

def preprocess_missing_data(
    train_raw: pd.DataFrame,
    test_raw: pd.DataFrame,
) -> (pd.DataFrame, pd.DataFrame):
    no_photo_replacement = _get_config()["parameters"]["missing_values_replacements"]["no_photo"]
    no_category_replacement = _get_config()["parameters"]["missing_values_replacements"]["no_category"]

    train_raw['main_photo'] = train_raw['main_photo'].fillna(no_photo_replacement)
    train_raw['category_l2'] = train_raw['category_l2'].fillna(no_category_replacement)
    train_raw['category_l4'] = train_raw['category_l4'].fillna(no_category_replacement)

    test_raw['main_photo'] = test_raw['main_photo'].fillna(no_photo_replacement)
    test_raw['category_l2'] = test_raw['category_l2'].fillna(no_category_replacement)
    test_raw['category_l4'] = test_raw['category_l4'].fillna(no_category_replacement)

    _display_basic_info_about_dataset(train_raw, 'train_raw')
    _display_basic_info_about_dataset(test_raw, 'test_raw')

    return train_raw, test_raw