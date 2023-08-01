from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

def date_finder():
    current_date = datetime.now()

    begin_date = current_date - relativedelta(months=2)
    begin_date_str = begin_date.strftime('%Y-%m-%d')
    return begin_date_str

@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a Google Cloud Storage bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#googlecloudstorage
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    date = date_finder()
    bucket_name = 'aseye-raw-data'
    object_key = 'Aseye/Raw_Data_Emissions/{}.parquet'.format(date)
    formatt = 'Parquet'

    GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile)).export(
        df,
        bucket_name,
        object_key,
        formatt,
    )
