import io
import pandas as pd
import requests

from mage_ai.data_preparation.shared.secrets import get_secret_value

from datetime import datetime

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


def API_EIA_Facility_Extraction(year):
    parameters = {
    'api_key': get_secret_value('EIA_API_KEY'),
    'year': year
    }
    url = "https://api.epa.gov/easey/streaming-services/facilities/attributes"
    response = requests.get(url, params=parameters)

    return pd.DataFrame(response.json())


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    current_date = datetime.now()
    current_year = current_date.year
    df = API_EIA_Facility_Extraction(current_year)
    return df



@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
        
        
    test_df_facilities = API_EIA_Facility_Extraction("2023")
    assert (
    lambda test_df_facilities: test_df_facilities[(
        test_df_facilities["facilityId"] == 3
        ) & (
            test_df_facilities["unitId"] == "7A"
            )
            & (
            test_df_facilities["year"] == 2023
            )]["latitude"].values[0]
            )(test_df_facilities) == 31.0069 , 'Facility API Pull failed'

