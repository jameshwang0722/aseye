import io
import pandas as pd
import requests
from mage_ai.data_preparation.shared.secrets import get_secret_value


if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


def API_EIA_DAILY_Extraction(state, beginDate, endDate):
    parameters = {
    'api_key': get_secret_value('EIA_API_KEY'),
    'stateCode': state,
    'beginDate': beginDate,
    'endDate': endDate,
    }
    url = 'https://api.epa.gov/easey/streaming-services/emissions/apportioned/daily'
    response = requests.get(url, params=parameters)

    return pd.DataFrame(response.json())

@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    state = "AL"
    beginDate = '2021-01-01'
    endDate = '2021-12-31'
    df_emission = API_EIA_DAILY_Extraction(state, beginDate, endDate)

    return df_emission


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

    test_df_emission = API_EIA_DAILY_Extraction("AL", "2021-09-26", "2021-09-27")
    assert (
    lambda test_df_emission: test_df_emission[(
        test_df_emission["facilityId"] == 880101
        ) & (
            test_df_emission["unitId"] == "PB3"
            )
            & (
            test_df_emission["date"] == "2021-09-26"
            )]["heatInput"].values[0]
            )(test_df_emission) == 5670.0 , 'Emission API Pull failed'



    


