import io
import pandas as pd
import requests

from mage_ai.data_preparation.shared.secrets import get_secret_value

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import calendar

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


def API_EIA_DAILY_Extraction(state):
    parameters = {
    'api_key': get_secret_value('EIA_API_KEY'),
    'stateCode': state,
    'beginDate': '2014-01-01',
    'endDate': '2023-05-31',
    }
    url = 'https://api.epa.gov/easey/streaming-services/emissions/apportioned/daily'
    response = requests.get(url, params=parameters)

    return pd.DataFrame(response.json())

@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """


    #states = ['AL', 'AK', 'AS', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'DC', 'FM', 
    #'FL', 'GA', 'GU', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MH', 'MD', 
    #'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 
    #'MP', 'OH', 'OK', 'OR', 'PW', 'PA', 'PR', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 
    #'VI', 'VA', 'WA', 'WV', 'WI', 'WY']

    states = ['GU', 'HI', 'ID', 'IL', 'IN', 'IA']
    df = pd.DataFrame()

    for state in states:
        state_df = API_EIA_DAILY_Extraction(state)
        df = pd.concat([df, state_df])

    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

    test_df_emission = API_EIA_DAILY_Extraction("AL")
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
