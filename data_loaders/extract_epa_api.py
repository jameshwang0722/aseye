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


def API_EIA_DAILY_Extraction(beginDate, endDate):
    parameters = {
    'api_key': get_secret_value('EIA_API_KEY'),
    #'stateCode': state,
    'beginDate': beginDate,
    'endDate': endDate,
    }
    url = 'https://api.epa.gov/easey/streaming-services/emissions/apportioned/daily'
    response = requests.get(url, params=parameters)

    return pd.DataFrame(response.json())


def date_finder():
    current_date = datetime.now()

    begin_date = current_date - relativedelta(months=2)
    begin_date_str = begin_date.strftime('%Y-%m-%d')

    _, last_day = calendar.monthrange(begin_date.year, begin_date.month)
    end_date = begin_date.replace(day=last_day)
    end_date_str = end_date.strftime('%Y-%m-%d')
    return begin_date_str, end_date_str

@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """

    beginDate, endDate = date_finder()

    #states = ['AL', 'AK', 'AS', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'DC', 'FM', 
    #'FL', 'GA', 'GU', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MH', 'MD', 
    #'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 
    #'MP', 'OH', 'OK', 'OR', 'PW', 'PA', 'PR', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 
    #'VI', 'VA', 'WA', 'WV', 'WI', 'WY']




    #state_data_dict = {}
    #for state in states:
    #    state_data_dict[state] = API_EIA_DAILY_Extraction(state, beginDate, endDate)

    df = API_EIA_DAILY_Extraction(beginDate, endDate)
    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

    test_df_emission = API_EIA_DAILY_Extraction("2021-09-26", "2021-09-27")
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


    test_beginDate, test_endDate = date_finder()

    year_str, month_str, day_str = test_beginDate.split("-")
    begin_year = int(year_str)
    begin_month = int(month_str)
    begin_day = int(day_str)

    year_str, month_str, day_str = test_endDate.split("-")
    end_year = int(year_str)
    end_month = int(month_str)
    end_day = int(day_str)
    
    current_date = datetime.now()
    current_year = current_date.year
    current_month = current_date.month
    current_day = current_date.day

    assert begin_day == 1

    if current_month<=2:
        assert begin_month == current_month+10
        assert end_month == current_month+10
        assert begin_year == current_year-1
        assert end_year == current_year-1
    else:
        assert begin_month == current_month-2
        assert end_month == current_month-2
        assert begin_year == current_year
        assert end_year == current_year    
    
    if end_month == 2:
        if end_year % 4 == 0:
            assert end_day == 29
        else:
            assert end_day == 28
    elif end_month in [1,3,5,7,8,10,12]:
        assert end_day == 31
    else:
        end_day ==30


    


