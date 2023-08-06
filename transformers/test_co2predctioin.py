import pandas as pd
import statsmodels.api as sm
from statsmodels.tsa.stattools import adfuller
import pmdarima as pm
from pandas.api.types import is_datetime64_any_dtype

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


def adfuller_test(CO2_Emission):
    result=adfuller(CO2_Emission)
    labels = ['ADF Test Statistic','p-value','#Lags Used','Number of Observations Used']
    for value,label in zip(result,labels):
        print(label+' : '+str(value) )
    if result[1] <= 0.05: #null hypothesis is wrong and is stationary
        print("strong evidence against the null hypothesis(Ho), reject the null hypothesis. Data has no unit root and is stationary")
        return None
    else:
        print("weak evidence against null hypothesis, time series has a unit root, indicating it is non-stationary ")
        return 1

def sarimax_monthly(df, facility,ind):
    d = adfuller_test(df['co2Mass'])
    sarimax= pm.auto_arima(df['co2Mass'],
                           start_p=1, start_q=1,
                           test='adf',
                           max_p=3, max_q=3, m=12,
                           start_P=0, seasonal=True,
                           d=d, D=1, trace=True,
                           error_action='ignore',
                           suppress_warnings=True,
                           stepwise=True)

    model=sm.tsa.statespace.SARIMAX(df['co2Mass'],order=sarimax.order,seasonal_order=sarimax.seasonal_order)
    results=model.fit()
    df_prediction = pd.DataFrame(results.predict(start = len(df), end = len(df)+11, dynamic= True))
    df_prediction.rename(columns={'index': 'datetime', 'predicted_mean': 'predicted_CO2_emission'}, inplace=True)
    df_prediction['facilityId'] = facility
    parameter = {"facilityId": facility,'p': sarimax.order[0],'d': sarimax.order[1],'q': sarimax.order[2],'seasonal_P': sarimax.seasonal_order[0], 
    'seasonal_D': sarimax.seasonal_order[1], 'seasonal_Q': sarimax.seasonal_order[2], 'S': sarimax.seasonal_order[3]}
    df_parameter = pd.DataFrame(parameter, index=[ind])
    return df_prediction, df_parameter

@transformer
def transform(df, *args, **kwargs):

    prediction_df = pd.DataFrame()
    facility_predicted_log_df = pd.DataFrame()
    prediction_parameter_df = pd.DataFrame()
    index=0

    grouped_data = df.groupby('facilityId')
    grouped_data = [group.reset_index(drop=True) for _, group in grouped_data]
    for data in grouped_data:
        facility = data['facilityId'][0]
        print("facility:", facility)
        if len(data)>80 and int(data['date'][len(data)-1][:4]) == 2021:
            data_sorted = data.sort_values(['date'])
            training_data = pd.DataFrame(data_sorted[['date', 'co2Mass']])
            training_data.set_index('date', inplace=True)
            df_prediction, df_parameter = sarimax_monthly(training_data, facility, index)
            if is_datetime64_any_dtype(df_prediction.index) ==True:
                prediction_df = pd.concat([prediction_df, df_prediction])
                prediction_parameter_df = pd.concat([prediction_parameter_df, df_parameter])
                index +=1
            log = {
                'facilityId': [facility],
                'Predicted?': ['Yes']
            }
        else:
            facility_predicted_log_df = pd.concat([facility_predicted_log_df, data])
            log = {
                'facilityId': [facility],
                'Predicted?': ['No']
            }
        log_df = pd.DataFrame(log)
        facility_predicted_log_df = pd.concat([facility_predicted_log_df, log_df])
    prediction_df.reset_index(inplace=True)

    return {"predicted_CO2_emission": prediction_df.to_dict(orient="dict"),
    "facility_prediction_checker": facility_predicted_log_df.to_dict(orient="dict"), 
    "facility_prediction_parameter": prediction_parameter_df.to_dict(orient="dict")}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
