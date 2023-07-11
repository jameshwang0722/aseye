import pandas as pd
import statsmodels.api as sm
import pmdarima as pm

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


def sarimax_monthly(df, facility,ind):
    sarimax= pm.auto_arima(df['co2_monthly_emission'],
                           start_p=1, start_q=1,
                           test='adf',
                           max_p=3, max_q=3, m=12,
                           start_P=0, seasonal=True,
                           d=None, D=0, trace=True,
                           error_action='ignore',
                           suppress_warnings=True,
                           stepwise=True)

    model=sm.tsa.statespace.SARIMAX(df['co2_monthly_emission'],order=sarimax.order,seasonal_order=sarimax.seasonal_order)
    results=model.fit()
    df_prediction = pd.DataFrame(results.predict(start = len(df), end = len(df)+11, dynamic= True))
    df_prediction.reset_index(inplace=True)
    df_prediction.rename(columns={'index': 'datetime', 'predicted_mean': 'predicted_CO2_emission'}, inplace=True)
    df_prediction['facility'] = facility
    parameter = {"facility": facility,'p': sarimax.order[0],'d': sarimax.order[1],'q': sarimax.order[2],'seasonal_P': sarimax.seasonal_order[0], 'seasonal_D': sarimax.seasonal_order[1], 'seasonal_Q': sarimax.seasonal_order[2], 'S': sarimax.seasonal_order[3]}
    df_parameter = pd.DataFrame(parameter, index=[ind])
    return df_prediction, df_parameter

@transformer
def transform(df, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    prediction_df = pd.DataFrame()
    prediction_parameter_df = pd.DataFrame()
    index=0

    grouped_data = df.groupby('facilityName')

    for facility, data in grouped_data:
        data_sorted = data.sort_values(['date'])
        training_data = pd.DataFrame(data_sorted[['date', 'co2_monthly_emission']])
        training_data.set_index('date', inplace=True)
        df_prediction, df_parameter = sarimax_monthly(training_data, facility, index)
        prediction_df = pd.concat([prediction_df, df_prediction])
        prediction_parameter_df = pd.concat([prediction_parameter_df, df_parameter])
        index +=1
    print(prediction_df)

    return {"predicted_CO2_emission": prediction_df.to_dict(orient="dict"), 
    "facility_prediction_parameter": prediction_parameter_df.to_dict(orient="dict")}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
