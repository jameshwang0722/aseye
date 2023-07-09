import pandas as pd
import statsmodels.api as sm
import pmdarima as pm

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


def sarimax_monthly(df, facility):
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
    return df_prediction

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

    grouped_data = df.groupby('facilityName')

    for facility, data in grouped_data:
        data_sorted = data.sort_values(['date'])
        training_data = pd.DataFrame(data_sorted[['date', 'co2_monthly_emission']])
        training_data.set_index('date', inplace=True)
        df_prediction = sarimax_monthly(training_data, facility)
        prediction_df = pd.concat([prediction_df, df_prediction])

    
    return prediction_df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
