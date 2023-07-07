import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, df2, *args, **kwargs):
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
    filtered_raw_data = df[['stateCode','facilityName', 'facilityId', 'unitId', 'date', 'so2Mass', 
                              'co2Mass', 'noxMass', 'heatInput']].copy()
    grouped_df = filtered_raw_data.groupby(['stateCode', 'facilityName', 'facilityId', 'date' ]) \
                .agg({'so2Mass': 'sum', 'co2Mass': 'sum', 'noxMass': 'sum', 'heatInput': 'sum'})
    grouped_df = grouped_df.reset_index()
    grouped_df['production_id'] = grouped_df.index
    grouped_df['datetime'] = pd.to_datetime(grouped_df['date'], format='%Y-%m-%d')
    grouped_df = grouped_df.drop(['date'], axis=1)

    datetime_dim = grouped_df[['datetime']].drop_duplicates().reset_index(drop=True)
    datetime_dim['day'] = datetime_dim['datetime'].dt.day
    datetime_dim['week'] = datetime_dim['datetime'].dt.isocalendar().week
    datetime_dim['month'] = datetime_dim['datetime'].dt.month
    datetime_dim['year'] = datetime_dim['datetime'].dt.year
    datetime_dim['weekday'] = datetime_dim['datetime'].dt.weekday
    datetime_dim['datetime_id'] = datetime_dim.index
    datetime_dim = datetime_dim[['datetime_id', 'datetime', 'day', 'week', 'month', 'year', 'weekday']]

    facility_dim = grouped_df[['facilityId', 'facilityName', 'stateCode']].drop_duplicates().reset_index(drop=True)
    facility_dim['facility_id'] = facility_dim.index
    facility_dim = facility_dim[['facility_id','facilityId', 'facilityName', 'stateCode']]
    facility_dim = pd.merge(facility_dim, df2[['facilityId', 'latitude', 'longitude']], how = 'left', on = 'facilityId')
    facility_dim = facility_dim.drop_duplicates().reset_index(drop=True)

    fact_table = grouped_df.merge(facility_dim, on = 'facilityId') \
             .merge(datetime_dim, on='datetime') \
             [['production_id','facility_id', 'datetime_id', 'so2Mass', 'co2Mass', 'noxMass', 'heatInput']]
    fact_table = fact_table.sort_values(by='production_id', ascending=True)
    fact_table = fact_table.reset_index()


    return {"datetime_dim":datetime_dim.to_dict(orient="dict"),
    "facility_dim":facility_dim.to_dict(orient="dict"),
    "fact_table":fact_table.to_dict(orient="dict")}
@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
