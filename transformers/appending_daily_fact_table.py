import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def format_monthly_data_for_union(datetime_dim,monthly_emission_data, index_start):
    monthly_emission_data['datetime'] = pd.to_datetime(monthly_emission_data['datetime'], format='%Y-%m-%d')
    df = pd.merge(monthly_emission_data, datetime_dim, on='datetime', how = 'left')
    new_indexes = range(index_start, index_start + len(df))
    df['production_id'] = new_indexes
    df = df[['production_id','facilityId', 'datetime_id','so2Mass', 'co2Mass', 'noxMass', 'heatInput']].copy()
    return df

def filter_and_group_by_facility(df):
    filtered_raw_data = df[['stateCode','facilityName', 'facilityId', 'unitId', 'date', 'so2Mass', 
                              'co2Mass', 'noxMass', 'heatInput']].copy()
    grouped_df = filtered_raw_data.groupby(['stateCode', 'facilityName', 'facilityId', 'date' ]) \
                .agg({'so2Mass': 'sum', 'co2Mass': 'sum', 'noxMass': 'sum', 'heatInput': 'sum'})
    grouped_df = grouped_df.reset_index()
    grouped_df['datetime'] = pd.to_datetime(grouped_df['date'], format='%Y-%m-%d')
    grouped_df = grouped_df.drop(['date'], axis=1)
    return grouped_df
    
@transformer
def transform(df1,df2,df3, *args, **kwargs):
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
    filtered_daily_emission = filter_and_group_by_facility(df3)
    index_start = df2['production_id'].max()+1
    df = format_monthly_data_for_union(df1,filtered_daily_emission, index_start)
    new_fact_table = pd.concat([df2, df])
    new_fact_table = new_fact_table.reset_index(drop=True)
    return new_fact_table


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
