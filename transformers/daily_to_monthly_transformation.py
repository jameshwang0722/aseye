import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
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
    datetime_dim = pd.DataFrame(data["datetime_dim"])
    facility_dim = pd.DataFrame(data["facility_dim"])
    fact_table = pd.DataFrame(data["fact_table"])


    df = pd.merge(fact_table, datetime_dim[['datetime_id', 'datetime']], how = 'left', on = 'datetime_id')



    columns_to_drop = ['index', 'production_id','datetime_id']
    df = df.drop(columns_to_drop, axis=1)

    # Set the day component of the date to the first day of the month
    df['datetime'] = pd.to_datetime(df['datetime'])
    datetime_dim['datetime'] = pd.to_datetime(datetime_dim['datetime'])
    df['Date'] = df['datetime'].dt.to_period('M').dt.to_timestamp()

    # Group the data by month and facility, and sum the daily production
    df_monthly = df.groupby([pd.Grouper(key='Date'), 'facilityId']).sum().reset_index()
    df_monthly['datetime'] = df_monthly['Date']


    #convert table to include datetime_id instead of datetime (remove if you want to keep real date instead of date_id)
    df_monthly = pd.merge(df_monthly, datetime_dim[['datetime_id', 'datetime']], how = 'left', on = 'datetime')
    columns_to_drop = ['Date', 'datetime']
    df_monthly = df_monthly.drop(columns_to_drop, axis=1)




    return df_monthly.to_dict(orient="dict")


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
