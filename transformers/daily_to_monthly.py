import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


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

    # Drop specific columns
    columns_to_drop = ['index', 'production_id','datetime_id', 'datetime_id_1', 'day', 'week','weekday', 'month', 'year' ]
    df = df.drop(columns_to_drop, axis=1)

    # Set the day component of the date to the first day of the month
    df['datetime'] = pd.to_datetime(df['datetime'])
    df['Date'] = df['datetime'].dt.to_period('M').dt.to_timestamp()

    # Group the data by month and facility, and sum the daily production
    df_monthly = df.groupby([pd.Grouper(key='Date'), 'facilityId']).sum().reset_index()




    return df_monthly


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
