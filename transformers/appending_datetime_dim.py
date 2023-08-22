import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


def create_datetime_dim(grouped_df, index_start):
    datetime_dim = grouped_df[['datetime']].drop_duplicates().reset_index(drop=True)
    datetime_dim = datetime_dim.sort_values(['datetime'])
    datetime_dim['day'] = datetime_dim['datetime'].dt.day
    datetime_dim['week'] = datetime_dim['datetime'].dt.isocalendar().week
    datetime_dim['month'] = datetime_dim['datetime'].dt.month
    datetime_dim['year'] = datetime_dim['datetime'].dt.year
    datetime_dim['weekday'] = datetime_dim['datetime'].dt.weekday
    datetime_dim['datetime_id'] = datetime_dim.index
    datetime_dim = datetime_dim[['datetime_id', 'datetime', 'day', 'week', 'month', 'year', 'weekday']]
    new_index = range(index_start, index_start + len(datetime_dim))
    datetime_dim['datetime_id'] = new_index
    return datetime_dim

@transformer
def transform(df,df2, *args, **kwargs):
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
    dates = pd.DataFrame()
    dates['datetime'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
    index_start = df2['datetime_id'].max()+1
    datetime_dim_month = create_datetime_dim(dates, index_start)
    new_index = range(index_start, index_start + len(datetime_dim_month))
    datetime_dim_month['datetime_id'] = new_index
    new_datetime_dim = pd.concat([df2, datetime_dim_month])
    new_datetime_dim = new_datetime_dim.reset_index(drop=True)
    new_datetime_dim['datetime'] =pd.to_datetime(new_datetime_dim['datetime'], format='%Y-%m-%d')

    return new_datetime_dim


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
