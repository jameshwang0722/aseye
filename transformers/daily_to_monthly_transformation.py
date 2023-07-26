import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


def monthly_fact_table_creation(datetime_dim, fact_table):
    #combine dataframe and drop PK
    df = pd.merge(fact_table, datetime_dim[['datetime_id', 'datetime']], how = 'left', on = 'datetime_id')
    columns_to_drop = ['production_id','datetime_id']
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
    
    #sorting df then giving index
    df_monthly = df_monthly.sort_values(by=['facilityId', 'datetime_id'])
    df_monthly.reset_index(inplace=True)
    df_monthly.rename(columns={'index': 'monthly_production_id'}, inplace=True)

    return df_monthly


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
    fact_table = pd.DataFrame(data["fact_table"])

    df_monthly = monthly_fact_table_creation(datetime_dim, fact_table)





    return df_monthly.to_dict(orient="dict")


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

    test_fact_table = pd.read_csv('Aseye/test_sample/fact_table_sample.csv')
    test_datetime_dim = pd.read_csv('Aseye/test_sample/datetime_dim_sample.csv')


    test_df_monthly = monthly_fact_table_creation(test_datetime_dim, test_fact_table)
    assert (
    lambda test_df_monthly: test_df_monthly[(
        test_df_monthly["facilityId"] == 3
        ) & (
            test_df_monthly["datetime_id"] == 0
            )]["co2Mass"].values[0]
            )(test_df_monthly) == 549002.05 , 'monthly fact table function failed'

