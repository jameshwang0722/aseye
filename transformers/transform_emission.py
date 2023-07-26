import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


def filter_and_group_by_facility(df):
    filtered_raw_data = df[['stateCode','facilityName', 'facilityId', 'unitId', 'date', 'so2Mass', 
                              'co2Mass', 'noxMass', 'heatInput']].copy()
    grouped_df = filtered_raw_data.groupby(['stateCode', 'facilityName', 'facilityId', 'date' ]) \
                .agg({'so2Mass': 'sum', 'co2Mass': 'sum', 'noxMass': 'sum', 'heatInput': 'sum'})
    grouped_df = grouped_df.reset_index()
    grouped_df['production_id'] = grouped_df.index
    grouped_df['datetime'] = pd.to_datetime(grouped_df['date'], format='%Y-%m-%d')
    grouped_df = grouped_df.drop(['date'], axis=1)
    return grouped_df

def create_datetime_dim(grouped_df):
    datetime_dim = grouped_df[['datetime']].drop_duplicates().reset_index(drop=True)
    datetime_dim = datetime_dim.sort_values(['datetime'])
    datetime_dim['day'] = datetime_dim['datetime'].dt.day
    datetime_dim['week'] = datetime_dim['datetime'].dt.isocalendar().week
    datetime_dim['month'] = datetime_dim['datetime'].dt.month
    datetime_dim['year'] = datetime_dim['datetime'].dt.year
    datetime_dim['weekday'] = datetime_dim['datetime'].dt.weekday
    datetime_dim['datetime_id'] = datetime_dim.index
    datetime_dim = datetime_dim[['datetime_id', 'datetime', 'day', 'week', 'month', 'year', 'weekday']]
    return datetime_dim

def create_facility_dim(grouped_df, facility_raw_data):
    facility_dim = grouped_df[['facilityId', 'facilityName', 'stateCode']].drop_duplicates().reset_index(drop=True)
    facility_dim = facility_dim[['facilityId', 'facilityName', 'stateCode']]
    facility_dim = pd.merge(facility_dim, facility_raw_data[['facilityId', 'latitude', 'longitude']], how = 'left', on = 'facilityId')
    facility_dim = facility_dim.drop_duplicates().reset_index(drop=True)
    facility_dim = facility_dim.sort_values(['facilityId'])
    return facility_dim

def create_fact_table(grouped_df, datetime_dim, facility_dim):
    fact_table = grouped_df.merge(facility_dim, on = 'facilityId') \
            .merge(datetime_dim, on='datetime') \
            [['production_id','facilityId', 'datetime_id', 'so2Mass', 'co2Mass', 'noxMass', 'heatInput']]
    fact_table = fact_table.sort_values(by='production_id', ascending=True)
    #fact_table = fact_table.reset_index()
    return fact_table


@transformer
def transform(emission_raw_data, facility_raw_data, *args, **kwargs):
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
    grouped_df = filter_and_group_by_facility(emission_raw_data)
    datetime_dim = create_datetime_dim(grouped_df)
    facility_dim = create_facility_dim(grouped_df, facility_raw_data)
    fact_table = create_fact_table(grouped_df, datetime_dim, facility_dim)


    return {"datetime_dim":datetime_dim.to_dict(orient="dict"),
    "facility_dim":facility_dim.to_dict(orient="dict"),
    "fact_table":fact_table.to_dict(orient="dict")}
    
@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    test_emission_sample = pd.read_csv('Aseye/test_sample/daily-emissions-facility-sample.csv')
    test_facility_sample = pd.read_csv('Aseye/test_sample/facility_sample.csv')

    test_filter_and_group_by_facility = filter_and_group_by_facility(test_emission_sample)
    assert (
        lambda test_filter_and_group_by_facility: test_filter_and_group_by_facility[(
            test_filter_and_group_by_facility["facilityId"] == 3
            ) & (
                test_filter_and_group_by_facility["datetime"] == "2021-01-01"
                )]["co2Mass"].values[0]
                )(test_filter_and_group_by_facility) == 10290.9 , 'filter and group by facility function failed'
    
    test_create_datetime_dim = create_datetime_dim(test_filter_and_group_by_facility) 
    assert (
        lambda test_create_datetime_dim: test_create_datetime_dim[
                test_create_datetime_dim["datetime_id"] == 0
                ]["year"].values[0]
                )(test_create_datetime_dim) == 2012, 'create datetime dim function failed'


    test_create_facility_dim = create_facility_dim(test_filter_and_group_by_facility, test_facility_sample)
    assert (
        lambda test_create_facility_dim: test_create_facility_dim[
                test_create_facility_dim["facilityId"] == 7
                ]["facilityName"].values[0]
                )(test_create_facility_dim) == 'Gadsden', 'create facility dim function failed'
    
    
    test_create_fact_table = create_fact_table(test_filter_and_group_by_facility, test_create_datetime_dim, test_create_facility_dim)
    assert (
        lambda test_create_fact_table: test_create_fact_table[(
            test_create_fact_table["facilityId"] == 3
            ) & (
                test_create_fact_table["datetime_id"] == 1
                )]["co2Mass"].values[0]
                )(test_create_fact_table) == 16757.2 , 'create fact table function failed'

