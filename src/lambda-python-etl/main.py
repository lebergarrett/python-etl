import pandas as pd
import transformation
import json
import boto3
import awswrangler as wr

def lambda_handler(event, context):
    # URL where the covid data is stored
    nyt_data_url = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv'
    nyt_df = transformation.load_df(nyt_data_url)

    # URL where the Johns Hopkins data is stored (for recoveries)
    jh_data_url = 'https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv'
    jh_df = transformation.load_df(jh_data_url)

    # transform data so it matches format of nyt data
    jh_df = transformation.keep_columns(jh_df, ['date', 'Country/Region', 'Recovered'])
    jh_df = transformation.filter_df(jh_df, 'Country/Region', 'US')

    # Merge tables (dataframes) based on matching dates
    merged_df = transformation.merge_dfs(nyt_df, jh_df, 'date')

    # Set up connection to dynamodb table
    dynamodbclient = boto3.resource('dynamodb')
    table = dynamodbclient.Table("python-etl-table")

    # Check if this is first load of table
    tabledata = json.loads(table.describe_table())
    if tabledata["Table"]["ItemCount"] == 0:
        wr.dyanmodb.put_df(merged_df, table)
    # If not first load, only missing days need updated
    else:
        # Check date in most recent row
        # Add row per missing date until today