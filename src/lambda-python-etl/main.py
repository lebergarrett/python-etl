import pandas as pd
import transformation
import boto3

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

    # Set up loading of dataframe into dynamodb table
    dynamodbclient = boto3.resource('dynamodb')
    table = dynamodbclient.Table("python-etl-table")
    #initial load will use boto3.batch_write_item
    #updates will add that day, unless any previous days have failed, then it will retry them