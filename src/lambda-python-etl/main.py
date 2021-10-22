import awswrangler as wr
import boto3
import datetime
import json
import pandas as pd
import transformation

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
        date = datetime.date.today()
        # Set to last item in dataframe
        current_row = -1

        while True:
            # First iteration should always have to add the item
            if current_row == -1:
                table.put_item(
                    # Hopefully this is converting to JSON as intended??
                    Item = merged_df.iloc[current_row].to_json(orient="records")
                )
            else:
                # This won't run until at least the second iteration
                # Query the db for day before, if not found, put item
                # if found, break
                try:
                    response = table.query(
                        KeyConditionExpression=Key('Date').eq(date)
                    )
                except dynamodbclient.meta.client.exceptions.ResourceNotFoundException
                    print("Info not found for previous date. Adding now")
                    table.put_item(
                        Item = merged_df.iloc[current_row].to_json(orient="records")
                    )
                if response:
                    break
                # Maybe add another break condition

            date = datetime.timedelta(days=1)
            current_row -= 1