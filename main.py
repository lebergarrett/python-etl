import pandas as pd
import requests
import datetime

# URL where the covid data is stored
nyt_data_url = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv'
# Create a 'dataframe' from csv at dst url (basically a table)
nyt_df = pd.read_csv(nyt_data_url)
# Turn the date field from a string into datetime
nyt_df['date'] = pd.to_datetime(nyt_df.date)

# URL where the John Hopkins data is stored (for recoveries)
jh_data_url = 'https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv'
# Create a dataframe from csv at dst
jh_df = pd.read_csv(jh_data_url, usecols=['Date', 'Country/Region', 'Recovered'])
# Rename Date column to date so it matches nyt df
jh_df.rename(columns={"Date": "date"}, inplace=True)
# Turn the date field from a string into datetime
jh_df['date'] = pd.to_datetime(jh_df.date)
# Filter dataframe to just US
jh_df = jh_df.loc[jh_df['Country/Region'] == 'US']

# Merge tables (dataframes) based on matching dates
merged_df = pd.merge(nyt_df, jh_df, on='date')
# Drop the Country/Region column
merged_df = merged_df.drop(columns=['Country/Region'])
# Change the Recovered column datatype to int, it starts as float for some reason
merged_df = merged_df.astype({"Recovered": int})

print(merged_df)
merged_df.to_csv(r'merged_df.csv', index = False)