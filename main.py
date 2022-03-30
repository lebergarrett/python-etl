import pandas as pd
import transformation

# URL where the covid data is stored
nyt_data_url='https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv'
nyt_df=transformation.load_df(nyt_data_url)

# URL where the Johns Hopkins data is stored (for recoveries)
jh_data_url='https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv'
jh_df=transformation.load_df(jh_data_url)
jh_df=transformation.keep_columns(jh_df, ['date', 'Country/Region', 'Recovered'])
jh_df=transformation.filter_df(jh_df, 'Country/Region', 'US')

# Merge tables (dataframes) based on matching dates
merged_df=transformation.merge_dfs(nyt_df, jh_df, 'date')

print(merged_df)
merged_df.to_csv(r'merged_df.csv', index = False)