import pandas as pd


def load_df(url):
    # loads csv into df, but also cleans 'date' column name
    df=pd.read_csv(url)
    if 'Date' in df.columns:
        df.rename(columns={"Date": "date"}, inplace=True)
    df['date']=pd.to_datetime(df.date)

    return df


def keep_columns(df, cols):
    # cols should be a list of columns to keep
    df=df[cols]

    return df


def filter_df(df, col, value):
    # value should be something in the column
    df=df.loc[df[col]==value]
    return df


def merge_dfs(df1, df2, key):
    df=pd.merge(df1, df2, on=key)
    if 'Country/Region' in df.columns:
        df=df.drop(columns=['Country/Region'])
    if 'Recovered' in df.columns:
        df.rename(columns={"Recovered": "recovered"}, inplace=True)
        df=df.astype({"recovered": int})

    return df