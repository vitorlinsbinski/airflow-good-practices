import pandas as pd

def drop_null_and_remove_duplicates(df: pd.DataFrame):
    assert isinstance(df, pd.DataFrame)
    assert not df.empty, "DataFrame vazio antes da transformação"
    
    df_without_na = df.dropna()
    assert df_without_na.isna().sum().sum() == 0

    df_without_duplicates = df_without_na.drop_duplicates()
    assert not df_without_duplicates.duplicated().any()

    return df_without_duplicates

