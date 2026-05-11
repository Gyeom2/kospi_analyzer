import pandas as pd

df = pd.read_parquet('kospi_db/')
print(df.head())
print(df['업종명'].unique())  # 업종이 잘 들어갔는지 확인
