import pandas as pd

# 1. 기존 단일 파일 읽기
df = pd.read_parquet('kospi_1year_with_sectors.parquet')

# 2. 파티션 기준이 될 'year' 컬럼 생성 (일자 컬럼이 datetime 형식이어야 함)
df['year'] = pd.to_datetime(df['일자']).dt.year

# 3. 파티셔닝 구조로 저장 (파일명이 아니라 '폴더명'을 지정합니다)
# 기존 파일과 헷갈리지 않게 폴더명을 'kospi_db' 등으로 지정하는 것이 좋습니다.
df.to_parquet('kospi_db', partition_cols=[
              'year'], engine='pyarrow', index=False)

print("파티셔닝 전환 완료! 'kospi_db' 폴더가 생성되었습니다.")
