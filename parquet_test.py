import pandas as pd
import os

# 데이터 경로 설정
db_path = r'G:\내 드라이브\StockData\kospi_db'

# 1. 전체 데이터 로드 (Partition 구조 자동 인식)
df = pd.read_parquet(db_path)

# 2. 최신 날짜 확인
latest_date = df['일자'].max()
print(f"최신 데이터 날짜: {latest_date}")

# 3. 오늘 수집된 데이터 개수 확인
today_count = len(df[df['일자'] == latest_date])
print(f"[{latest_date.strftime('%Y-%m-%d')}] 수집된 종목 수: {today_count}개")

# 4. 데이터 샘플 출력
print(df[df['일자'] == latest_date].head())
