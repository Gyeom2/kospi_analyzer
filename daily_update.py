import pandas as pd
import os
from datetime import datetime
# gather.py에서 공통으로 사용할 클래스와 함수를 가져옵니다.
from gather import 전종목시세, patched_wrap_get_market_sector_classifications, set_krx_auth


def run_daily_update(db_path):
    # 1. 오늘 날짜 설정 (YYYYMMDD)
    # 장 마감 후 실행한다고 가정합니다.
    today_str = datetime.now().strftime('%Y%m%d')
    today_dt = pd.to_datetime(today_str)

    print(f"[{today_str}] 데일리 업데이트를 시작합니다.")

    # 2. KRX 인증
    set_krx_auth()
    api = 전종목시세()

    # 3. 오늘치 시세 데이터 수집
    try:
        today_df = api.fetch(today_str)
        if today_df.empty:
            print(f"데이터가 없습니다. (주말/공휴일 혹은 장 미마감)")
            return

        today_df['일자'] = today_dt
    except Exception as e:
        print(f"시세 수집 중 오류 발생: {e}")
        return

    # 4. 오늘치 업종 정보 매칭
    print("업종 정보 매핑 중...")
    sector_df = patched_wrap_get_market_sector_classifications(
        today_str, "KOSPI")
    if not sector_df.empty:
        sector_info = sector_df[['업종명']].reset_index()
        today_df = pd.merge(today_df, sector_info, on='종목코드', how='left')
        today_df['업종명'] = today_df['업종명'].fillna('미분류')
    else:
        print("업종 정보를 가져오지 못해 시세만 추가합니다.")

    # 5. 파티션 컬럼(year) 생성
    # 파티셔닝 구조를 유지하기 위해 'year' 컬럼이 반드시 필요합니다.
    today_df['year'] = today_df['일자'].dt.year

    # 6. 기존 데이터 로드 및 합치기 (폴더 구조 대응)
    if os.path.exists(db_path):
        # 폴더명을 입력하면 하위 모든 파티션을 읽어옵니다.
        existing_df = pd.read_parquet(db_path)

        # 중복 방지: 오늘 날짜 데이터가 이미 있다면 삭제
        existing_df = existing_df[existing_df['일자'] != today_dt]

        # 데이터 합치기
        final_df = pd.concat([existing_df, today_df], ignore_index=True)
        print(f"기존 데이터({len(existing_df)}행)에 오늘 데이터를 추가했습니다.")
    else:
        final_df = today_df
        print("기존 DB 폴더가 없어 새로 생성합니다.")

    # 7. 최종 저장 (파티셔닝 적용)
    # partition_cols를 지정하면 year별로 폴더를 자동 생성/업데이트합니다.
    final_df.to_parquet(
        db_path,
        index=False,
        engine='pyarrow',
        partition_cols=['year']
    )
    print(f"성공적으로 '{db_path}' 폴더에 파티션 저장되었습니다.")


if __name__ == "__main__":
    # 파일명이 아닌 '폴더명'을 지정합니다.
    TARGET_DB = '/kospi_db'
    run_daily_update(TARGET_DB)
