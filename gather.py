import pandas as pd
import numpy as np
import os
import re
from datetime import datetime, timedelta
from pandas import DataFrame

from pykrx import stock
from pykrx.website.krx.market.core import 업종분류현황
from pykrx.website.krx.krxio import KrxWebIo
from workalendar.asia import SouthKorea


class 전종목시세(KrxWebIo):
    @property
    def bld(self):
        # [MDCSTAT01501] 주식 - 종목시세 - 전종목 시세
        return "dbms/MDC/STAT/standard/MDCSTAT01501"

    def fetch(self, target_date: str) -> DataFrame:
        """
        특정 일자의 전 종목 시세 데이터를 수집합니다.
        :param target_date: 조회 일자 (YYYYMMDD 형식)
        """
        # payload.txt 내용을 바탕으로 구성
        result = self.read(
            bld=self.bld,
            locale="ko_KR",
            mktId="STK",       # KOSPI 기준 (KOSDAQ은 KSQ)
            trdDd=target_date,  # 조회 일자
            share="1",
            money="1",
            csvxls_isNo="false"
        )

        # 응답 데이터 확인 (response.txt 구조 반영)
        if "OutBlock_1" not in result:
            print(f"{target_date} 데이터가 없습니다.")
            return DataFrame()

        # 리스트 데이터를 DataFrame으로 변환
        df = DataFrame(result['OutBlock_1'])

        # 분석에 필요한 주요 컬럼 매핑 (response.txt 참고)
        # ISU_SRT_CD: 종목코드, ISU_ABBRV: 종목명, TDD_CLSPRC: 종가,
        # TDD_HGPRC: 고가, TDD_LWPRC: 저가, MKTCAP: 시가총액
        target_cols = {
            'ISU_SRT_CD': '종목코드',
            'ISU_ABBRV': '종목명',
            'TDD_CLSPRC': '종가',
            'TDD_HGPRC': '고가',
            'TDD_LWPRC': '저가',
            'MKTCAP': '시가총액'
        }

        # 컬럼 필터링 및 이름 변경
        df = df[list(target_cols.keys())].rename(columns=target_cols)

        # 수치 데이터 형식 변환 (문자열 -> 숫자)
        numeric_columns = ['종가', '고가', '저가', '시가총액']
        for col in numeric_columns:
            if col in df.columns:
                # 콤마 제거 및 하이픈을 0으로 대체
                df[col] = df[col].astype(str).str.replace(
                    ',', '').replace('-', '0')
                # 안전하게 숫자로 변환 (변환 불가 시 NaN -> 0 처리)
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        # 날짜 정보 추가
        df['일자'] = pd.to_datetime(target_date)

        return df


def patched_wrap_get_market_sector_classifications(date: str, market: str) -> pd.DataFrame:
    """보완된 업종별 분류 현황 함수"""
    market2mktid = {
        "KOSPI": "STK",
        "KOSDAQ": "KSQ",
    }

    # 1. 원본 데이터 fetch
    try:
        df = 업종분류현황().fetch(date, market2mktid[market])

    except Exception as e:
        print(f"데이터 fetch 중 오류 발생: {e}")
        return pd.DataFrame()

    if df.empty:
        return pd.DataFrame()

    # 2. KRX 응답 키 변경 대응 (유연한 컬럼 선택)
    # 2026년 기준 예상되는 컬럼 리스트와 실제 응답을 비교합니다.
    target_columns = {
        "ISU_SRT_CD": "종목코드",
        "ISU_ABBRV": "종목명",
        "IDX_IND_NM": "업종명",
        "TDD_CLSPRC": "종가",
        "CMPPREVDD_PRC": "대비",
        "FLUC_RT": "등락률",
        "MKTCAP": "시가총액"
    }

    # 실제 존재하는 컬럼만 필터링하여 매핑
    available_cols = [col for col in target_columns.keys()
                      if col in df.columns]

    if not available_cols:
        # 만약 영문 대문자 키가 없다면 소문자나 다른 형식을 체크하도록 로직 확장 가능
        return pd.DataFrame()

    df = df[available_cols].copy()
    df.columns = [target_columns[col] for col in available_cols]

    # 3. 데이터 정제 (NaN 및 문자열 처리)
    df = df.replace(r"\-$", "0", regex=True)
    df = df.replace("", "0", regex=True)
    df = df.replace(",", "", regex=True)

    # 4. 안전한 데이터 타입 변환
    dtype_map = {
        "종가": np.int32,
        "대비": np.float64,
        "등락률": np.float64,
        "시가총액": np.int64,
    }

    for col, dtype in dtype_map.items():
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col], errors='coerce').fillna(0).astype(dtype)

    return df.set_index("종목코드") if "종목코드" in df.columns else df


def set_krx_auth():
    config_path = "config.txt"
    if os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as f:
            for line in f:
                if "=" in line:
                    key, value = line.strip().split('=', 1)
                    # pykrx가 내부적으로 찾는 환경 변수명으로 설정합니다.
                    if key == "ID":
                        os.environ["KRX_ID"] = value
                    elif key == "PW":
                        os.environ["KRX_PW"] = value
        print("KRX 로그인 정보가 설정되었습니다.")
    else:
        print("config.txt 파일을 찾을 수 없습니다.")


# 인증 정보 적용
set_krx_auth()


# 1. 수집할 날짜 리스트 생성 (영업일 기준 예시)
date_list = pd.date_range(start='2025-05-08', end='2026-05-08', freq='B')
cal = SouthKorea()
api = 전종목시세()
all_data = []

# 2. 루프를 돌며 수집
print("데이터 수집 시작...")
for dt in date_list:
    if not cal.is_working_day(dt):
        continue

    target_str = dt.strftime('%Y%m%d')
    try:
        daily_df = api.fetch(target_str)
        if not daily_df.empty:
            daily_df['일자'] = dt
            all_data.append(daily_df)
            print(f"{target_str} 수집 완료")
    except Exception as e:
        print(f"{target_str} 에러 발생: {e}")

# 3. 데이터 통합 및 업종명 매칭
if all_data:
    combined_df = pd.concat(all_data, ignore_index=True)

    # [추가] 최신 날짜 기준으로 업종 정보 수집
    last_date = date_list[-1].strftime('%Y%m%d')
    print(f"업종 정보 매핑 중... (기준일: {last_date})")

    sector_df = patched_wrap_get_market_sector_classifications(
        last_date, "KOSPI")

    if not sector_df.empty:
        # 종목코드와 업종명만 추출 (sector_df는 종목코드가 인덱스인 상태)
        sector_info = sector_df[['업종명']].reset_index()

        # 시세 데이터와 업종 정보를 '종목코드' 기준으로 합침 (Left Join)
        # 1년치 모든 날짜의 종목들에 동일한 업종명이 붙게 됩니다.
        final_df = pd.merge(combined_df, sector_info, on='종목코드', how='left')

        # 업종명이 없는 경우(상장폐지 등) 처리
        final_df['업종명'] = final_df['업종명'].fillna('미분류')
    else:
        final_df = combined_df
        print("주의: 업종 정보를 불러오지 못했습니다.")

    # 4. Parquet 저장
    final_df.to_parquet('kospi_1year_with_sectors.parquet', index=False)
    print("최종 파일 저장 완료: kospi_1year_with_sectors.parquet")

    # 결과 확인
    print(final_df.head())
