import pandas as pd
import numpy as np
import os
import re
from datetime import datetime, timedelta
from pandas import DataFrame

from pykrx import stock
from pykrx.website.krx.krxio import KrxWebIo
from workalendar.asia import SouthKorea


class 채권지표수익률(KrxWebIo):
    @property
    def bld(self) -> str:
        # [MDCSTAT11501] 채권 - 원화채권 - 수익률 - 지표수익률추이
        return "dbms/MDC/STAT/standard/MDCSTAT11501"

    def fetch(self, start_date: str, end_date: str) -> DataFrame:
        """
        지정한 기간 동안의 채권 지표수익률 데이터를 수집합니다.
        :param start_date: 조회 시작 일자 (YYYYMMDD 형식) e.g., '20220103'
        :param end_date: 조회 종료 일자 (YYYYMMDD 형식) e.g., '20220228'
        """
        # payload.txt 및 request_header.txt 스펙 반영
        result = self.read(
            bld=self.bld,               # MDCSTAT11501
            locale="ko_KR",             #
            strtDd=start_date,          # 조회 시작일
            endDd=end_date,             # 조회 종료일
            csvxls_isNo="false"         #
        )

        # response.txt 응답 데이터 구조 검증 및 추출
        # 주가 시세의 'OutBlock_1' 대신 채권 추이는 'output' 키를 사용합니다.
        if "output" not in result or not result['output']:
            print(f"{start_date} ~ {end_date} 기간의 채권 데이터가 없습니다.")
            return DataFrame()

        # 리스트 데이터를 DataFrame으로 변환
        df = DataFrame(result['output'])

        # response.txt를 바탕으로 한 주요 채권 지표 컬럼 매핑
        # PRC_YD1~7은 각 만기별 채권수익률(%), CMP_PRC1~7은 전일대비 대비폭입니다.
        target_cols = {
            'TRD_DD': '일자',              #
            'PRC_YD1': '국고채_3년',        #
            'PRC_YD2': '국고채_5년',        #
            'PRC_YD3': '국고채_10년',       #
            'PRC_YD4': '국고채_20년',       #
            'PRC_YD5': '국고채_30년',       #
            'PRC_YD6': '국고채_50년',       #
            'PRC_YD7': '국고채_1년'         #
        }

        # 정의된 컬럼만 필터링 및 이름 변경
        available_cols = [col for col in target_cols.keys()
                          if col in df.columns]
        df = df[available_cols].rename(columns=target_cols)

        # '일자' 컬럼 포맷 통일 (2022/02/28 -> 2022-02-28)
        if '일자' in df.columns:
            df['일자'] = pd.to_datetime(df['일자']).dt.strftime('%Y-%m-%d')

        # 수치 데이터 형식 변환 (문자열 -> 숫자)
        # 채권 수익률 컬럼들만 선택하여 정제 프로세스 진행
        numeric_columns = [val for key,
                           val in target_cols.items() if key != 'TRD_DD']

        for col in numeric_columns:
            if col in df.columns:
                # 하이픈(-) 및 공백 제거 후 수치형 변환
                df[col] = df[col].astype(str).str.replace(',', '').str.strip()
                df[col] = df[col].replace('-', '0')

                # 안전하게 결측치를 0 또는 NaN 처리
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

        # 일자 기준 오름차순 정렬 (최신일이 아래로 가게 정렬)
        df = df.sort_values('일자').reset_index(drop=True)

        return df


def get_bond_indicator_yields(start_date: str, end_date: str) -> pd.DataFrame:
    """
    KrxWebIo 기반의 채권지표수익률 클래스를 활용하여 지정된 기간의 채권 수익률을 가져옵니다.
    """
    # 1. 원본 데이터 fetch
    try:
        # 이미 클래스 내부에서 한글 컬럼 매핑이 완료된 DataFrame이 반환됨
        df = 채권지표수익률().fetch(start_date, end_date)
    except Exception as e:
        print(f"채권 데이터 fetch 중 오류 발생: {e}")
        return pd.DataFrame()

    if df.empty:
        return pd.DataFrame()

    # 2. 데이터 정제 (특수문자, 하이픈, 빈 문자열 제거)
    # 이미 한글 컬럼이므로 복사본을 만들어 바로 정제 진행
    df = df.copy()
    df = df.replace(r"\-$", "0", regex=True)
    df = df.replace("", "0", regex=True)
    df = df.replace(",", "", regex=True)

    # 3. 안전한 데이터 타입 변환 (소수점 채권 수익률 특성 반영 float64)
    dtype_map = {
        "국고채_1년": np.float64,
        "국고채_3년": np.float64,
        "국고채_5년": np.float64,
        "국고채_10년": np.float64,
        "국고채_20년": np.float64,
        "국고채_30년": np.float64,
        "국고채_50년": np.float64,
    }

    for col, dtype in dtype_map.items():
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col], errors='coerce').fillna(0.0).astype(dtype)

    # 4. 날짜 데이터 포맷팅 및 정렬 후 인덱스 설정
    if "일자" in df.columns:
        df["일자"] = pd.to_datetime(df["일자"]).dt.strftime('%Y-%m-%d')
        df = df.sort_values("일자")
        return df.set_index("일자")

    return df


def set_krx_auth():
    # 1. 먼저 이미 환경변수(GitHub Secrets 등)가 설정되어 있는지 확인
    if os.environ.get("KRX_ID") and os.environ.get("KRX_PW"):
        print("시스템 환경 변수에서 KRX 로그인 정보를 확인했습니다.")
        return  # 이미 있다면 파일 읽을 필요 없이 종료

    # 2. 환경변수가 없다면 config.txt 파일 찾기 (로컬 환경용)
    config_path = "config.txt"
    if os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as f:
            for line in f:
                if "=" in line:
                    key, value = line.strip().split('=', 1)
                    if key == "ID":
                        os.environ["KRX_ID"] = value
                    elif key == "PW":
                        os.environ["KRX_PW"] = value
        print("config.txt 파일로부터 KRX 로그인 정보를 설정했습니다.")
    else:
        # 파일도 없고 환경변수도 없는 진짜 에러 상황
        print("경고: KRX 로그인 정보를 찾을 수 없습니다. (config.txt 없음 & 환경변수 미설정)")


# 인증 정보 적용
set_krx_auth()

# result = get_bond_indicator_yields("20220103", "20221229")
# print(f"result: {result}")


def get_first_and_last_working_day(year: int, cal: SouthKorea, absolute_end_date: str = None):
    """
    지정된 연도의 첫 번째 영업일과 마지막 영업일을 찾아 YYYYMMDD 문자열로 반환합니다.
    만약 absolute_end_date가 주어지고 그 날짜가 해당 연도 내에 있으면, 종료일 계산 시 상한선으로 둡니다.
    """
    # 1. 첫 영업일 찾기 (1월 1일부터 전진하며 탐색)
    current_date = datetime(year, 1, 1)
    while not cal.is_working_day(current_date):
        current_date += timedelta(days=1)
    first_working_day = current_date.strftime("%Y%m%d")

    # 2. 마지막 영업일 찾기 (12월 31일부터 후진하며 탐색)
    if absolute_end_date and datetime.strptime(absolute_end_date, "%Y-%m-%d").year == year:
        # 2026년처럼 수집 종료일 상한선이 지정된 경우
        end_limit_date = datetime.strptime(absolute_end_date, "%Y-%m-%d")
        current_date = end_limit_date
    else:
        # 일반적인 연도 마감
        current_date = datetime(year, 12, 31)

    while not cal.is_working_day(current_date):
        current_date -= timedelta(days=1)
    last_working_day = current_date.strftime("%Y%m%d")

    return first_working_day, last_working_day


def gather_and_save_bond_yields():
    """
    영업일 에러 방지 로직이 추가된 채권 지표수익률 수집 파이프라인 함수입니다.
    2022년부터 2026년 5월 15일까지의 데이터를 BondsData/year=YYYY/ 구조로 파티셔닝 저장합니다.
    """
    BASE_DIR = "BondsData"
    cal = SouthKorea()

    start_year = 2022
    absolute_end = "2026-05-15"
    current_year = datetime.strptime(absolute_end, "%Y-%m-%d").year

    print(f"📊 채권 지표수익률 수집 및 파티셔닝 시작 ({start_year}년 ~ {absolute_end})")

    for year in range(start_year, current_year + 1):
        try:
            # 💡 workalendar를 통해 에러를 유발하지 않는 정확한 연도별 시작/종료 영업일 산출
            year_start, year_end = get_first_and_last_working_day(
                year, cal, absolute_end)

            # 수집 대상 기간 상한선 검증 방어 코드
            if int(year_start) > int(absolute_end.replace("-", "")):
                print(f"⏩ {year}년 시작일이 최종 수집 제한일({absolute_end})을 초과하여 스킵합니다.")
                continue

            print(f"\n⚡ [{year}년] 영업일 기준 안전 조회 요청: {year_start} ~ {year_end}")

            # API 호출 ('일자'가 인덱스인 DataFrame 반환)
            df_yearly = get_bond_indicator_yields(year_start, year_end)

            if df_yearly.empty:
                print(f"❌ {year}년 수집된 채권 데이터가 없습니다. 건너뜁니다.")
                continue

            # '일자' 인덱스를 일반 컬럼으로 리셋
            df_yearly = df_yearly.reset_index()

            # 안전장치: 반환 데이터 중에서 한 번 더 한국 영업일 기준 필터링 수행
            df_yearly['datetime_temp'] = pd.to_datetime(df_yearly['일자'])
            is_working_day_mask = df_yearly['datetime_temp'].apply(
                lambda x: cal.is_working_day(x))
            df_filtered = df_yearly[is_working_day_mask].drop(
                columns=['datetime_temp']).copy()

            if df_filtered.empty:
                print(f"⚠️ {year}년에 해당하는 유효 영업일 데이터가 존재하지 않습니다.")
                continue

            # Pandas Native 파티셔닝을 위한 연도 파티션 키 할당
            df_filtered['year'] = year

            # PyArrow 엔진 기반 Parquet 연도별 파티셔닝 적재구조 가동
            df_filtered.to_parquet(
                path=BASE_DIR,
                index=False,
                engine='pyarrow',
                compression='snappy',
                partition_cols=['year']  # 지정 폴더 하위에 year=YYYY 디렉토리가 자동 바인딩됩니다.
            )

            target_path = os.path.join(BASE_DIR, f"year={year}")
            print(
                f"💾 {year}년 데이터 파티셔닝 저장 완료 -> {target_path} (총 {len(df_filtered)}건)")
            print(df_filtered.tail(1))  # 정상 적재 유무 검증을 위한 마지막 열 모니터링 출력

        except Exception as e:
            print(f"❌ {year}년 채권 데이터 처리 중 치명적 에러 발생: {e}")

    print("\n🎉 유효 영업일 최적화 반영 및 채권 데이터 연도별 파티셔닝 적재가 전면 완료되었습니다.")


if __name__ == "__main__":
    # 수집 스크립트 실행
    gather_and_save_bond_yields()
