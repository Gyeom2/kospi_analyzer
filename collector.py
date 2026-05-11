import pandas as pd
import numpy as np
import os
import re
from datetime import datetime, timedelta
from pandas import DataFrame

from pykrx import stock
from pykrx.website.krx.market.core import 업종분류현황
from pykrx.website.krx.krxio import KrxWebIo

import streamlit as st
from workalendar.asia import SouthKorea


class 선물투자자별거래실적(KrxWebIo):
    @property
    def bld(self):
        # [13101] 투자자별 거래실적(개별선물) BLD 코드
        return "dbms/MDC/STAT/standard/MDCSTAT13101"

    def fetch(self, start_date: str, end_date: str, ticker: str = "KR___FUK2I") -> DataFrame:
        """
        코스피200 선물 등 개별 선물의 투자자별 매매 데이터를 가져옵니다.

        Args:
            start_date (str): 조회 시작일 (YYYYMMDD)
            end_date   (str): 조회 종료일 (YYYYMMDD)
            ticker     (str): 선물 종목 코드 (기본값: KR___FUK2I - 코스피200 선물)
        """
        # KrxWebIo.read()는 내부적으로 OTP 발급 및 세션 헤더를 자동으로 처리합니다.
        result = self.read(
            itvTpCd="1",        # 기간 선택
            strtDd=start_date,
            endDd=end_date,
            inqTpCd="1",        # 개별종목 선택
            prtType="AMT",      # 거래대금 기준 (수량 기준은 'VOL')
            prtCheck="SUN",     # 순매수 기준
            isuCd=ticker,       # 종목코드
            share="1",          # 계약 단위
            money="3"           # 백만원 단위
        )

        if "output" not in result:
            return DataFrame()

        df = DataFrame(result['output'])

        # 데이터 정제 (숫자 변환 및 컬럼명 변경)
        target_cols = {
            'INVST_TP_NM': '투자자',
            'NETBID_TRDVOL': '순매수계약수',
            'NETBID_TRDVAL': '순매수대금'
        }

        df = df[list(target_cols.keys())].rename(columns=target_cols)

        # 콤마 제거 및 숫자형 변환
        for col in ['순매수계약수', '순매수대금']:
            df[col] = df[col].str.replace(',', '').apply(
                pd.to_numeric, errors='coerce').fillna(0)

        return df.set_index('투자자')


class 선물최근월물시세(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT12701"

    def fetch(self, start_date: str, end_date: str, prod_id: str = "KR___FUK2I") -> DataFrame:
        # KrxWebIo.read()가 세션 및 헤더를 자동으로 관리합니다.
        result = self.read(
            prodId=prod_id,
            strtDd=start_date,
            endDd=end_date,
            share="1",
            money="3"
        )

        if "output" not in result:
            return DataFrame()

        df = DataFrame(result['output'])

        # 미결제약정을 포함한 주요 컬럼 선택
        df = df[['TRD_DD', 'TDD_CLSPRC', 'ACC_TRDVOL', 'ACC_OPNINT_QTY']]
        df.columns = ['일자', '종가', '거래량', '미결제약정']

        # 숫자 전처리: 콤마 제거 및 수치형 변환
        for col in ['종가', '거래량', '미결제약정']:
            df[col] = df[col].str.replace(',', '').apply(pd.to_numeric)

        return df.set_index('일자')


class 베이시스추이(KrxWebIo):
    @property
    def bld(self):
        # [13054] 선물/현물 가격 및 베이시스 추이
        return "dbms/MDC/STAT/standard/MDCSTAT13401"

    def fetch(self, start_date: str, end_date: str) -> DataFrame:
        """
        코스피200 선물과 현물의 베이시스 추이 데이터를 수집합니다.
        """
        result = self.read(
            bld=self.bld,
            locale="ko_KR",
            secugrpId="1",      # 지수선물
            aggBasTpCd="0",
            prodId="KR___FUK2I",  # 코스피200 선물
            expmmNo="1",        # 최근월물
            strtDd=start_date,
            endDd=end_date,
            csvxls_isNo="false"
        )

        if "output" not in result:
            return DataFrame()

        df = DataFrame(result['output'])

        # 필요한 컬럼 추출 및 변환
        target_cols = {
            'TRD_DD': '일자',
            'TDD_CLSPRC': '선물종가',
            'SPOT_PRC': '현물종가',
            'MKT_BASIS': '시장베이시스'
        }

        df = df[list(target_cols.keys())].rename(columns=target_cols)

        # 숫자형 변환 (콤마 제거)
        for col in ['선물종가', '현물종가', '시장베이시스']:
            df[col] = df[col].str.replace(',', '').apply(pd.to_numeric)

        return df.set_index('일자').sort_index()


class 프로그램매매동향(KrxWebIo):
    @property
    def bld(self):
        # 제공해주신 BLD 코드 적용
        return "dbms/MDC/STAT/standard/MDCSTAT02601"

    def fetch(self, start_date: str, end_date: str, market: str = "STK") -> DataFrame:
        """
        코스피/코스닥 프로그램 매매(차익, 비차익, 전체) 합계 데이터를 가져옵니다.
        """
        result = self.read(
            bld=self.bld,
            locale="ko_KR",
            mktId=market,       # STK: 코스피, KSQ: 코스닥
            strtDd=start_date,
            endDd=end_date,
            share="2",          # 제공된 payload 값 적용
            money="3",          # 단위: 원
            csvxls_isNo="false"
        )

        if "output" not in result:
            return DataFrame()

        df = DataFrame(result['output'])

        # 제공된 response 구조에 맞춘 컬럼 매핑
        # ITM_TP_NM: 구분(차익/비차익/전체), NETBID_TRDVAL: 순매수대금
        target_cols = {
            'ITM_TP_NM': '구분',
            'NETBID_TRDVAL': '순매수대금',
            'ASK_TRDVAL': '매도대금',
            'BID_TRDVAL': '매수대금'
        }

        df = df[list(target_cols.keys())].rename(columns=target_cols)

        # 숫자형 변환 (콤마 제거)
        for col in ['순매수대금', '매도대금', '매수대금']:
            df[col] = df[col].str.replace(',', '').apply(pd.to_numeric)

        return df.set_index('구분')


class 옵션투자자별거래실적(KrxWebIo):
    @property
    def bld(self):
        # [13101] 투자자별 거래실적(개별선물/옵션) BLD 코드
        return "dbms/MDC/STAT/standard/MDCSTAT13101"

    def fetch(self, start_date: str, end_date: str, option_type: str = "P") -> DataFrame:
        """
        코스피200 옵션의 투자자별 매매 데이터를 가져옵니다.

        Args:
            start_date (str): 조회 시작일 (YYYYMMDD)
            end_date   (str): 조회 종료일 (YYYYMMDD)
            option_type(str): 옵션 구분 (P: 풋옵션, C: 콜옵션)
        """
        # payload 구조 반영
        result = self.read(
            bld=self.bld,
            locale="ko_KR",
            strtDd=start_date,
            endDd=end_date,
            inqTpCd="1",        # 기간 합계
            prtType="AMT",      # 거래대금 기준
            prtCheck="SUN",     # 순매수 기준
            isuCd="KR___OPK2I",  # 코스피 200 옵션 종목코드
            isuOpt=option_type,  # P: 풋옵션, C: 콜옵션
            strtDdBox1=start_date,
            endDdBox1=end_date,
            share="1",
            money="3",          # 단위: 원
            csvxls_isNo="false"
        )

        if "output" not in result:
            return DataFrame()

        df = DataFrame(result['output'])

        # 데이터 정제 (숫자 변환 및 컬럼명 변경)
        target_cols = {
            'INVST_TP_NM': '투자자',
            'NETBID_TRDVOL': '순매수계약수',
            'NETBID_TRDVAL': '순매수대금'
        }

        df = df[list(target_cols.keys())].rename(columns=target_cols)

        # 콤마 제거 및 숫자형 변환
        for col in ['순매수계약수', '순매수대금']:
            df[col] = df[col].str.replace(',', '').apply(
                pd.to_numeric, errors='coerce').fillna(0)

        return df.set_index('투자자')


class 옵션풋콜레이쇼(KrxWebIo):
    @property
    def bld(self):
        # [13056] 종목별 풋콜레이쇼
        return "dbms/MDC/STAT/standard/MDCSTAT13601"

    def fetch(self, start_date: str, end_date: str) -> DataFrame:
        """
        코스피200 옵션의 풋콜레이쇼 추이 데이터를 수집합니다.
        """
        result = self.read(
            bld=self.bld,
            locale="ko_KR",
            prodId="KR___OPK2I",  # 코스피200 옵션
            strtDd=start_date,
            endDd=end_date,
            share="1",           # 거래량 기준
            csvxls_isNo="false"
        )

        if "output" not in result:
            return DataFrame()

        df = DataFrame(result['output'])

        # 컬럼명 매핑 및 데이터 변환
        target_cols = {
            'TRD_DD': '일자',
            'PVOL': '풋옵션거래량',
            'CVOL': '콜옵션거래량',
            'PCRATIO': '풋콜레이쇼'
        }

        df = df[list(target_cols.keys())].rename(columns=target_cols)

        # 숫자형 변환 (콤마 제거)
        for col in ['풋옵션거래량', '콜옵션거래량', '풋콜레이쇼']:
            df[col] = df[col].str.replace(',', '').apply(pd.to_numeric)

        return df.set_index('일자').sort_index()


class 옵션최근월물시세추이(KrxWebIo):
    @property
    def bld(self):
        # [15018] 최근월물 시세 추이(옵션) BLD 코드
        return "dbms/MDC/STAT/standard/MDCSTAT12702"

    def fetch(self, date: str, ticker: str = "KR___OPK2I", right_type: str = "ALL") -> DataFrame:
        """
        코스피200 옵션 등 최근월물 옵션의 시세 추이 데이터를 가져옵니다.

        Args:
            date       (str): 조회 일자 (YYYYMMDD)
            ticker     (str): 상품 ID (기본값: KR___OPK2I - 코스피200 옵션)
            right_type (str): 권리 구분 (ALL: 전체, 1: 콜, 2: 풋)
            aggBasTpCd (str): 시장 구분 ('': 전체, 0: 정규, 2: 야간)
        """
        # KrxWebIo.read()를 통해 페이로드 전달 및 데이터 수신
        result = self.read(
            bld=self.bld,
            prodId=ticker,      # 상품 ID
            aggBasTpCd="2",     # 마지막 영업일에서 다음 시작 영업일로 넘어갈 때의 데이터를 조회해야 최신 데이터 조회가능함
            strtDd=date,        # 시작일
            endDd=date,          # 종료일
            rghtTpCd=right_type,  # 권리구분
            share="1",           # 계약 단위
            money="1",           # 원 단위
            csvCheck="f"         # CSV 체크
        )

        if "output" not in result:
            return DataFrame()

        df = DataFrame(result['output'])

        # 1. 권리구분 및 행사가 파싱 함수 정의
        def parse_isu_nm(nm):
            # 1. C/P 구분 추출 (기존 로직 유지)
            right = '콜' if ' C ' in nm else '풋' if ' P ' in nm else '기타'

            # 2. 행사가 추출 로직 개선
            # 패턴 설명:
            # [\d,]+ : 숫자 또는 콤마가 하나 이상 반복
            # \.? : 소수점이 있을 수도 있고 없을 수도 있음
            # \d* : 소수점 뒤 숫자가 올 수 있음
            # \s*$ : 문자열 끝에 공백이 있어도 허용
            match = re.search(r'([\d,]+\.?\d*)\s*$', nm.strip())

            if match:
                strike_str = match.group(1).replace(',', '')  # 콤마 제거
                try:
                    strike_price = float(strike_str)
                except ValueError:
                    strike_price = 0.0
            else:
                strike_price = 0.0

            return pd.Series([right, strike_price])

        # 2. 파싱 적용 및 컬럼 생성
        df[['구분', '행사가']] = df['ISU_NM'].apply(parse_isu_nm)

        # 3. 데이터 정제 (주요 컬럼 선택 및 이름 변경)
        target_cols = {
            'ISU_NM': '종목명',
            '구분': '구분',
            '행사가': '행사가',
            'TDD_CLSPRC': '종가',
            'FLUC_RT': '등락률',
            'ACC_TRDVOL': '거래량',
            'ACC_TRDVAL': '거래대금',
            'ACC_OPNINT_QTY': '미결제약정',
            'NXTDD_BAS_PRC': '익일기준가'
        }

        # 존재하는 컬럼 필터링 및 이름 변경
        available_cols = [c for c in target_cols.keys() if c in df.columns]
        df = df[available_cols].rename(columns=target_cols)

        # 4. 숫자형 데이터 변환 (기존 로직 유지)
        numeric_cols = ['종가', '등락률', '거래량', '거래대금', '미결제약정', '익일기준가']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.replace(
                    ',', '').replace('-', '0')
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        return df.set_index('종목명')


class 국고채지표수익률(KrxWebIo):
    @property
    def bld(self):
        # [source: 6] bld: dbms/MDC/STAT/standard/MDCSTAT11501
        return "dbms/MDC/STAT/standard/MDCSTAT11501"

    def fetch(self, start_date: str, end_date: str) -> DataFrame:
        """
        국고채 지표별 수익률 추이 데이터를 가져옵니다.
        """
        # [source: 6] payload 구조 반영
        result = self.read(
            bld=self.bld,
            locale="ko_KR",
            strtDd=start_date,
            endDd=end_date,
            csvxls_isNo="false"
        )

        if "output" not in result:
            return DataFrame()

        # [source: 8] 응답 데이터 로드
        df = DataFrame(result['output'])

        # 컬럼 매핑 (PRC_YD1: 3년, PRC_YD2: 5년, PRC_YD3: 10년 등)
        target_cols = {
            'TRD_DD': '일자',
            'PRC_YD1': '국고채3년',
            'PRC_YD3': '국고채10년'
        }

        df = df[list(target_cols.keys())].rename(columns=target_cols)

        # 숫자형 변환
        for col in ['국고채3년', '국고채10년']:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # 날짜순 정렬 및 장단기 금리차(Spread) 계산
        df = df.sort_values(by='일자')
        df['장단기금리차'] = df['국고채10년'] - df['국고채3년']

        return df.set_index('일자')


# --- 1. config.txt에서 로그인 정보 읽기 및 설정 ---


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


def patched_get_market_sector_classifications(date, market):
    from pykrx.stock.stock_api import krx

    if isinstance(date, datetime):
        date = date.strftime("%Y%m%d")
    date = str(date).replace("-", "")

    df = patched_wrap_get_market_sector_classifications(date, market)

    if not df.empty and "종가" in df.columns:
        if (df["종가"] == 0).all(axis=None):
            return pd.DataFrame()
    # else:
        # print(f"inside else: {df}")
    return df


# --- 2. 실행 ---
# 특정 날짜 설정 (최신 영업일 기준)
target_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

# 해당 주의 영업일 중 가장 빠른 영업일, 가장 느린 영업일
# 이전 주의 영업일 중 가장 빠른 영업일, 가장 느린 영업일


@st.cache_data
def get_business_day():
    target_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    # target_date = datetime.strptime("20260313", "%Y%m%d")
    start_dt = target_date - timedelta(days=target_date.weekday())
    start_date = start_dt.strftime("%Y%m%d")
    b_start_date = stock.get_nearest_business_day_in_a_week(
        date=start_date, prev=False)

    end_dt = start_dt + timedelta(days=4)
    end_date = end_dt.strftime("%Y%m%d")
    b_end_date = stock.get_nearest_business_day_in_a_week(date=end_date)

    prev_start_dt = start_dt - timedelta(days=7)
    prev_start_date = prev_start_dt.strftime("%Y%m%d")
    b_prev_start_date = stock.get_nearest_business_day_in_a_week(
        date=prev_start_date, prev=False)

    prev_end_dt = end_dt - timedelta(days=7)
    prev_end_date = prev_end_dt.strftime("%Y%m%d")
    b_prev_end_date = stock.get_nearest_business_day_in_a_week(
        date=prev_end_date)

    return b_start_date, b_end_date, b_prev_start_date, b_prev_end_date


b_start_date, b_end_date, b_prev_start_date, b_prev_end_date = get_business_day()


@st.cache_data
def get_predicted_next_day(date_str):
    cal = SouthKorea()
    curr_date = datetime.strptime(date_str, "%Y%m%d").date()

    # 다음 날부터 시작해서 영업일이 나올 때까지 루프
    next_day = cal.add_working_days(curr_date, 1)

    return next_day.strftime("%Y%m%d")


@st.cache_data  # 데이터 수집 결과를 캐싱하여 성능 향상
def get_weekly_fund_flow(start_date, end_date):
    """주간 자금 흐름 요약 데이터를 가져오는 함수"""
    try:
        # 1. 투자자별 순매수 거래대금 조회
        df_trading_value = stock.get_market_trading_value_by_date(
            start_date, end_date, "KOSPI")

        weekly_sum = df_trading_value.sum()

        # 2. UI에 전달할 결과 데이터 구조화
        result = {
            "foreign": weekly_sum['외국인합계'],
            "institution": weekly_sum['기관합계'],
            "individual": weekly_sum['개인'],
            "etc": weekly_sum['기타법인'],
            "raw_df": df_trading_value  # 상세 내역이 필요할 경우를 대비해 DF 포함
        }
        return result
    except Exception as e:
        st.error(f"데이터 조회 중 오류 발생: {e}")
        return None


@st.cache_data
def get_kospi_cap(end_date):
    """시가총액 데이터프레임만 전용으로 가져와 캐싱"""
    return stock.get_market_cap(end_date, market="KOSPI")


@st.cache_data
def get_market_holding_status(end_date):
    """코스피 전체 시가총액 및 외국인 보유 현황을 계산하는 함수"""
    try:
        # A. 시장 전체 시가총액 합계 구하기
        df_cap = get_kospi_cap(end_date)
        total_market_cap = df_cap['시가총액'].sum()

        # B. 외국인 보유 시가총액 계산
        # 보유수량 데이터를 가져와 현재 종가와 곱하여 합산합니다.
        df_for = stock.get_exhaustion_rates_of_foreign_investment(
            end_date, market="KOSPI")

        # 인덱스(티커) 기준으로 병합하여 종가와 보유수량 매칭
        df_combined = pd.concat([df_cap[['종가']], df_for[['보유수량']]], axis=1)
        foreign_cap = (df_combined['종가'] * df_combined['보유수량']).sum()

        # C. 결과 구조화 (단위: 원)
        result = {
            "total_cap": total_market_cap,
            "foreign_cap": foreign_cap,
            "foreign_share": (foreign_cap / total_market_cap) * 100 if total_market_cap > 0 else 0
        }
        return result
    except Exception as e:
        st.error(f"보유 현황 데이터 조회 중 오류 발생: {e}")
        return None


def process_investor_data(df_net, df_price, df_sector, df_cap):
    """
    특정 투자자의 데이터를 받아 상위/하위 종목 및 섹터별 합산을 계산하는 독립 함수
    기존 분석에 순매수 강도 및 역발상 매수 종목 로직 추가
    """
    try:
        # 1. 종목별 순매수 데이터와 수익률(등락률) 결합
        # df_net의 인덱스가 티커일 경우를 대비해 join 시 인덱스를 활용합니다.
        merged = df_net.join(df_price[['등락률']], how='inner')

        # 2. 순매수 상위/하위 20개 종목 추출
        top_20 = merged.sort_values(by="순매수거래대금", ascending=False).head(20)
        bottom_20 = merged.sort_values(by="순매수거래대금", ascending=True).head(20)

        # 3. 섹터별 합산 로직
        # 종목명 기준으로 업종명을 매핑하기 위해 merge 수행
        sector_merged = pd.merge(df_net.reset_index(), df_sector[[
            '종목명', '업종명']], on='종목명', how='left')
        sector_sum = sector_merged.groupby(
            '업종명')['순매수거래대금'].sum().reset_index()

        top_sectors = sector_sum.sort_values(
            by='순매수거래대금', ascending=False).head(5)
        bottom_sectors = sector_sum.sort_values(
            by='순매수거래대금', ascending=True).head(5)

        # 4. 순매수 강도 분석 (추가)
        intensity_df = df_net.join(df_cap[['시가총액']], how='inner')
        intensity_df['순매수강도'] = (intensity_df['순매수거래대금'] /
                                 intensity_df['시가총액']) * 100
        top_intensity = intensity_df.sort_values(
            by="순매수강도", ascending=False).head(10)

        # 5. 역발상 매수 종목 (추가)
        # 순매도 상위 섹터 리스트 추출
        top_selling_sectors = bottom_sectors['업종명'].tolist()
        contrarian_gems = {}

        # 등락률 결합된 merged 활용
        gem_base = merged.join(df_sector[['업종명']], how='left')

        for sector in top_selling_sectors:
            sector_stocks = gem_base[gem_base['업종명'] == sector]
            # 해당 섹터 내 순매수 > 0 인 종목
            gems = sector_stocks[sector_stocks['순매수거래대금'] > 0].sort_values(
                by='순매수거래대금', ascending=False).head(5)
            if not gems.empty:
                contrarian_gems[sector] = gems[['종목명', '순매수거래대금', '등락률']]

        return {
            "top_20": top_20[["종목명", "순매수거래대금", "등락률"]],
            "bottom_20": bottom_20[["종목명", "순매수거래대금", "등락률"]],
            "top_sectors": top_sectors,
            "bottom_sectors": bottom_sectors,
            # 강도 추가
            "top_intensity": top_intensity[["종목명", "순매수거래대금", "순매수강도"]],
            "contrarian_gems": contrarian_gems  # 역발상 종목 추가
        }
    except Exception as e:
        print(f"process_investor_data 중 오류 발생: {e}")
        return None


@st.cache_data
def get_intensity_delta(start_date, end_date, prev_start_date, prev_end_date):

    # 2. 이번 주 및 지난 주 분석 데이터 가져오기
    curr_analysis = get_investor_analysis(start_date, end_date)
    prev_analysis = get_investor_analysis(prev_start_date, prev_end_date)

    if not curr_analysis or not prev_analysis:
        return None

    results = {}
    for inv in ["외국인", "기관"]:
        curr_int = curr_analysis[inv]['top_intensity']
        prev_int = prev_analysis[inv]['top_intensity']

        # 종목명을 기준으로 두 데이터프레임 결합 (순매수강도 비교)
        delta_df = curr_int.merge(
            prev_int[['종목명', '순매수강도']],
            on='종목명',
            how='left',
            suffixes=('', '_prev')
        ).fillna(0)

        # 변화량(Delta) 계산
        delta_df['강도변화'] = delta_df['순매수강도'] - delta_df['순매수강도_prev']
        results[inv] = delta_df.sort_values(by='강도변화', ascending=False)

    return results


@st.cache_data
def get_sector_rotation(start_date, end_date, prev_start_date, prev_end_date):
    """이번 주와 지난주의 섹터 수급 순위 변화를 분석합니다."""

    # 2. 데이터 가져오기
    curr_data = get_investor_analysis(start_date, end_date)
    prev_data = get_investor_analysis(prev_start_date, prev_end_date)

    if not curr_data or not prev_data:
        return None

    rotation_results = {}
    for inv in ["외국인", "기관"]:
        # 이번 주 섹터 전체 순위 (head(5)가 아닌 전체를 가져오기 위해 로직 수정 필요할 수 있음)
        curr_sectors = curr_data[inv]['top_sectors'].copy()
        prev_sectors = prev_data[inv]['top_sectors'].copy()

        # 순위 부여
        curr_sectors['현재순위'] = range(1, len(curr_sectors) + 1)
        prev_sectors['이전순위'] = range(1, len(prev_sectors) + 1)

        # 데이터 결합
        merged = curr_sectors.merge(
            prev_sectors[['업종명', '이전순위']],
            on='업종명',
            how='left'
        )

        # 신규 진입 섹터 처리 및 순위 변동 계산
        merged['이전순위'] = merged['이전순위'].fillna(10)  # 5위권 밖은 10위로 가정
        merged['순위변동'] = merged['이전순위'] - merged['현재순위']

        rotation_results[inv] = merged

    return rotation_results


@st.cache_data
def get_investor_analysis(start_date, end_date):
    """기존 코드의 흐름을 따라 데이터를 수집하고 분리된 함수로 전달"""
    try:
        # print(f"get_investor_analysis내부: {start_date},{end_date}")
        # 데이터 수집 (기존 코드 로직)
        df_net_for = stock.get_market_net_purchases_of_equities_by_ticker(
            start_date, end_date, "KOSPI", investor='외국인')
        df_net_inst = stock.get_market_net_purchases_of_equities_by_ticker(
            start_date, end_date, "KOSPI", investor='기관합계')

        # 427라인: 수익률 데이터 수집
        df_price = stock.get_market_price_change_by_ticker(
            start_date, end_date)

        # 업종 분류 데이터 수집
        df_sector = patched_get_market_sector_classifications(
            end_date, "KOSPI")
        # 시가총액 추가
        df_cap = get_kospi_cap(end_date)

        # 430, 437라인의 호출 부분을 분리된 함수로 대체
        return {
            "외국인": process_investor_data(df_net_for, df_price, df_sector, df_cap),
            "기관": process_investor_data(df_net_inst, df_price, df_sector, df_cap)
        }
    except Exception as e:
        print(f"분석 중 오류 발생: {e}")
        return None


@st.cache_data
def get_supply_continuity(end_date):
    """최근 10영업일간 외국인/기관의 순매수 연속성을 분석합니다."""
    try:
        # 1. 최근 10영업일 날짜 리스트 가져오기
        # stock.get_market_ohlcv_by_date 등에서 날짜 리스트를 추출하거나 직접 계산
        dt_end = datetime.strptime(end_date, "%Y%m%d")
        dt_start = dt_end - timedelta(days=15)  # 넉넉하게 15일 전부터 조회
        start_date = dt_start.strftime("%Y%m%d")

        results = {}
        for inv_name, inv_key in [("외국인", "외국인"), ("기관", "기관합계")]:
            # 2. 일자별 종목별 순매수량 조회
            df = stock.get_market_net_purchases_of_equities_by_ticker(
                start_date, end_date, "KOSPI", investor=inv_key
            )

            # 실제 데이터가 존재하는 날짜 수 확인 (최대 10일 제한)
            df_daily = stock.get_market_trading_value_by_date(
                start_date, end_date, "KOSPI")
            valid_days = df_daily.index.unique()[-10:]

            # 3. 10일치 일별 데이터를 합산하여 '순매수 일수' 계산
            # (이 부분은 get_market_net_purchases_of_equities_by_ticker가 기간 합계이므로,
            # 정확한 일수 계산을 위해 루프를 돌거나 일별 데이터를 병합해야 합니다.)

            # 간소화된 로직: 일별 데이터를 순회하며 매수 여부(>0) 체크
            count_df = None
            for day in valid_days:
                day_str = day.strftime("%Y%m%d")
                day_df = stock.get_market_net_purchases_of_equities_by_ticker(
                    day_str, day_str, "KOSPI", investor=inv_key
                )

                is_buying = (day_df['순매수거래대금'] > 0).astype(int)
                if count_df is None:
                    count_df = is_buying
                else:
                    count_df += is_buying

            # 결과 정리
            res_df = pd.DataFrame({
                "종목명": df['종목명'],
                "순매수일수": count_df,
                "총순매수액": df['순매수거래대금']
            }).dropna()

            # 1. 1,000억 원 이상 필터링 (1,000억 = 100,000,000,000)
            res_df = res_df[res_df['총순매수액'] >= 100_000_000_000]

            # 2. 순매수 일수를 정수형으로 변환 (소수점 제거)
            res_df['순매수일수'] = res_df['순매수일수'].astype(int)

            # 점수 및 아이콘 부여 로직
            def assign_status(days):
                if days >= 5:
                    return "🔥 집중매집"
                elif days >= 3:
                    return "👀 주목"
                else:
                    return "-"

            res_df['상태'] = res_df['순매수일수'].apply(assign_status)
            results[inv_name] = res_df.sort_values(
                by=["순매수일수", "총순매수액"], ascending=False)

        return results
    except Exception as e:
        print(f"수급 연속성 분석 중 오류: {e}")
        return None


@st.cache_data
def get_sell_continuity(end_date):
    """최근 10영업일간 외국인/기관의 순매도 연속성을 분석합니다."""
    try:
        # 날짜 설정 (최근 10영업일 추출 로직은 이전과 동일)
        dt_end = datetime.strptime(end_date, "%Y%m%d")
        dt_start = dt_end - timedelta(days=15)
        start_date = dt_start.strftime("%Y%m%d")

        results = {}
        for inv_name, inv_key in [("외국인", "외국인"), ("기관", "기관합계")]:
            df = stock.get_market_net_purchases_of_equities_by_ticker(
                start_date, end_date, "KOSPI", investor=inv_key)

            # 실제 데이터가 존재하는 날짜 확인
            df_daily = stock.get_market_trading_value_by_date(
                start_date, end_date, "KOSPI")
            valid_days = df_daily.index.unique()[-10:]

            count_df = None
            for day in valid_days:
                day_str = day.strftime("%Y%m%d")
                day_df = stock.get_market_net_purchases_of_equities_by_ticker(
                    day_str, day_str, "KOSPI", investor=inv_key)
                # 순매수액이 0보다 작으면(매도) 1점 추가
                is_selling = (day_df['순매수거래대금'] < 0).astype(int)
                count_df = is_selling if count_df is None else count_df + is_selling

            res_df = pd.DataFrame({
                "종목명": df['종목명'],
                "순매도일수": count_df,
                "총순매도액": df['순매수거래대금']  # 음수값 유지
            }).dropna()

            # 필터링: 총 순매도액이 -500억 이하인 종목만 (매도는 매수보다 기준을 낮게 잡아도 무방)
            res_df = res_df[res_df['총순매도액'] <= -50_000_000_000]
            res_df['순매도일수'] = res_df['순매도일수'].astype(int)

            def assign_sell_status(days):
                if days >= 8:
                    return "🚨 지속 투매"
                elif days >= 5:
                    return "⚠️ 주의 요망"
                else:
                    return "-"

            res_df['상태'] = res_df['순매도일수'].apply(assign_sell_status)
            # 매도액이 큰 순서대로 정렬 (절대값이 큰 순)
            results[inv_name] = res_df.sort_values(
                by=["순매도일수", "총순매도액"], ascending=[False, True])

        return results
    except Exception as e:
        print(f"순매도 분석 중 오류: {e}")
        return None


@st.cache_data
def analyze_52w_high_low(db_path):
    """52주 신고가/신저가 및 섹터 분석 데이터를 반환하는 함수"""
    try:
        # 1. 파티셔닝된 전체 데이터 로드
        if not os.path.exists(db_path):
            return None

        df = pd.read_parquet(db_path)
        df = df.sort_values(['종목코드', '일자'])

        # 2. 52주(약 250영업일) 기준 최고가/최저가 계산
        window = 250
        df['52W_High'] = df.groupby('종목코드')['고가'].transform(
            lambda x: x.rolling(window=window, min_periods=1).max()
        )
        df['52W_Low'] = df.groupby('종목코드')['저가'].transform(
            lambda x: x.rolling(window=window, min_periods=1).min()
        )

        # 3. 가장 최근 날짜 데이터 추출
        latest_date = df['일자'].max()
        today_df = df[df['일자'] == latest_date].copy()

        # 4. 신고가/신저가 종목 판별
        high_breakouts = today_df[today_df['고가'] >= today_df['52W_High']]
        low_breakouts = today_df[today_df['저가'] <= today_df['52W_Low']]

        # 5. 결과 데이터 구조화 (UI 전달용)
        result = {
            "date": latest_date,
            "high_count": len(high_breakouts),
            "low_count": len(low_breakouts),
            "high_df": high_breakouts[['종목코드', '종목명', '업종명', '종가']],
            "low_df": low_breakouts[['종목코드', '종목명', '업종명', '종가']],
            "sector_rank": high_breakouts['업종명'].value_counts() if '업종명' in high_breakouts.columns else None,
            "raw_df": today_df  # 전체 요약 데이터가 필요할 경우 대비
        }

        return result

    except Exception as e:
        st.error(f"52주 데이터 분석 중 오류 발생: {e}")
        return None


@st.cache_data
def get_futures_analysis(start_date, end_date):
    """
    코스피200 선물 가격 추이, 미결제약정 및 투자자별 매매동향 수집 및 분석
    """
    try:
        # 1. 선물 가격 추이 데이터
        io_trend = 선물최근월물시세()
        df_trend = io_trend.fetch(start_date, end_date)
        # "주간" 데이터만 필터링 (터미널 출력 로직 반영)
        df_price_trend = df_trend[df_trend.index.str.contains("주간")].copy()

        # 2. 미결제약정 상관관계 분석 로직 추가
        oi_interpretation = None
        if len(df_price_trend) >= 2:
            curr = df_price_trend.iloc[-1]
            prev = df_price_trend.iloc[0]

            p_diff = curr['종가'] - prev['종가']
            o_diff = curr['미결제약정'] - prev['미결제약정']

            # 국면 판단
            if p_diff > 0 and o_diff > 0:
                phase, desc = "강세 시장", "신규 매수 유입으로 상승세 강화"
            elif p_diff > 0 and o_diff < 0:
                phase, desc = "기술적 반등", "숏커버링(매도 포지션 청산)에 의한 일시적 상승"
            elif p_diff < 0 and o_diff > 0:
                phase, desc = "약세 시장", "신규 매도 유입으로 하락세 강화"
            elif p_diff < 0 and o_diff < 0:
                phase, desc = "하락 둔화", "롱청산(매수 포지션 이익실현/손절)으로 인한 하락"
            else:
                phase, desc = "방향성 탐색", "지수 및 미결제약정 변화가 미미함"

            oi_interpretation = {
                "phase": phase,
                "desc": desc,
                "p_diff": p_diff,
                "o_diff": o_diff
            }

        # 3. 투자자별 선물 매매동향
        io_investor = 선물투자자별거래실적()
        df_future_raw = io_investor.fetch(start_date, end_date)

        target_investors = ["기관합계", "기타법인", "개인", "외국인"]
        df_future_investor = df_future_raw.reindex(target_investors).fillna(0)

        return {
            # 여기에 미결제약정 컬럼이 포함되어 있음
            "price_trend": df_price_trend,
            "investor_trend": df_future_investor,
            "oi_analysis": oi_interpretation    # 분석 결과 추가
        }
    except Exception as e:
        print(f"선물 데이터 분석 중 오류 발생: {e}")
        return None


@st.cache_data
def get_basis_analysis(start_date, end_date):
    """베이시스 추이 데이터 반환"""
    try:
        io = 베이시스추이()
        return io.fetch(start_date, end_date)
    except Exception as e:
        print(f"베이시스 조회 오류: {e}")
        return None


@st.cache_data
def get_program_trading_summary(start_date, end_date):
    """프로그램 매매 요약 데이터 수집 함수"""
    try:
        io = 프로그램매매동향()
        df = io.fetch(start_date, end_date, "STK")
        if df.empty:
            return None

        return {
            "arbitrage": df.loc["차익", "순매수대금"],
            "non_arbitrage": df.loc["비차익", "순매수대금"],
            "total": df.loc["전체", "순매수대금"],
            "raw_df": df
        }
    except Exception as e:
        print(f"프로그램 매매 분석 오류: {e}")
        return None


def get_options_analysis(start_date, end_date):
    """
    코스피200 옵션 투자자별 매매동향 수집 및 포지션 해석
    """
    try:
        io_option = 옵션투자자별거래실적()  # 클래스 인스턴스 생성

        # 1. 콜옵션(C) 및 풋옵션(P) 조회
        df_call = io_option.fetch(start_date, end_date, option_type="C")
        df_put = io_option.fetch(start_date, end_date, option_type="P")

        # 2. 데이터 병합 및 필터링
        summary = pd.DataFrame({
            "콜옵션_순매수": df_call['순매수대금'],
            "풋옵션_순매수": df_put['순매수대금']
        })

        # 시각화용 데이터 가공: 풋옵션 매수는 하방 배팅이므로 부호를 반전(-)
        # 콜옵션 매수(+) vs 풋옵션 매수(-) 구조로 변경
        summary['풋옵션_반전'] = -summary['풋옵션_순매수']

        target_investors = ["외국인", "개인", "기관합계", "기타법인"]  # 순서 적용
        summary_filtered = summary.reindex(target_investors).fillna(0)

        # 3. 외국인 포지션 해석 로직
        foreign_call = summary_filtered.loc["외국인", "콜옵션_순매수"]
        foreign_put = summary_filtered.loc["외국인", "풋옵션_순매수"]

        position_text = ""
        if foreign_call > 0 and foreign_put < 0:
            position_text = "상방 배팅 (콜 매수 / 풋 매도)"
        elif foreign_call < 0 and foreign_put > 0:
            position_text = "하방 배팅 (콜 매도 / 풋 매수)"
        else:
            position_text = "중립 또는 복합 전략 구사 중"

        return {
            "summary": summary_filtered,
            "foreign_pos": {
                "call": foreign_call,
                "put": foreign_put,
                "text": position_text
            }
        }
    except Exception as e:
        print(f"옵션 분석 중 오류 발생: {e}")
        return None


@st.cache_data
def get_pcr_analysis(start_date, end_date):
    """풋콜레이쇼 데이터 반환"""
    try:
        io = 옵션풋콜레이쇼()
        return io.fetch(start_date, end_date)
    except Exception as e:
        print(f"풋콜레이쇼 조회 오류: {e}")
        return None


@st.cache_data
def get_max_pain_analysis(date):
    """
    특정 날짜의 옵션 시세 데이터를 바탕으로 맥스페인(Max Pain) 가격을 계산합니다.
    281라인의 '옵션최근월물시세추이' 클래스 결과물을 활용합니다.
    """
    try:
        io_price = 옵션최근월물시세추이()
        # 281라인 클래스를 활용해 전체 옵션 시세(ALL)를 가져옵니다.
        # 옵션최근월물시세추이의 경우 5/8(금) 야간에 이루어진 거래는 5/11(월) 야간으로 조회할 수 있음
        # 가장 마지막 영업일에서 그 다음 시작 영업일, 야간으로 조회해야 최신 옵션 데이터 현황을 조회할 수 있음

        df_options = io_price.fetch(date, right_type="ALL")
        # print(f"date: {date}")

        if df_options.empty:
            return None

        # 1. 행사가 리스트 추출 및 정렬
        # fetch 메서드에서 이미 '행사가' 컬럼이 생성되어 있으므로 이를 활용합니다.
        strikes = sorted(df_options['행사가'].unique())

        # 2. 콜/풋 데이터 분리 (fetch 메서드의 '구분' 컬럼 활용)
        df_call = df_options[df_options['구분'] == '콜']
        df_put = df_options[df_options['구분'] == '풋']

        pain_values = []

        # 3. 각 행사가별로 만기 시 총 가치(매도자 지급액) 계산
        for s in strikes:
            # 콜옵션 페이오프 합계: Max(0, 만기지수 - 행사가) * 미결제약정
            call_loss = df_call.apply(
                lambda x: max(0, s - x['행사가']) * x['미결제약정'], axis=1
            ).sum()

            # 풋옵션 페이오프 합계: Max(0, 행사가 - 만기지수) * 미결제약정
            put_loss = df_put.apply(
                lambda x: max(0, x['행사가'] - s) * x['미결제약정'], axis=1
            ).sum()

            pain_values.append(call_loss + put_loss)

        # 4. 총 손실이 최소가 되는 지점(Min)의 인덱스를 찾아 행사가 반환
        min_pain_index = np.argmin(pain_values)
        max_pain_price = strikes[min_pain_index]

        # 5. 결과 구조화
        pain_df = pd.DataFrame({
            '행사가': strikes,
            '고통지수': pain_values
        })

        return {
            "max_pain": max_pain_price,
            "pain_df": pain_df,
            # 시각화 등에 활용할 데이터셋
            "calls": df_call[['행사가', '미결제약정', '종가']],
            "puts": df_put[['행사가', '미결제약정', '종가']]
        }

    except Exception as e:
        st.error(f"맥스페인 계산 중 오류 발생: {e}")
        return None


def get_bond_analysis(start_date, end_date):
    """
    국고채 금리 수집 및 변동성 분석
    """
    try:
        bond_io = 국고채지표수익률()
        df_bond = bond_io.fetch(start_date, end_date)

        if df_bond.empty:
            return None

        # 시각화를 위해 인덱스(날짜) 정렬 (과거 -> 현재)
        df_bond = df_bond.sort_index(ascending=True)

        # 변동폭 계산
        first_day = df_bond.iloc[0]
        last_day = df_bond.iloc[-1]

        analysis = {
            "df": df_bond,
            "latest": last_day,
            "diff": {
                "3y": last_day['국고채3년'] - first_day['국고채3년'],
                "10y": last_day['국고채10년'] - first_day['국고채10년'],
                "spread": last_day['장단기금리차'] - first_day['장단기금리차']
            }
        }
        return analysis
    except Exception as e:
        print(f"채권 분석 중 오류 발생: {e}")
        return None
