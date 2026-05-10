from pykrx import stock
import pandas as pd
import numpy as np
import os
import requests
from datetime import datetime, timedelta
import pykrx.website.krx.market.wrap as wrap
from pykrx.website.krx.market.core import 업종분류현황
from pykrx.website.krx.krxio import KrxWebIo
from pandas import DataFrame
import streamlit as st


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

# 해당 주의 월요일과 금요일 구하기
# start_dt = target_date - timedelta(days=target_date.weekday())
# start_date = start_dt.strftime("%Y%m%d")
# end_date = stock.get_nearest_business_day_in_a_week()
start_date = "20260427"
end_date = "20260430"


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
    sector_sum = sector_merged.groupby('업종명')['순매수거래대금'].sum().reset_index()

    top_sectors = sector_sum.sort_values(by='순매수거래대금', ascending=False).head(5)
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
        "top_intensity": top_intensity[["종목명", "순매수거래대금", "순매수강도"]],  # 강도 추가
        "contrarian_gems": contrarian_gems  # 역발상 종목 추가
    }


@st.cache_data
def get_intensity_delta(start_date, end_date):

    # 1. 날짜 설정 (이번 주 vs 지난 주)
    curr_end_dt = datetime.strptime(end_date, "%Y%m%d")
    prev_end_dt = curr_end_dt - timedelta(days=7)
    prev_start_dt = prev_end_dt - timedelta(days=curr_end_dt.weekday())

    prev_start = prev_start_dt.strftime("%Y%m%d")
    prev_end = prev_end_dt.strftime("%Y%m%d")

    # 2. 이번 주 및 지난 주 분석 데이터 가져오기
    # (기존 get_investor_analysis 함수를 활용)
    curr_analysis = get_investor_analysis(start_date, end_date)
    prev_analysis = get_investor_analysis(prev_start, prev_end)

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
def get_sector_rotation(start_date, end_date):
    """이번 주와 지난주의 섹터 수급 순위 변화를 분석합니다."""
    # 1. 날짜 계산 (이번 주 vs 지난 주)
    curr_end_dt = datetime.strptime(end_date, "%Y%m%d")
    prev_end_dt = curr_end_dt - timedelta(days=7)
    prev_start_dt = prev_end_dt - timedelta(days=curr_end_dt.weekday())

    prev_start = prev_start_dt.strftime("%Y%m%d")
    prev_end = prev_end_dt.strftime("%Y%m%d")

    # 2. 데이터 가져오기
    curr_data = get_investor_analysis(start_date, end_date)
    prev_data = get_investor_analysis(prev_start, prev_end)

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


def get_futures_analysis(start_date, end_date):
    """
    코스피200 선물 가격 추이 및 투자자별 매매동향 수집
    """
    try:
        # 1. 선물 가격 추이 데이터
        io_trend = 선물최근월물시세()
        df_trend = io_trend.fetch(start_date, end_date)
        # "주간" 데이터만 필터링 (터미널 출력 로직 반영)
        df_price_trend = df_trend[df_trend.index.str.contains("주간")].copy()

        # 2. 투자자별 선물 매매동향
        io_investor = 선물투자자별거래실적()
        df_future_raw = io_investor.fetch(start_date, end_date)

        target_investors = ["기관합계", "기타법인", "개인", "외국인"]
        # 존재하지 않는 인덱스 대비 reindex
        df_future_investor = df_future_raw.reindex(target_investors).fillna(0)

        return {
            "price_trend": df_price_trend,
            "investor_trend": df_future_investor
        }
    except Exception as e:
        print(f"선물 데이터 분석 중 오류 발생: {e}")
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
