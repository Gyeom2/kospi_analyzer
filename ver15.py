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


original_get_market_sector = wrap.get_market_sector_classifications


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


# --- 2. 기존 로직 실행 ---
# 특정 날짜 설정 (최신 영업일 기준)
target_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

# 해당 주의 월요일과 금요일 구하기
start_dt = target_date - timedelta(days=target_date.weekday())
start_date = start_dt.strftime("%Y%m%d")
end_date = stock.get_nearest_business_day_in_a_week()

try:
    print(f" 분석 기간: {start_date} ~ {end_date} (1주일)")

    # --- 2. 투자자별 순매수 데이터 가져오기 ---
    # get_market_net_purchases_of_equities_by_ticker는 해당 기간의 '합계'를 반환합니다.
    # '전체' 시장의 투자자별 요약을 위해 on='investor'를 사용하거나
    # 모든 종목의 합계를 구하는 방식을 사용합니다.

    df_investors = stock.get_market_net_purchases_of_equities(
        start_date, end_date, "KOSPI")

    # 주요 투자자 리스트 (기관합계, 외국인, 개인)

    df_trading_value = stock.get_market_trading_value_by_date(
        start_date, end_date, "KOSPI")
    weekly_sum = df_trading_value.sum()

    foreign_total = weekly_sum['외국인합계']
    inst_total = weekly_sum['기관합계']
    indiv_total = weekly_sum['개인']
    etc_total = weekly_sum['기타법인']

    # --- 3. 데이터 정제 및 출력 ---
    print(f"\n===== {start_date} ~ {end_date} 주간 자금 흐름 요약 =====")
    print(f"외국인합계: {foreign_total:>15,} (원)")
    print(f"기관합계  : {inst_total:>15,} (원)")
    print(f"개인      : {indiv_total:>15,} (원)")
    print(f"기타법인      : {etc_total:>15,} (원)")
    print("-" * 50)

    # 4. 유입/유출 판단 로직
    for name, value in [("외국인", foreign_total), ("기관", inst_total), ("개인", indiv_total), ("기타법인", etc_total)]:
        status = "순유입(↑)" if value > 0 else "순유출(↓)"
        print(f"{name} 세력은 이번 주에 {status} 포지션입니다.")

except Exception as e:
    # 에러 발생 시 컬럼명 확인을 위해 출력
    print(f"데이터 조회 중 오류 발생: {e}")
    print(f"현재 데이터프레임 컬럼명: {df_trading_value.columns.tolist()}")

# 코스피(KOSPI) 전체 종목 정보 가져오기
try:
    # A. 시장 전체 시가총액 합계 구하기
    df_cap = stock.get_market_cap(end_date, market="KOSPI")
    total_market_cap = df_cap['시가총액'].sum()

    # B. 투자자별 보유 시가총액 데이터 가져오기
    # 이 함수는 해당 날짜의 투자자별 '보유금액' 정보를 포함합니다.
    df_investor = stock.get_market_net_purchases_of_equities_by_ticker(
        start_date, end_date, "KOSPI")

    # C. 각 투자자별 보유 금액 추출 (단위: 원)
    # '매수'나 '도'가 아닌 '보유금액' 관련 컬럼이 있는지 확인이 필요하지만,
    # 통상적으로 '금액' 관련 지표를 합산하여 비중을 도출합니다.

    # 외국인은 이미 우리가 구한 방식을 사용하는 것이 가장 정확합니다.
    df_for = stock.get_exhaustion_rates_of_foreign_investment(
        end_date, market="KOSPI")
    df_combined = pd.concat([df_cap, df_for], axis=1)
    foreign_cap = (df_combined['종가'] * df_combined['보유수량']).sum()

    # 결과 출력
    total_market_cap = df_cap['시가총액'].sum()

    print(f"\n===== {end_date} 코스피 투자자별 보유 현황 =====")
    print(f"전체 시가총액: {total_market_cap:>25,} 원")
    print("-" * 60)
    print(
        f"외국인 보유분: {foreign_cap:>25,} 원 ({foreign_cap/total_market_cap*100:>6.2f}%)")

except Exception as e:
    # 컬럼명을 확인하기 위해 에러 발생 시 데이터프레임 구조를 출력해봅니다.
    print(f"데이터 구조 확인용 출력:\n{df_investor.head()}")
    print(f"오류 발생: {e}")


try:
    # --- 1. 종목별 주간 순매수 데이터 가져오기 ---
    # 이 함수는 시작일부터 종료일까지의 누적 순매수 데이터를 반환합니다.
    df_net_purchase_for = stock.get_market_net_purchases_of_equities_by_ticker(
        start_date, end_date, "KOSPI", investor='외국인')
    df_net_purchase_inst = stock.get_market_net_purchases_of_equities_by_ticker(
        start_date, end_date, "KOSPI", investor='기관합계')

    # --- 2. 투자자별 TOP 20 추출 함수 ---
    # def get_top_bottom(df, num):
    #     # '순매수거래대금' 컬럼을 기준으로 정렬합니다.
    #     # 상위 20개 (순매수 1위~20위)
    #     top_num = df.sort_values(by="순매수거래대금", ascending=False).head(num)
    #     # 하위 20개 (순매도 1위~20위, 즉 값이 가장 작은 것 20개)
    #     bottom_num = df.sort_values(by="순매수거래대금", ascending=True).head(num)
    #     # 가독성을 위해 '종목명'과 '순매수거래대금' 컬럼만 필터링하여 반환합니다.
    #     return top_num[["종목명", "순매수거래대금"]], bottom_num[["종목명", "순매수거래대금"]]
    # # --- 3. 외국인 및 기관 데이터 추출 ---
    # # 외국인 결과 출력
    # for_top, for_bottom = get_top_bottom(df_net_purchase_for, 20)
    # print(f"\n==== 외국인 주간 리포트 ====")
    # print(f"[순매수 TOP 20]\n{for_top}")
    # print(f"\n[순매도 TOP 20]\n{for_bottom}")

    # # 기관 결과 출력
    # inst_top, inst_bottom = get_top_bottom(df_net_purchase_inst, 20)
    # print(f"\n==== 기관합계 주간 리포트 ====")
    # print(f"[순매수 TOP 20]\n{inst_top}")
    # print(f"\n[순매도 TOP 20]\n{inst_bottom}")

    def get_top_bottom_with_yield(df_net, df_price, num=20):
        # 1. 순매수 데이터와 등락률(수익률) 데이터 결합
        # df_price에는 '등락률' 컬럼이 포함되어 있습니다.
        merged = df_net.join(df_price[['등락률']], how='inner')

        # 2. 상위/하위 20개 추출
        top_num = merged.sort_values(by="순매수거래대금", ascending=False).head(num)
        bottom_num = merged.sort_values(by="순매수거래대금", ascending=True).head(num)

        return top_num[["종목명", "순매수거래대금", "등락률"]], bottom_num[["종목명", "순매수거래대금", "등락률"]]

    # --- 주간 수익률 데이터 가져오기 ---
    df_price = stock.get_market_price_change_by_ticker(start_date, end_date)

    # 외국인 결과 출력 (수익률 포함)
    for_top_yield, for_bottom_yield = get_top_bottom_with_yield(
        df_net_purchase_for, df_price)
    print(f"\n==== 외국인 주간 리포트 (수익률 포함) ====")
    print(f"[순매수 TOP 20]\n{for_top_yield}")
    print(f"[순매도 TOP 20]\n{for_bottom_yield}")

    # 기관 결과 출력 (수익률 포함)
    inst_top_yield, inst_bottom_yield = get_top_bottom_with_yield(
        df_net_purchase_inst, df_price)
    print(f"\n==== 기관합계 주간 리포트 (수익률 포함) ====")
    print(f"[순매수 TOP 20]\n{inst_top_yield}")
    print(f"[순매도 TOP 20]\n{inst_bottom_yield}")

    df_sector = patched_get_market_sector_classifications(end_date, "KOSPI")

    def get_top_5_sector(net_perchase_investor):
        merged_df = pd.merge(
            net_perchase_investor,
            df_sector[['종목명', '업종명']],
            on='종목명',
            how='left'
        )

        sector_sum = merged_df.groupby('업종명')[['순매수거래대금', '매수거래대금']].sum()
        top_5_sectors = sector_sum.sort_values(
            by='순매수거래대금', ascending=False).head(5)

        return top_5_sectors

    def get_bottom_5_sector(net_perchase_investor):
        merged_df = pd.merge(
            net_perchase_investor,
            df_sector[['종목명', '업종명']],
            on='종목명',
            how='left'
        )

        sector_sum = merged_df.groupby('업종명')[['순매수거래대금', '매도거래대금']].sum()
        bottom_5_sectors = sector_sum.sort_values(
            by='순매수거래대금', ascending=True).head(5)

        return bottom_5_sectors

    for inv_name, df_data in [("외국인", df_net_purchase_for), ("기관합계", df_net_purchase_inst)]:
        top_5_sectors = get_top_5_sector(df_data)
        bottom_5_sectors = get_bottom_5_sector(df_data)
        formatted_df_top = top_5_sectors.map(lambda x: f"{x:,.0f}")
        formatted_df_bottom = bottom_5_sectors.map(lambda x: f"{x:,.0f}")
        print(f"\n--- {inv_name} 주간 순매수 거래대금 상위 5개 섹터 (합계) ---")
        print(formatted_df_top)
        print(f"\n--- {inv_name} 주간 순매도 거래대금 상위 5개 섹터 (합계) ---")
        print(formatted_df_bottom)

except Exception as e:
    print(f"종목별 순매수 데이터 조회 중 오류 발생: {e}")

# --- 순매수 강도 분석 섹션 ---


def get_top_intensity(df_net, df_cap, num=5):
    # 시가총액 데이터와 병합 (티커/종목코드 기준)
    merged = df_net.join(df_cap[['시가총액']], how='inner')

    # 순매수 강도 계산: (순매수거래대금 / 시가총액) * 100
    merged['순매수강도'] = (merged['순매수거래대금'] / merged['시가총액']) * 100

    # 강도 순으로 정렬 후 상위 추출
    top_intensity = merged.sort_values(by="순매수강도", ascending=False).head(num)
    return top_intensity[["종목명", "순매수거래대금", "시가총액", "순매수강도"]]


# 외국인 및 기관 강도 상위 5개 추출
for_intensity = get_top_intensity(df_net_purchase_for, df_cap)
inst_intensity = get_top_intensity(df_net_purchase_inst, df_cap)

print(f"\n==== 외국인 순매수 강도 TOP 5 ({end_date}) ====")
print(for_intensity)

print(f"\n==== 기관합계 순매수 강도 TOP 5 ({end_date}) ====")
print(inst_intensity)


def get_stock_gems_in_selling_sector(df_net_purchase, df_sector, df_price, inv_name, top_n_sectors=5):
    # 1. 종목별 데이터와 업종 데이터 결합 (인덱스인 종목코드 기준)
    merged_df = df_net_purchase.join(df_sector[['업종명']], how='left')
    merged_df = merged_df.join(df_price[['등락률']], how='left')

    # 2. 섹터별 합산 순매수금액 계산
    sector_sum = merged_df.groupby('업종명')['순매수거래대금'].sum()

    # 3. 순매도액이 가장 큰(금액이 가장 낮은) 상위 N개 섹터 추출
    top_selling_sectors = sector_sum.sort_values(
        ascending=True).head(top_n_sectors).index.tolist()

    print(f"\n==== [{inv_name}] 순매도 상위 섹터 내 '역발상 매수' 종목 ====")
    print("-" * 65)

    for sector in top_selling_sectors:
        # 해당 섹터에 속한 종목들 필터링
        sector_stocks = merged_df[merged_df['업종명'] == sector]
        # 그 중 순매수액이 0보다 큰 종목만 추출
        gems = sector_stocks[sector_stocks['순매수거래대금'] >
                             0].sort_values(by='순매수거래대금', ascending=False)

        if not gems.empty:
            print(f"▶ 섹터: {sector} (섹터 전체 순매도: {sector_sum[sector]:,.0f}원)")
            # 상위 5개 종목만 표출
            print(gems[['종목명', '순매수거래대금', '등락률']].head(5))
            print("-" * 40)
        else:
            print(f"▶ 섹터: {sector} 내 순매수 종목 없음")


get_stock_gems_in_selling_sector(
    df_net_purchase_for, df_sector, df_price, "외국인")
get_stock_gems_in_selling_sector(
    df_net_purchase_inst, df_sector, df_price,  "기관합계")


# --- 5. 지수선물 매매동향 및 코스피 지수 변동추이 분석 ---
try:
    io_trend = 선물최근월물시세()
    df_trend = io_trend.fetch(start_date, end_date)
    df_day_only = df_trend[df_trend.index.str.contains("주간")]
    print("===== 코스피200 선물 거래 추이 =====")
    print(df_day_only)
except Exception as e:
    print(f"코스피200 지수선물 데이터 조회 중 오류 발생: {e}")


try:
    # 클래스 인스턴스 생성
    io = 선물투자자별거래실적()

    # 데이터 조회
    df_future = io.fetch(start_date, end_date)

    target_investors = ["기관합계", "기타법인", "개인", "외국인"]

    df_filtered = df_future.reindex(target_investors)

    df_formatted = df_filtered.map(
        lambda x: f"{x:,.0f}" if pd.notnull(x) else "0")

    if not df_future.empty:
        print("===== 코스피200 선물 투자자별 순매수 현황 =====")
        print(df_formatted)
    else:
        print("데이터를 불러오지 못했습니다. 세션 상태를 확인하세요.")
except Exception as e:
    print(f"코스피200 지수선물 데이터 조회 중 오류 발생: {e}")

try:
    # 클래스 인스턴스 생성
    io_option = 옵션투자자별거래실적()

    # 1. 콜옵션(C) 조회
    df_call = io_option.fetch(start_date, end_date, option_type="C")

    # 2. 풋옵션(P) 조회[cite: 1]
    df_put = io_option.fetch(start_date, end_date, option_type="P")

    # 결과 정리를 위한 간단한 데이터프레임 병합
    # 외국인과 기관합계의 순매수대금만 추출하여 비교
    summary = pd.DataFrame({
        "콜옵션_순매수": df_call['순매수대금'],
        "풋옵션_순매수": df_put['순매수대금']
    })

    target_investors = ["기관합계", "기타법인", "개인", "외국인"]
    summary_filtered = summary.reindex(target_investors)

    # 출력 포맷팅
    formatted_summary = summary_filtered.map(
        lambda x: f"{x:,.0f}" if pd.notnull(x) else "0")

    print(f"\n===== 코스피200 옵션 투자자별 매매동향 ({start_date} ~ {end_date}) =====")
    print(formatted_summary)

    # 3. 간단한 시장 포지션 해석
    foreign_call = summary.loc["외국인", "콜옵션_순매수"]
    foreign_put = summary.loc["외국인", "풋옵션_순매수"]

    print("-" * 60)
    print(f"외국인 콜옵션: {foreign_call:,.0f}원 | 풋옵션: {foreign_put:,.0f}원")

    if foreign_call > 0 and foreign_put < 0:
        print("▶ 외국인 포지션: 상방 배팅 (콜 매수 / 풋 매도)")
    elif foreign_call < 0 and foreign_put > 0:
        print("▶ 외국인 포지션: 하방 배팅 (콜 매도 / 풋 매수)")
    else:
        print("▶ 외국인 포지션: 중립 또는 복합 전략 구사 중")

except Exception as e:
    print(f"옵션 데이터 조회 중 오류 발생: {e}")


try:
    # 1. 클래스 인스턴스 생성 및 데이터 조회
    bond_io = 국고채지표수익률()
    df_bond = bond_io.fetch(start_date, end_date)

    print(f"\n===== 7. 국고채 금리 및 장단기 금리차 변동 ({start_date} ~ {end_date}) =====")
    if not df_bond.empty:
        # 가독성을 위해 출력 포맷 설정
        print(df_bond.map(lambda x: f"{x:.3f}%"))

        # 주간 변동성 분석
        first_day = df_bond.iloc[0]
        last_day = df_bond.iloc[-1]

        diff_3y = last_day['국고채3년'] - first_day['국고채3년']
        diff_10y = last_day['국고채10년'] - first_day['국고채10년']
        diff_spread = last_day['장단기금리차'] - first_day['장단기금리차']

        print("-" * 65)
        # 3년물 해석 (통화정책 민감도)
        trend_3y = "상승(긴축 우려↑)" if diff_3y > 0 else "하락(금리 인하 기대↑)"
        print(
            f"▶ 국고채 3년: {last_day['국고채3년']:.3f}% ({diff_3y:+.3f}p) -> {trend_3y}")

        # 장단기 금리차 해석 (경기 전망)
        spread_status = "확대(경기 회복 신호)" if diff_spread > 0 else "축소(경기 둔화 우려)"
        print(
            f"▶ 장단기 금리차: {last_day['장단기금리차']:.3f}p ({diff_spread:+.3f}p) -> {spread_status}")
    else:
        print("지표수익률 데이터를 불러오지 못했습니다.")

except Exception as e:
    print(f"채권 지표 분석 중 오류 발생: {e}")
