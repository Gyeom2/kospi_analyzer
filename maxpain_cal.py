import pandas as pd
import numpy as np
import os
import re
from datetime import datetime, timedelta
from pandas import DataFrame

from pykrx import stock
from pykrx.website.krx.market.core import 업종분류현황
from pykrx.website.krx.krxio import KrxWebIo
import plotly.graph_objects as go

import streamlit as st

from collector import set_krx_auth, get_business_day, get_max_pain_analysis, get_predicted_next_day, get_futures_analysis


set_krx_auth()


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
            aggBasTpCd="0",     # "": 전체, "0": 정규, "2": 야간
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


start_date, end_date, prev_start_date, prev_end_date = get_business_day()
option_date = get_predicted_next_day(end_date)

futures_data = get_futures_analysis(start_date, end_date)
# max_pain_data = get_max_pain_analysis(option_date)
max_pain_data = get_max_pain_analysis("20260511")

print(f"start_date: {start_date}")
print(f"end_date: {end_date}")
print(f"option_date: {option_date}")

st.write("#### 🎯 옵션 맥스페인(Max Pain) 분석")

if max_pain_data:
    current_futures_price = futures_data['price_trend'].iloc[0]['종가'] if futures_data else 0

    # --- [A] 전체 데이터 분석 섹션 (기존 유지) ---
    with st.container():
        st.subheader("1️⃣ 시장 전체 맥스페인 (All Strikes)")
        # st.write(f"option_date: {option_date}")
        mp_all = max_pain_data['max_pain']
        pain_df_all = max_pain_data['pain_df']

        # 전체 차트 시각화
        fig_all = go.Figure()
        fig_all.add_trace(go.Scatter(
            x=pain_df_all['행사가'], y=pain_df_all['고통지수'],
            mode='lines+markers', name='Total Pain',
            line=dict(color='orange', width=2), fill='tozeroy',
            fillcolor='rgba(255, 165, 0, 0.15)'
        ))
        fig_all.add_vline(x=mp_all, line_dash="dash",
                          line_color="red", annotation_text=f"MP: {mp_all:.2f}")
        fig_all.add_vline(x=current_futures_price, line_dash="dot",
                          line_color="#5FBFF9", annotation_text="현재가")
        fig_all.update_layout(
            height=400, template="plotly_dark", title="시장 전체 에너지 분포")
        st.plotly_chart(fig_all, use_container_width=True)

    # --- [B] ±250 범위 한정 분석 섹션 (추가) ---

    with st.container():
        st.subheader(f"2️⃣ 실질적 맥스페인 (현재가 ±250 범위)")

        # 현재가 기준 ±200 범위 필터링 및 재계산 로직
        range_min = current_futures_price - 250
        range_max = current_futures_price + 250

        # 1. 해당 범위 내의 옵션만 추출
        calls_near = max_pain_data['calls'][(max_pain_data['calls']['행사가'] >= range_min) & (
            max_pain_data['calls']['행사가'] <= range_max)]
        puts_near = max_pain_data['puts'][(max_pain_data['puts']['행사가'] >= range_min) & (
            max_pain_data['puts']['행사가'] <= range_max)]

        # 2. 범위 내 행사가 기준 Pain 재계산 (Matplotlib 코드 로직 활용)
        near_strikes = sorted(
            list(set(calls_near['행사가']) | set(puts_near['행사가'])))

        def calculate_near_pain(target_price):
            p = 0
            for _, row in calls_near.iterrows():
                p += max(0, target_price - row['행사가']) * row['미결제약정']
            for _, row in puts_near.iterrows():
                p += max(0, row['행사가'] - target_price) * row['미결제약정']
            return p

        near_pain_results = [
            {"행사가": s, "고통지수": calculate_near_pain(s)} for s in near_strikes]
        near_pain_df = pd.DataFrame(near_pain_results)

        if not near_pain_df.empty:
            mp_near = near_pain_df.loc[near_pain_df['고통지수'].idxmin(
            ), '행사가']

            # 지표 표시
            c1, c2 = st.columns(2)
            c1.metric("범위 내 맥스페인", f"{mp_near:.2f}",
                      delta=f"{mp_near - mp_all:+.2f} (전체 대비)")
            c2.metric("현재 지수", f"{current_futures_price:.2f}")

            # 범위 한정 차트 시각화
            fig_near = go.Figure()
            fig_near.add_trace(go.Scatter(
                x=near_pain_df['행사가'], y=near_pain_df['고통지수'],
                mode='lines+markers', name='Near-Price Pain',
                line=dict(color='#00CC96', width=3),  # 차별화를 위해 녹색 계열 사용
                fill='tozeroy', fillcolor='rgba(0, 204, 150, 0.15)'
            ))
            fig_near.add_vline(x=mp_near, line_dash="dash", line_color="red",
                               annotation_text=f"Near MP: {mp_near:.2f}")
            fig_near.add_vline(x=current_futures_price, line_dash="dot",
                               line_color="#5FBFF9", annotation_text="현재가")
            fig_near.update_layout(height=400, template="plotly_dark",
                                   title=f"현재가({current_futures_price:.2f}) 중심 세부 분포")
            st.plotly_chart(fig_near, use_container_width=True)

            # 3. 상세 데이터 표 (요청하신 대로 순차적 배치)
            st.write("#### 📊 범위 내 주요 미결제약정 (OI) TOP 10")
            st.write("**✅ 범위 내 콜옵션 TOP 10 (상방 저항)**")
            st.table(calls_near.sort_values('미결제약정', ascending=False).head(10)[['행사가', '미결제약정', '종가']].style.format({
                "행사가": "{:.2f}", "미결제약정": "{:,.0f}", "종가": "{:.2f}"
            }))

            st.write("**✅ 범위 내 풋옵션 TOP 10 (하방 지지)**")
            st.table(puts_near.sort_values('미결제약정', ascending=False).head(10)[['행사가', '미결제약정', '종가']].style.format({
                "행사가": "{:.2f}", "미결제약정": "{:,.0f}", "종가": "{:.2f}"
            }))

    st.info("""
        **💡 비교 분석 팁:**
        - **전체 맥스페인**: 시장의 장기적/구조적 균형점을 보여줍니다.
        - **범위 내 맥스페인**: 현재 가격 근처에서 활동하는 트레이더들의 실질적인 이해관계를 보여줍니다. 
        - 두 수치가 비슷할수록 해당 지점의 회귀 본능이 강하며, 큰 차이가 날 경우 현재 구간에서 강한 변동성이 나타날 수 있습니다.
    """)
else:
    st.info("데이터를 불러올 수 없습니다.")
