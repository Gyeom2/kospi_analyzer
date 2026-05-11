import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from pykrx import stock
import plotly.express as px
import plotly.graph_objects as go
import collector

# --- [기존 스타일 설정 및 날짜 로직 유지] ---
st.markdown("""
    <style>
    .stTable td, .stTable th {
        font-size: 12px !important;
        white-space: nowrap !important;
    }
    </style>
    """, unsafe_allow_html=True)


def apply_sector_colors(styler):
    # styler.data를 통해 원본 데이터프레임에 접근합니다.
    df = styler.data

    # 1. 섹터명 컬럼 확인 (데이터에 따라 '업종명' 또는 '섹터명'으로 통일)
    sector_col = '업종명' if '업종명' in df.columns else '섹터명'

    sectors = df[sector_col].unique()

    # 2. 섹터별 배경색 매핑 (흰색과 연한 회색 교차)
    sector_bg_map = {
        sector: 'background-color: #262730; color: white;' if i % 2 == 1
        else 'background-color: transparent; color: white;'
        for i, sector in enumerate(sectors)
    }

    # 3. 데이터프레임 전체에 행 단위로 적용
    return styler.apply(lambda row: [sector_bg_map.get(row[sector_col], '')] * len(row), axis=1)


def format_date_str(date_str):
    """'YYYYMMDD' 문자열을 'YYYY년 MM월 DD일'로 변환"""
    try:
        dt = datetime.strptime(date_str, "%Y%m%d")  # 문자열을 datetime 객체로 변환
        return dt.strftime("%Y년 %m월 %d일")         # 원하는 형식의 문자열로 반환
    except:
        return date_str


start_date, end_date, prev_start_date, prev_end_date = collector.get_business_day()
option_date = collector.get_predicted_next_day(end_date)

db_path = "G:\내 드라이브\StockData\kospi_db"

st.title("주간 코스피 분석")
st.header(
    f"{format_date_str(start_date)} ~ {format_date_str(end_date)}", divider=True)

# 데이터 사전 호출 (모든 탭에서 공유)
fund_data = collector.get_weekly_fund_flow(start_date, end_date)
holding_data = collector.get_market_holding_status(end_date)
analysis_data = collector.get_investor_analysis(start_date, end_date)
rotation_data = collector.get_sector_rotation(
    start_date, end_date, prev_start_date, prev_end_date)
delta_data = collector.get_intensity_delta(
    start_date, end_date, prev_start_date, prev_end_date)
continuity_data = collector.get_supply_continuity(end_date)
sell_data = collector.get_sell_continuity(end_date)
kospi_52w_data = collector.analyze_52w_high_low(db_path)
futures_data = collector.get_futures_analysis(start_date, end_date)
program_data = collector.get_program_trading_summary(start_date, end_date)
basis_df = collector.get_basis_analysis(start_date, end_date)
option_data = collector.get_options_analysis(start_date, end_date)
pcr_df = collector.get_pcr_analysis(start_date, end_date)
max_pain_data = collector.get_max_pain_analysis(option_date)
bond_data = collector.get_bond_analysis(start_date, end_date)

# --- 탭 구성 ---
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 시장 요약",
    "👽 외국인 상세",
    "🏢 기관 상세",
    "🪙 섹터/종목 수급",
    "📈 파생 및 매크로"
])

# ---------------------------------------------------------
# Tab 1: 시장 요약 (1. 자금 흐름 & 2. 보유 현황)
# ---------------------------------------------------------
with tab1:
    if fund_data:
        st.subheader("1. 주간 자금 흐름 요약 (단위: 억 원)")
        cols = st.columns(4)
        investors = [
            ("외국인", fund_data['foreign']),
            ("기관", fund_data['institution']),
            ("개인", fund_data['individual']),
            ("기타법인", fund_data['etc'])
        ]

        for col, (label, val) in zip(cols, investors):
            # 억 단위 변환 (가독성 확보)
            val_in_100m = val / 100_000_000

            col.metric(
                label=label,
                value=f"{val_in_100m:,.0f} 억",
                delta=f"{val_in_100m:,.0f} 억 ({'순유입' if val > 0 else '순유출'})",
                delta_color="normal"  # 양수 초록, 음수 빨강
            )

    # 유입/유출 판단 로직을 시각화
    if fund_data:
        # 데이터 준비 (단위: 억 원)
        labels = ["외국인", "기관", "개인", "기타"]
        values = [fund_data['foreign']/1e8, fund_data['institution']/1e8,
                  fund_data['individual']/1e8, fund_data['etc']/1e8]

        fig = go.Figure(go.Waterfall(
            name="주간 흐름",
            orientation="v",
            measure=["relative", "relative", "relative", "relative", "total"],
            x=labels + ["합계"],
            y=values + [0],  # total은 자동으로 계산됨
            text=[f"{v:,.0f}" for v in values] + ["Total"],
            decreasing={"marker": {"color": "Blue"}},  # 순유출 시 파랑
            increasing={"marker": {"color": "Red"}},  # 순유입 시 빨강
            totals={"marker": {"color": "Gray"}}
        ))

        fig.update_layout(
            title="투자자별 시장 영향력 합계 (단위: 억 원)",
            showlegend=False
        )
        st.plotly_chart(fig, use_container_width=True)

        st.divider()  # 구분선 추가

    display_date = format_date_str(end_date)
    st.subheader(f"2. 코스피 투자자별 보유 현황 ({display_date})")

    if holding_data:
        # 1. 주요 지표 출력 (단위: 조 원)
        total_trillion = holding_data['total_cap'] / 1e12
        foreign_trillion = holding_data['foreign_cap'] / 1e12

        col1, col2 = st.columns(2)
        with col1:
            st.metric("전체 시가총액", f"{total_trillion:,.1f} 조 원")
        with col2:
            st.metric("외국인 보유 금액", f"{foreign_trillion:,.1f} 조 원",
                      delta=f"{holding_data['foreign_share']:.2f}% (비중)")

        # 2. 시각화 (도넛 차트)
        labels = ['외국인 보유', '개인+기관+기타']
        values = [holding_data['foreign_cap'],
                  holding_data['total_cap'] - holding_data['foreign_cap']]

        fig_pie = go.Figure(data=[go.Pie(
            labels=labels,
            values=values,
            hole=.5,
            marker_colors=['#E74C3C', "#74EAC8"]  # 외국인 강조 색상
        )])

        fig_pie.update_layout(
            annotations=[dict(text='KOSPI', x=0.5, y=0.5,
                              font_size=20, showarrow=False)],
            margin=dict(t=0, b=0, l=0, r=0),
            height=300
        )

        st.plotly_chart(fig_pie, use_container_width=True)

        st.divider()

with tab2:
    if analysis_data:
        if rotation_data:
            st.subheader(f"3. 투자 주체별 상세 분석 ({start_date} ~ {end_date})")
            investor_type = "외국인"
            st.write(f"## 🏢 {investor_type} 상세 분석")

            data = analysis_data[investor_type]

            # --- 섹터별 순매수 현황 ---
            st.write(f"### 3-1. {investor_type} 섹터별 순매수 상/하위 5")

            top_s = data['top_sectors'].sort_values(
                by='순매수거래대금', ascending=False)
            bot_s = data['bottom_sectors'].sort_values(
                by='순매수거래대금', ascending=False)
            combined_sectors = pd.concat([top_s, bot_s])

            fig_sector = px.bar(
                combined_sectors,
                x='순매수거래대금',
                y='업종명',
                orientation='h',
                color='순매수거래대금',
                color_continuous_scale='RdBu_r',
                range_color=[-max(abs(combined_sectors['순매수거래대금'])),
                             max(abs(combined_sectors['순매수거래대금']))],
                title=f"{investor_type} 주요 매매 섹터"
            )

            fig_sector.update_layout(
                yaxis={'categoryorder': 'trace', 'autorange': 'reversed'},
                xaxis_title="순매수거래대금 (원)",
                yaxis_title="업종명",
                coloraxis_showscale=False,
                margin=dict(l=20, r=20, t=40, b=20),
                height=400  # 차트 높이 고정
            )

            st.plotly_chart(fig_sector, use_container_width=True)

            st.write(f"### 3-2. 🔄 {investor_type} 섹터 로테이션 (수급 유입 가속 섹터)")

            df_rot = rotation_data[investor_type]

            # 순위가 상승한 섹터 강조
            rising_sectors = df_rot[df_rot['순위변동'] > 0].sort_values(
                by='순위변동', ascending=False)

            if not rising_sectors.empty:
                cols = st.columns(len(rising_sectors))
                for i, (idx, row) in enumerate(rising_sectors.iterrows()):
                    cols[i].metric(
                        label=row['업종명'],
                        value=f"{row['현재순위']}위",
                        delta=f"{int(row['순위변동'])} 계단 상승",
                        delta_color="normal"
                    )
        else:
            st.info("지난주 대비 순위가 급상승한 섹터가 없습니다.")

        # 전체 로테이션 테이블
        st.write("#### 3-3. 📊 섹터별 수급 순위 변동 상세")
        display_rot = df_rot[['업종명', '순매수거래대금',
                              '이전순위', '현재순위', '순위변동']].copy()

        display_rot['이전순위'] = display_rot['이전순위'].astype(int)
        display_rot['현재순위'] = display_rot['현재순위'].astype(int)
        display_rot['순위변동'] = display_rot['순위변동'].astype(int)

        display_rot['순매수거래대금'] = display_rot['순매수거래대금'].apply(
            lambda x: f"{x/1e8:,.1f}억")

        st.table(display_rot)

        # --- 종목별 상세 현황 (스크롤 없이 전체 노출) ---
        col_top, col_bottom = st.columns(2)

        with col_top:
            st.write(f"#### 3-4. {investor_type} 순매수 상위 20")
            df_top = data['top_20'].copy()

            # 소수점 1자리 및 단위 추가 포맷팅
            df_top_display = df_top.rename(columns={'순매수거래대금': '순매수(억)'})

            # 데이터 포맷 변경: 억 단위는 천단위 구분기호 포함 소수점 1자리, 등락률은 % 포함 소수점 1자리
            df_top_display['순매수(억)'] = df_top_display['순매수(억)'].apply(
                lambda x: f"{(x/1e8):,.1f}")
            df_top_display['등락률'] = df_top_display['등락률'].apply(
                lambda x: f"{x:.1f}%")

            st.table(df_top_display)

        with col_bottom:
            st.write(f"#### {investor_type} 순매도 상위 20")
            df_bottom = data['bottom_20'].copy()

            # 동일하게 적용
            df_bottom_display = df_bottom.rename(
                columns={'순매수거래대금': '순매도(억)'})

            df_bottom_display['순매도(억)'] = df_bottom_display['순매도(억)'].apply(
                lambda x: f"{(x/1e8):,.1f}")
            df_bottom_display['등락률'] = df_bottom_display['등락률'].apply(
                lambda x: f"{x:.1f}%")

            st.table(df_bottom_display)

        # --- 순매수 강도 TOP 5 ---
        st.write(f"### 3-5. {investor_type} 순매수 강도 TOP 10")

        st.info(f"""
            **순매수 강도 분석:** 
            단순 매수 금액이 아닌 **시가총액 대비 매수 비중**을 확인합니다. 
            수치가 높을수록 {investor_type}이 해당 종목을 강력하게 집중 매집하고 있음을 의미합니다.
            *(산식: 순매수거래대금 / 시가총액 * 100)*
        """)
        df_int = data['top_intensity'].copy()
        df_int_display = df_int.rename(
            columns={'순매수거래대금': '순매수(억)', '순매수강도': '강도(%)'})
        df_int_display['순매수(억)'] = df_int_display['순매수(억)'].apply(
            lambda x: f"{(x/1e8):,.1f}")
        df_int_display['강도(%)'] = df_int_display['강도(%)'].apply(
            lambda x: f"{x:.3f}%")

        st.table(df_int_display)

        if delta_data:
            st.write(f"### 3-6. 🚀 {investor_type} 수급 가속 종목 (전주 대비 강도 증가)")
            st.caption("지난주 대비 시가총액 대비 매집 비중이 가장 크게 늘어난 종목들입니다.")

            inv_delta = delta_data[investor_type].head(5)

            # 메트릭 카드로 시각화
            m_cols = st.columns(5)
            for i, (idx, row) in enumerate(inv_delta.iterrows()):
                m_cols[i].metric(
                    label=row['종목명'],
                    value=f"{row['순매수강도']:.3f}%",
                    delta=f"{row['강도변화']:.3f}%p",
                    delta_color="normal"
                )

        # --- 역발상 매수 종목 ---
        st.write(f"### 3-7. {investor_type} 역발상 매수 종목")
        st.success(f"""
            **🔍 진흙 속의 진주 (Contrarian Selection):**
            {investor_type}이 해당 섹터 전체에 대해서는 '팔자' 기조를 유지하고 있지만, 
            그 와중에도 **예외적으로 매수세를 보이고 있는 종목**들입니다. 
            섹터의 하락 압력 속에서도 기관이 선택한 종목이 무엇인지 확인해 보세요.
        """)

        gems_dict = data['contrarian_gems']
        if gems_dict:
            for sector, df_gems in gems_dict.items():
                st.write(f"#### 📍 섹터: {sector} (해당 섹터 전체는 매도 중)")

                # 포맷팅 적용 (소수점 1자리 및 % 기호)
                display_gems = df_gems.copy()
                display_gems['순매수거래대금'] = display_gems['순매수거래대금'].apply(
                    lambda x: f"{(x/1e8):,.1f}")
                display_gems['등락률'] = display_gems['등락률'].apply(
                    lambda x: f"{x:.1f}%")

                st.table(display_gems.rename(columns={
                    '순매수거래대금': '순매수(억)',
                    '등락률': '등락(%)'
                }))
        else:
            st.write("현재 역발상 매수 패턴이 포착된 종목이 없습니다.")

# ---------------------------------------------------------
# Tab 3: 기관 상세 분석 (3번의 기관 파트)
# ---------------------------------------------------------
with tab3:
    if analysis_data:
        if rotation_data:
            st.subheader(f"3. 투자 주체별 상세 분석 ({start_date} ~ {end_date})")
            investor_type = "기관"
            st.write(f"## 🏢 {investor_type} 상세 분석")

            data = analysis_data[investor_type]

            # --- 섹터별 순매수 현황 ---
            st.write(f"### 3-1. {investor_type} 섹터별 순매수 상/하위 5")

            top_s = data['top_sectors'].sort_values(
                by='순매수거래대금', ascending=False)
            bot_s = data['bottom_sectors'].sort_values(
                by='순매수거래대금', ascending=False)
            combined_sectors = pd.concat([top_s, bot_s])

            fig_sector = px.bar(
                combined_sectors,
                x='순매수거래대금',
                y='업종명',
                orientation='h',
                color='순매수거래대금',
                color_continuous_scale='RdBu_r',
                range_color=[-max(abs(combined_sectors['순매수거래대금'])),
                             max(abs(combined_sectors['순매수거래대금']))],
                title=f"{investor_type} 주요 매매 섹터"
            )

            fig_sector.update_layout(
                yaxis={'categoryorder': 'trace', 'autorange': 'reversed'},
                xaxis_title="순매수거래대금 (원)",
                yaxis_title="업종명",
                coloraxis_showscale=False,
                margin=dict(l=20, r=20, t=40, b=20),
                height=400  # 차트 높이 고정
            )

            st.plotly_chart(fig_sector, use_container_width=True)

            st.write(f"### 3-2. 🔄 {investor_type} 섹터 로테이션 (수급 유입 가속 섹터)")

            df_rot = rotation_data[investor_type]

            # 순위가 상승한 섹터 강조
            rising_sectors = df_rot[df_rot['순위변동'] > 0].sort_values(
                by='순위변동', ascending=False)

            if not rising_sectors.empty:
                cols = st.columns(len(rising_sectors))
                for i, (idx, row) in enumerate(rising_sectors.iterrows()):
                    cols[i].metric(
                        label=row['업종명'],
                        value=f"{row['현재순위']}위",
                        delta=f"{int(row['순위변동'])} 계단 상승",
                        delta_color="normal"
                    )
            else:
                st.info("지난주 대비 순위가 급상승한 섹터가 없습니다.")

            # 전체 로테이션 테이블
            st.write("#### 3-3. 📊 섹터별 수급 순위 변동 상세")
            display_rot = df_rot[['업종명', '순매수거래대금',
                                  '이전순위', '현재순위', '순위변동']].copy()

            display_rot['이전순위'] = display_rot['이전순위'].astype(int)
            display_rot['현재순위'] = display_rot['현재순위'].astype(int)
            display_rot['순위변동'] = display_rot['순위변동'].astype(int)

            display_rot['순매수거래대금'] = display_rot['순매수거래대금'].apply(
                lambda x: f"{x/1e8:,.1f}억")

            st.table(display_rot)

            # --- 종목별 상세 현황 (스크롤 없이 전체 노출) ---
            col_top, col_bottom = st.columns(2)

            with col_top:
                st.write(f"#### 3-4. {investor_type} 순매수 상위 20")
                df_top = data['top_20'].copy()

                # 소수점 1자리 및 단위 추가 포맷팅
                df_top_display = df_top.rename(columns={'순매수거래대금': '순매수(억)'})

                # 데이터 포맷 변경: 억 단위는 천단위 구분기호 포함 소수점 1자리, 등락률은 % 포함 소수점 1자리
                df_top_display['순매수(억)'] = df_top_display['순매수(억)'].apply(
                    lambda x: f"{(x/1e8):,.1f}")
                df_top_display['등락률'] = df_top_display['등락률'].apply(
                    lambda x: f"{x:.1f}%")

                st.table(df_top_display)

            with col_bottom:
                st.write(f"#### {investor_type} 순매도 상위 20")
                df_bottom = data['bottom_20'].copy()

                # 동일하게 적용
                df_bottom_display = df_bottom.rename(
                    columns={'순매수거래대금': '순매도(억)'})

                df_bottom_display['순매도(억)'] = df_bottom_display['순매도(억)'].apply(
                    lambda x: f"{(x/1e8):,.1f}")
                df_bottom_display['등락률'] = df_bottom_display['등락률'].apply(
                    lambda x: f"{x:.1f}%")

                st.table(df_bottom_display)

            # --- 순매수 강도 TOP 5 ---
            st.write(f"### 3-5. {investor_type} 순매수 강도 TOP 10")

            st.info(f"""
                **순매수 강도 분석:** 
                단순 매수 금액이 아닌 **시가총액 대비 매수 비중**을 확인합니다. 
                수치가 높을수록 {investor_type}이 해당 종목을 강력하게 집중 매집하고 있음을 의미합니다.
                *(산식: 순매수거래대금 / 시가총액 * 100)*
            """)
            df_int = data['top_intensity'].copy()
            df_int_display = df_int.rename(
                columns={'순매수거래대금': '순매수(억)', '순매수강도': '강도(%)'})
            df_int_display['순매수(억)'] = df_int_display['순매수(억)'].apply(
                lambda x: f"{(x/1e8):,.1f}")
            df_int_display['강도(%)'] = df_int_display['강도(%)'].apply(
                lambda x: f"{x:.3f}%")

            st.table(df_int_display)

        if delta_data:
            st.write(f"### 3-6. 🚀 {investor_type} 수급 가속 종목 (전주 대비 강도 증가)")
            st.caption("지난주 대비 시가총액 대비 매집 비중이 가장 크게 늘어난 종목들입니다.")

            inv_delta = delta_data[investor_type].head(5)

            # 메트릭 카드로 시각화
            m_cols = st.columns(5)
            for i, (idx, row) in enumerate(inv_delta.iterrows()):
                m_cols[i].metric(
                    label=row['종목명'],
                    value=f"{row['순매수강도']:.3f}%",
                    delta=f"{row['강도변화']:.3f}%p",
                    delta_color="normal"
                )

        # --- 역발상 매수 종목 ---
        st.write(f"### 3-7. {investor_type} 역발상 매수 종목")
        st.success(f"""
            **🔍 진흙 속의 진주 (Contrarian Selection):**
            {investor_type}이 해당 섹터 전체에 대해서는 '팔자' 기조를 유지하고 있지만, 
            그 와중에도 **예외적으로 매수세를 보이고 있는 종목**들입니다. 
            섹터의 하락 압력 속에서도 기관이 선택한 종목이 무엇인지 확인해 보세요.
        """)

        gems_dict = data['contrarian_gems']
        if gems_dict:
            for sector, df_gems in gems_dict.items():
                st.write(f"#### 📍 섹터: {sector} (해당 섹터 전체는 매도 중)")

                # 포맷팅 적용 (소수점 1자리 및 % 기호)
                display_gems = df_gems.copy()
                display_gems['순매수거래대금'] = display_gems['순매수거래대금'].apply(
                    lambda x: f"{(x/1e8):,.1f}")
                display_gems['등락률'] = display_gems['등락률'].apply(
                    lambda x: f"{x:.1f}%")

                st.table(display_gems.rename(columns={
                    '순매수거래대금': '순매수(억)',
                    '등락률': '등락(%)'
                }))
        else:
            st.write("현재 역발상 매수 패턴이 포착된 종목이 없습니다.")

# ---------------------------------------------------------
# Tab 4: 섹터/종목 수급 분석
# ---------------------------------------------------------
with tab4:
    st.subheader(f"4. 섹터/종목 수급 분석 ({start_date} ~ {end_date})")
    st.write(f"### 4-1. 🏆 수급 연속성 점수판 (연속 순매수 TOP10)")

    continuity_data = collector.get_supply_continuity(end_date)
    st.markdown("""
    > **💡 수급 점수 활용 가이드**
    > * **🔥 집중매집 (5일 이상)**: 기관/외국인이 하락장에서도 물량을 모으거나 상승 추세를 강력히 지지하는 종목입니다.
    > * **👀 주목 (3일 이상)**: 단기 테마 형성 혹은 수급 전환의 초기 신호일 수 있습니다.
    > * **필터링**: 최근 10일간 총 순매수액 **1,000억 원 이상**인 대형 수급 종목만 표시합니다.
    """)

    if continuity_data:
        for i, inv in enumerate(["외국인", "기관"]):
            st.write(f"#### 🏅 {inv} 순매수 일수 TOP 10")

            df_display = continuity_data[inv].head(10).copy()

            # 출력 포맷팅
            df_display['총순매수액'] = df_display['총순매수액'].apply(
                lambda x: f"{x/1e8:,.1f}억")

            # 컬럼명 정리 및 인덱스 제거
            df_display = df_display.reset_index(drop=True)

            st.table(df_display.style.applymap(
                lambda val: 'color: #ff4b4b; font-weight: bold' if "🔥" in str(
                    val) else '',
                subset=['상태']
            ))

    else:
        st.info("수급 연속성 데이터를 계산 중이거나 불러올 수 없습니다.")

    st.write(f"### 4-2. 📉 수급 리스크 점수판 (연속 순매도 TOP 10)")
    st.markdown("""
    > **🚨 리스크 관리 가이드**
    > * **🚨 지속 투매 (8일 이상)**: 세력의 이탈이 거의 완료 단계이거나, 강력한 하락 압력이 지속되는 종목입니다.
    > * **⚠️ 주의 요망 (5일 이상)**: 수급이 꼬이기 시작한 종목으로, 지지선 이탈 여부를 반드시 확인해야 합니다.
    """)

    sell_data = collector.get_sell_continuity(end_date)

    if sell_data:
        for i, inv in enumerate(["외국인", "기관"]):
            st.write(f"#### 💀 {inv} 순매도 일수 TOP 10")
            df_display = sell_data[inv].head(10).copy()
            df_display['총순매도액'] = df_display['총순매도액'].apply(
                lambda x: f"{abs(x)/1e8:,.1f}억")

            st.table(df_display.style.applymap(
                lambda val: 'color: #ff4b4b; font-weight: bold' if "🚨" in str(
                    val) else '',
                subset=['상태']
            ))

    else:
        st.error("투자자 상세 분석 데이터를 불러오는 데 실패했습니다.")

    st.write(f"### 4-3. 🚀 코스피 52주 신고가/신저가 분석기")
    if kospi_52w_data:
        col1, col2 = st.columns(2)
        col1.metric("🚀 52주 신고가", f"{kospi_52w_data['high_count']}개")
        col2.metric("❄️ 52주 신저가", f"{kospi_52w_data['low_count']}개")

        # 1. [상승 섹터 분석] (기존 코드 유지)
        if kospi_52w_data['sector_rank'] is not None:
            st.subheader("🔥 주도 섹터 (신고가 빈도 순위)")
            sector_df = kospi_52w_data['sector_rank'].reset_index()
            sector_df.columns = ['섹터명', '신고가 종목 수']
            top_5_sectors = sector_df.head(5)

            st.table(top_5_sectors)

        # 상세 종목 표
        st.subheader("🚀 52주 신고가 경신 상세 종목(섹터별 TOP 3)")
        high_df = kospi_52w_data['high_df']

        if not high_df.empty:
            filtered_list = []
            for sector in top_5_sectors['섹터명']:
                sector_top_3 = high_df[high_df['업종명'] == sector].head(3)
                filtered_list.append(sector_top_3)

            display_df = pd.concat(filtered_list).reset_index(drop=True)

            styler = display_df.style.format({'종가': '{:,.0f}'})
            styled_df = apply_sector_colors(styler)
            st.table(styled_df)
        else:
            st.write("신고가 경신 종목이 없습니다.")

        # 2. [하락 섹터 및 신저가 분석] (새로 추가할 부분)
        low_df = kospi_52w_data['low_df']
        if not low_df.empty and '업종명' in low_df.columns:
            st.subheader("❄️ 하락 섹터 (신저가 빈도 순위)")
            # 신저가 빈도 계산
            low_sector_rank = low_df['업종명'].value_counts().reset_index()
            low_sector_rank.columns = ['섹터명', '신저가 종목 수']
            top_5_low_sectors = low_sector_rank.head(5)

            st.table(top_5_low_sectors)

            st.markdown("#### ❄️ 신저가 경신 상세 종목 (섹터별 TOP 3)")

            low_filtered = []
            for sector in top_5_low_sectors['섹터명']:
                s_df = low_df[low_df['업종명'] == sector].head(3)
                low_filtered.append(s_df)

            if low_filtered:
                display_low = pd.concat(low_filtered).reset_index(drop=True)
                # 스타일 적용 (다크모드용 배경색 교차 적용)
                st.table(apply_sector_colors(
                    display_low.style.format({'종가': '{:,.0f}'})))
        else:
            st.subheader("📉 소외 섹터")
            st.info("신저가 경신 종목이 없어 분석할 데이터가 없습니다.")

# ---------------------------------------------------------
# Tab 5: 파생상품 및 지표 (5. 선물/프로그램, 6. 옵션/PCR/MaxPain, 7. 채권)
# ---------------------------------------------------------
with tab5:
    # 5. 코스피200 지수선물 및 프로그램 분석
    st.subheader("5. 📊 코스피200 지수선물 및 프로그램 분석")
    if futures_data:
        # 1. 상단 시장 진단 메트릭 (이미지 1번의 상단 부분 유지)
        res = futures_data['oi_analysis']
        st.info(f"🎯 **시장 진단: {res['phase']}** ({res['desc']})")

        # 2. 중간 시각화 영역 (이미지 1번의 차트 레이아웃 유지)
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("#### ◆ 투자자별 선물 순매수 현황")
            df_inv = futures_data['investor_trend'].reindex(
                ["외국인", "개인", "기관합계", "기타법인"]).fillna(0)
            fig_inv = px.bar(
                df_inv.reset_index(),
                x='투자자', y='순매수대금',
                color='순매수대금', color_continuous_scale='RdBu_r',
                labels={'순매수대금': '순매수대금'}
            )
            fig_inv.update_layout(coloraxis_showscale=False, height=400)
            st.plotly_chart(fig_inv, use_container_width=True)

        with col2:
            st.markdown("#### 📈 최근월물 지수 및 베이시스 추이")

            df_prc = futures_data['price_trend'].copy()

            if basis_df is not None and not basis_df.empty:
                # Plotly를 이용한 이중 축 차트 생성
                fig_combined = go.Figure()

                # 1. 선물 지수 라인 (좌측 Y축)
                fig_combined.add_trace(go.Scatter(
                    x=basis_df.index, y=basis_df['선물종가'],
                    name="선물종가", line=dict(color="#5FBFF9", width=3),
                    yaxis="y1"
                ))

                # 2. 시장 베이시스 막대 (우측 Y축)
                # 양수/음수에 따라 색상 구분
                colors = ['#FF4B4B' if val >=
                          0 else '#4B4BFF' for val in basis_df['시장베이시스']]

                fig_combined.add_trace(go.Bar(
                    x=basis_df.index, y=basis_df['시장베이시스'],
                    name="시장베이시스", marker_color=colors, opacity=0.4,
                    yaxis="y2"
                ))

                fig_combined.update_layout(
                    height=400,
                    margin=dict(l=0, r=0, t=30, b=0),
                    yaxis=dict(title="선물 지수", side="left"),
                    yaxis2=dict(title="베이시스", side="right",
                                overlaying="y", showgrid=False),
                    legend=dict(orientation="h", yanchor="bottom",
                                y=1.02, xanchor="right", x=1),
                    hovermode="x unified"
                )
                st.plotly_chart(fig_combined, use_container_width=True)
    else:
        st.info("베이시스 데이터를 불러올 수 없습니다.")

    # 3. 하단 데이터 테이블 영역
    t_col1, t_col2 = st.columns([2, 3])

    with t_col1:
        # 1. 순매수대금을 '억원' 단위로 변환하여 표 너비 축소
        df_inv_display = df_inv.copy()
        df_inv_display['순매수대금(억)'] = df_inv_display['순매수대금'] / 1e8

        # 필요한 컬럼만 선택 (계약수와 억 단위 대금)
        df_inv_display = df_inv_display[['순매수계약수', '순매수대금(억)']]

        st.dataframe(
            df_inv_display.style.format({
                "순매수계약수": "{:,.0f}",
                "순매수대금(억)": "{:,.1f}"
            }),
            use_container_width=True
        )

    with t_col2:
        # 2. 기존 지수 데이터(df_prc)와 베이시스 데이터(basis_df) 결합
        if basis_df is not None and not basis_df.empty:
            # 일자를 기준으로 두 데이터프레임 병합
            df_prc_copy = df_prc.copy()
            df_prc_copy['join_key'] = df_prc_copy.index.str.split(' ').str[0]

            # basis_df의 인덱스도 동일한 형식(YYYY/MM/DD)인지 확인 후 병합
            df_combined_prc = pd.merge(
                df_prc_copy,
                basis_df[['시장베이시스']],
                left_on='join_key',
                right_index=True,
                how='left'
            )

            # 병합용 임시 컬럼 삭제 및 순서 재배치
            df_combined_prc = df_combined_prc[['종가', '시장베이시스', '거래량', '미결제약정']]

            st.dataframe(
                df_combined_prc.sort_index(ascending=False).style.format({
                    "종가": "{:.2f}",
                    "시장베이시스": "{:+.2f}",  # 부호 포함 출력
                    "거래량": "{:,.0f}",
                    "미결제약정": "{:,.0f}"
                }),
                use_container_width=True
            )
        else:
            # 베이시스 데이터가 없을 경우 기존 표 유지
            st.dataframe(
                df_prc.sort_index(ascending=False).style.format({
                    "종가": "{:.2f}",
                    "거래량": "{:,.0f}",
                    "미결제약정": "{:,.0f}"
                }),
                use_container_width=True
            )

    # ---------------------------------------------------------
    # 5. 프로그램 매매 동향 섹션 추가 (기존 구조 하단에 자연스럽게 배치)
    # ---------------------------------------------------------
    if program_data:
        st.markdown("#### 🤖 프로그램 매매 수급 동향 (현물)")

        # 1. 프로그램 수급 메트릭 및 설명 배치
        p1, p2, p3 = st.columns(3)

        with p1:
            st.metric("비차익 순매수", f"{program_data['non_arbitrage']/1e8:,.1f} 억")
            st.caption("💡 **바스켓 매매**: 시장 전체의 매수/매도 의지를 나타냅니다.")

        with p2:
            st.metric("차익 순매수", f"{program_data['arbitrage']/1e8:,.1f} 억")
            st.caption("💡 **베이시스 연동**: 선물과 현물의 가격 차이를 이용한 기계적 매매입니다.")

        with p3:
            st.metric("프로그램 합계", f"{program_data['total']/1e8:,.1f} 억")
            st.caption("💡 **전체 수급 합계**: 주간 프로그램 매매의 총 순유입 규모입니다.")

        # 2. 상세 내역 바로 노출 (Expander 제거)
        st.markdown("##### 📝 프로그램 매매 상세 내역")

        # 가독성을 위해 숫자에 콤마(,) 추가 및 스타일 적용
        st.dataframe(
            program_data['raw_df'].style.format("{:,.0f}"),
            use_container_width=True
        )

        # 하단 유의사항
        st.caption("※ 단위: 원 / 비차익 매매의 연속적 유입은 외국인의 강력한 현물 매수 의지로 해석됩니다.")

    else:
        st.warning("선물 분석 데이터를 불러올 수 없습니다.")

    st.divider()
    # 5. 옵션 및 Max Pain 분석
    st.subheader("6. 🎭 옵션 및 포지션 (Max Pain)")
    if option_data:
        # 1. 외국인 포지션 요약 안내
        pos = option_data['foreign_pos']
        st.info(f"**외국인 시장 전망:** {pos['text']}")

        col1, col2 = st.columns([1.2, 1])

        with col1:
            st.write("#### 🔹 투자자별 시장 방향성 (상방 vs 하방)")

            # 1. 데이터 가져오기 (이미 collector에서 처리된 상태)
            df_opt_vis = option_data['summary'].reset_index()

            # 2. 안전하게 컬럼명 확인 및 매칭
            # 현재 컬럼: ['index'(또는 '투자자'), '콜옵션_순매수', '풋옵션_순매수', '풋옵션_반전']
            # 시각화에 필요한 컬럼만 이름을 변경하여 사용
            df_opt_vis = df_opt_vis.rename(columns={
                df_opt_vis.columns[0]: '투자자',
                '콜옵션_순매수': '콜옵션 배팅',
                '풋옵션_반전': '풋옵션 배팅'
            })

            # 3. 누적 막대 차트 생성
            fig_opt = px.bar(
                df_opt_vis,
                x='투자자',
                # 콜옵션 매수(+)와 풋옵션 매도 반전값(-)만 사용하여 방향성 시각화
                y=['콜옵션 배팅', '풋옵션 배팅'],
                title="투자자별 옵션 포지션 (0 위로 길면 상방 배팅)",
                color_discrete_map={
                    "콜옵션 배팅": "#E74C3C",    # 빨강 (매수 시 상방)
                    "풋옵션 배팅": "#3498DB"    # 파랑 (매수 시 하방이나 반전시켜 하단 표시)
                },
                labels={"value": "순매수대금 배팅 규모", "variable": "포지션"}
            )

            # 4. 레이아웃 설정
            fig_opt.update_layout(
                barmode='relative',  # 양수와 음수를 합산하여 실제 방향 표시
                xaxis={'categoryorder': 'trace'},
                showlegend=True,
                legend_title="옵션 배팅"
            )

            # 0 기준선 추가
            fig_opt.add_hline(y=0, line_dash="dash",
                              line_color="white", opacity=0.5)

            st.plotly_chart(fig_opt, use_container_width=True)

        with col2:
            st.write("#### 📋 상세 데이터")
            # summary 데이터프레임에서 '풋옵션_반전'이 있다면 제거하고 출력합니다.
            display_df = option_data['summary'].drop(
                columns=['풋옵션_반전'], errors='ignore')
            st.dataframe(
                display_df.style.format("{:,.0f}"),
                use_container_width=True
            )

            st.write(f"**외국인 수치 상세**")
            st.write(f"- 콜옵션: `{pos['call']:,.0f}`원")
            st.write(f"- 풋옵션: `{pos['put']:,.0f}`원")

    else:
        st.error("옵션 데이터를 불러오는 데 실패했습니다.")

    if pcr_df is not None and not pcr_df.empty:
        st.markdown("#### ⚖️ 옵션 풋콜레이쇼(PCR) 추이 및 상세 내역")

        # 1. 상단 요약 메트릭
        latest_pcr = pcr_df.iloc[-1]
        avg_pcr = pcr_df['풋콜레이쇼'].mean()

        m1, m2, m3 = st.columns(3)
        with m1:
            st.metric("최근 PCR", f"{latest_pcr['풋콜레이쇼']:.2f}")
        with m2:
            st.metric("기간 평균 PCR", f"{avg_pcr:.2f}")
        with m3:
            status = "보수적(하락베팅)" if latest_pcr['풋콜레이쇼'] > 1 else "낙관적(상승베팅)"
            st.info(f"**현재 시장 심리:** {status}")

        # 2. 시각화와 상세 표

        # 풋콜레이쇼 시각화
        fig_pcr = px.area(
            pcr_df.reset_index(),
            x='일자', y='풋콜레이쇼',
            title="거래량 기준 풋콜레이쇼 추이 (PCR > 1: 하락 경계)",
            line_shape='spline',
            color_discrete_sequence=['#AB63FA']
        )
        fig_pcr.add_hline(y=1.0, line_dash="dash",
                          line_color="white", annotation_text="PCR 1.0 (균형)")
        fig_pcr.update_layout(height=350, margin=dict(l=0, r=0, t=30, b=0))
        st.plotly_chart(fig_pcr, use_container_width=True)

        # 상세 데이터 표 (Expander 없이 즉시 표출)
        st.markdown("##### 📝 일별 PCR 상세")
        st.dataframe(
            pcr_df.sort_index(ascending=False).style.format({
                "풋옵션거래량": "{:,.0f}",
                "콜옵션거래량": "{:,.0f}",
                "풋콜레이쇼": "{:.2f}"
            }),
            use_container_width=True
        )

        # 하단 가이드 문구
        st.caption(
            "※ **풋콜레이쇼(PCR)**: 풋옵션 거래량을 콜옵션 거래량으로 나눈 값입니다. 수치가 높을수록 하락에 대비하는 수급이 강함을 의미하며, 극단적으로 높을 경우 과도한 공포에 따른 반등 신호로 해석하기도 합니다.")

    else:
        st.info("풋콜레이쇼 데이터를 불러올 수 없습니다.")

    st.write("#### 🎯 옵션 맥스페인(Max Pain) 분석")
    # collector에서 전체 범위의 데이터를 가져옵니다.
    # max_pain_data = collector.get_max_pain_analysis(end_date)

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

    st.divider()

    # 6. 국고채 및 금리차
    st.subheader("7. 🏛️ 국고채 금리 및 장단기 금리차")
    if bond_data:
        df_bond = bond_data['df']
        diff = bond_data['diff']
        latest = bond_data['latest']

        # 1. 주요 지표 요약 (Metrics) - 상단에 배치
        m1, m2, m3 = st.columns(3)
        with m1:
            st.metric("국고채 3년", f"{latest['국고채3년']:.3f}%",
                      delta=f"{diff['3y']:+.3f}p")
        with m2:
            st.metric("국고채 10년", f"{latest['국고채10년']:.3f}%",
                      delta=f"{diff['10y']:+.3f}p")
        with m3:
            st.metric("장단기 금리차", f"{latest['장단기금리차']:.3f}p",
                      delta=f"{diff['spread']:+.3f}p")

        # 2. 금리 변동 추이 차트 - 가로 전체 너비 사용
        st.write("#### 📈 국고채 금리 추이")
        df_plot = df_bond.reset_index()

        fig_bond = px.line(
            df_plot,
            x=df_plot.columns[0],
            y=['국고채3년', '국고채10년'],
            markers=True,
            title="국고채 수익률 추이 (3년 vs 10년)",
            color_discrete_map={"국고채3년": "#FFCC00", "국고채10년": "#FF5733"}
        )

        # 차트 레이아웃 최적화
        fig_bond.update_layout(
            xaxis_title="일자",
            yaxis_title="수익률(%)",
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom",
                        y=1.02, xanchor="right", x=1)
        )
        st.plotly_chart(fig_bond, use_container_width=True)

        # 3. 상세 데이터 표
        st.write("#### 📋 상세 데이터")
        # 최신 데이터가 위로 오도록 역순 정렬
        df_bond_display = df_bond.sort_index(ascending=False)
        # 소수점 3자리와 % 기호 포맷팅
        formatted_df = df_bond_display.map(lambda x: f"{x:.3f}%")
        # 표 출력
        st.dataframe(formatted_df, use_container_width=True)

    else:
        st.info("채권 지표 데이터를 불러올 수 없습니다.")
