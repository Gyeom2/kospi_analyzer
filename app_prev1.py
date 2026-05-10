import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from pykrx import stock
import plotly.express as px
import plotly.graph_objects as go

import collector


def format_date_str(date_str):
    """'YYYYMMDD' 문자열을 'YYYY년 MM월 DD일'로 변환"""
    try:
        dt = datetime.strptime(date_str, "%Y%m%d")  # 문자열을 datetime 객체로 변환
        return dt.strftime("%Y년 %m월 %d일")         # 원하는 형식의 문자열로 반환
    except:
        return date_str


# 날짜 설정 로직 (사용자가 선택할 수 있게 위젯으로 구성 가능)
target_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
start_dt = target_date - timedelta(days=target_date.weekday())
start_date = start_dt.strftime("%Y%m%d")
end_date = stock.get_nearest_business_day_in_a_week()

st.title("주간 자금 흐름 요약")

# 데이터 함수 호출
fund_data = collector.get_weekly_fund_flow(start_date, end_date)

# app.py의 출력부 수정
if fund_data:
    st.subheader("주간 자금 흐름 요약 (단위: 억 원)")
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
st.subheader(f"코스피 투자자별 보유 현황 ({display_date})")

holding_data = collector.get_market_holding_status(end_date)

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

# 1. 투자자별 상세 분석 데이터 호출
# collector.py에 해당 함수들이 정의되어 있다고 가정합니다.
analysis_data = collector.get_investor_analysis(start_date, end_date)

if analysis_data:
    st.subheader(f"투자 주체별 상세 분석 ({start_date} ~ {end_date})")

    # 외국인과 기관 탭 분리
    tab1, tab2 = st.tabs(["외국인 상세", "기관 상세"])

    for tab, investor_type in zip([tab1, tab2], ["외국인", "기관"]):
        with tab:
            data = analysis_data[investor_type]

            # --- 섹터별 순매수 현황 (사용자 요청 정렬 로직) ---
            st.write(f"### {investor_type} 섹터별 순매수 상/하위 5")

            # 1. 데이터 정렬 로직 수정
            # 순매수 상위: 거래대금 큰 순서대로 (양수 큰 -> 작은)
            top_s = data['top_sectors'].sort_values(
                by='순매수거래대금', ascending=False)

            # 순매도 하위: 거래대금 작은 순서대로 (음수 큰 -> 0에 가까운)
            # 순매도 1위(가장 많이 판 것)가 아래쪽에 오게 하려면 오름차순 정렬
            bot_s = data['bottom_sectors'].sort_values(
                by='순매수거래대금', ascending=False)

            # 두 데이터를 합침 (상위 5개가 위, 하위 5개가 아래)
            combined_sectors = pd.concat([top_s, bot_s])

            fig_sector = px.bar(
                combined_sectors,
                x='순매수거래대금',
                y='업종명',
                orientation='h',
                color='순매수거래대금',
                color_continuous_scale='RdBu_r',
                # 0을 기준으로 색상 대칭 설정
                range_color=[-max(abs(combined_sectors['순매수거래대금'])),
                             max(abs(combined_sectors['순매수거래대금']))],
                title=f"{investor_type} 주요 매매 섹터 (순매수 상위 5 / 순매도 상위 5)"
            )

            # 2. Y축 정렬: 데이터프레임 순서(위에서 아래로) 강제 적용
            # Plotly의 기본 'autorange'가 순서를 뒤집는 것을 방지하기 위해 'reversed' 설정 검토
            fig_sector.update_layout(
                yaxis={
                    'categoryorder': 'trace',  # 데이터프레임 순서 그대로
                    'autorange': 'reversed'   # 위에서부터 첫 번째 데이터가 오도록 설정
                },
                xaxis_title="순매수거래대금 (원)",
                yaxis_title="업종명",
                coloraxis_showscale=False
            )

            st.plotly_chart(fig_sector, use_container_width=True)

            # --- 종목별 상세 현황 (컬럼 분리) ---
            col_top, col_bottom = st.columns(2)

            with col_top:
                st.write(f"#### {investor_type} 순매수 상위 20")
                # 가독성을 위해 금액 단위 조정 (예: 억 원) 및 포맷팅
                df_top = data['top_20'].copy()
                df_top['순매수거래대금'] = (df_top['순매수거래대금'] / 1e8).round(1)
                st.dataframe(
                    df_top.rename(columns={'순매수거래대금': '순매수(억)'}),
                    hide_index=True,
                    use_container_width=True
                )

            with col_bottom:
                st.write(f"#### {investor_type} 순매도 상위 20")
                df_bottom = data['bottom_20'].copy()
                df_bottom['순매수거래대금'] = (df_bottom['순매수거래대금'] / 1e8).round(1)
                st.dataframe(
                    df_bottom.rename(columns={'순매수거래대금': '순매도(억)'}),
                    hide_index=True,
                    use_container_width=True
                )

else:
    st.error("투자자 상세 분석 데이터를 불러오는 데 실패했습니다.")
