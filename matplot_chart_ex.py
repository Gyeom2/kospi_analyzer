import pandas as pd
import re
import matplotlib.pyplot as plt

# 폰트 설정
plt.rcParams['font.family'] = 'NanumGothic'

file_path = 'data_3834_20260505.csv'

# 데이터 로드
try:
    df = pd.read_csv(file_path, encoding='cp949')
except:
    df = pd.read_csv(file_path, encoding='utf-8-sig')

# 4/30 데이터 필터링
col_date = [c for c in df.columns if '일자' in c][0]
df_30 = df[df[col_date].astype(str).str.contains('2026/04/30')].copy()

# 종목명에서 정보 추출
col_name = [c for c in df.columns if '종목명' in c][0]
col_oi = [c for c in df.columns if '미결제' in c][0]


def parse_option(name):
    strike_match = re.search(r'(\d+\.?\d*)$', name.strip())
    strike = float(strike_match.group(1)) if strike_match else None
    otype = 'Call' if ' C ' in name else ('Put' if ' P ' in name else 'Other')
    return otype, strike


df_30[['Type', 'Strike']] = df_30[col_name].apply(
    lambda x: pd.Series(parse_option(x)))

# 사용자 요청: 행사가 700 초과 데이터만 필터링
df_realistic = df_30[(df_30['Strike'] > 700) &
                     (df_30['Type'] != 'Other')].copy()

# 행사가별 OI 합계
oi_pivot = df_realistic.groupby(['Strike', 'Type'])[
    col_oi].sum().unstack().fillna(0)

# 맥스페인 계산 함수


def get_pain(oi_df, price):
    total_pain = 0
    for strike, row in oi_df.iterrows():
        c_pain = max(0, price - strike) * row.get('Call', 0)
        p_pain = max(0, strike - price) * row.get('Put', 0)
        total_pain += (c_pain + p_pain)
    return total_pain


# 필터링된 범위 내에서 Pain 계산
strikes = sorted(oi_pivot.index.tolist())
pain_results = [get_pain(oi_pivot, s) for s in strikes]

# 최소 페인 지점 찾기
max_pain_price = strikes[pain_results.index(min(pain_results))]

print(f"Filtered Strikes (above 700) Count: {len(strikes)}")
print(f"Realistic Max Pain Price: {max_pain_price}")

# 시각화
plt.figure(figsize=(12, 6))
plt.plot(strikes, pain_results, color='darkorange',
         marker='o', markersize=4, label='Total Pain (> 700)')
plt.axvline(x=max_pain_price, color='red', linestyle='--',
            label=f'Realistic Max Pain: {max_pain_price}')
plt.axvline(x=998.50, color='green', linestyle=':',
            label='Futures Close (4/30): 998.50')
plt.axvline(x=1050, color='purple', linestyle='-.',
            label='Current Index (5/4): ~1050')
plt.title('Realistic Max Pain Analysis (Strikes > 700 only)')
plt.xlabel('Strike Price')
plt.ylabel('Total Pain Value')
plt.legend()
plt.grid(True, alpha=0.3)
plt.savefig('realistic_max_pain.png')
plt.show()

# 데이터 요약
print("\n[OI Distribution for Strikes > 700]")
print(oi_pivot.tail(10))
