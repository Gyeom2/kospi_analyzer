import os
from datetime import datetime, timedelta
import re
import pandas as pd
import numpy as np
from pandas import DataFrame

from pykrx import stock
from pykrx.website.krx.krxio import KrxWebIo

from workalendar.asia import SouthKorea


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
        # def parse_isu_nm(nm):
        #     # C/P 구분 추출
        #     right = '콜' if ' C ' in nm else '풋' if ' P ' in nm else '기타'

        #     # 행사가 추출 (문장 끝의 숫자와 소수점 찾기)
        #     match = re.search(r'(\d+\.\d+)$', nm.strip())
        #     strike_price = float(match.group(1)) if match else 0.0

        #     return pd.Series([right, strike_price])

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


def get_predicted_next_day(date_str):
    cal = SouthKorea()
    curr_date = datetime.strptime(date_str, "%Y%m%d").date()

    # 다음 날부터 시작해서 영업일이 나올 때까지 루프
    next_day = cal.add_working_days(curr_date, 1)

    return next_day.strftime("%Y%m%d")


option_date = get_predicted_next_day(b_end_date)


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
        # 940라인 부근의 로직을 참고하여 미결제약정 가중치를 적용합니다.
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
        print(f"맥스페인 계산 중 오류 발생: {e}")
        return None


result = get_max_pain_analysis(option_date)
now = datetime.now().strftime("%Y%m%d_%H%M%S")


def save_max_pain_to_excel(result, filename=f"max_pain_analysis_{now}.xlsx"):
    """
    get_max_pain_analysis 결과값을 엑셀 파일로 저장합니다.
    """
    if result is None:
        print("저장할 데이터가 없습니다.")
        return

    # ExcelWriter 생성
    with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
        # 1. 요약 정보 시트 (Max Pain 가격)
        summary_df = pd.DataFrame({
            '항목': ['Max Pain Price', '분석 기준일'],
            '값': [result['max_pain'], option_date]
        })
        summary_df.to_excel(writer, sheet_name='요약', index=False)

        # 2. 고통 지수 추이 시트
        result['pain_df'].to_excel(writer, sheet_name='고통지수_추이', index=False)

        # 3. 콜옵션 상세 데이터
        result['calls'].to_excel(writer, sheet_name='Call_데이터', index=False)

        # 4. 풋옵션 상세 데이터
        result['puts'].to_excel(writer, sheet_name='Put_데이터', index=False)

    print(f"엑셀 파일이 성공적으로 생성되었습니다: {filename}")


# 결과 실행 및 저장
if result:
    save_max_pain_to_excel(result)

print(f"option_date: {option_date}")
print(f"결과: {result}")
