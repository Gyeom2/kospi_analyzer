import os
import pandas as pd
from datetime import datetime
from workalendar.asia import SouthKorea

# bond_gather.py에서 이미 완벽히 검증된 정제 함수와 로그인 함수를 재사용합니다.
from bond_gather import get_bond_indicator_yields, set_krx_auth


def run_bond_daily_update(db_path="BondsData"):
    """
    매일 장 마감 후 당일의 채권 지표수익률을 수집하여
    BondsData/year=YYYY/ 구조의 Parquet 데이터베이스에 누적 업데이트합니다.
    """
    # 1. 오늘 날짜 정보 세팅 및 영업일 판별
    cal = SouthKorea()
    today_dt = datetime.now()
    today_str = today_dt.strftime('%Y%m%d')  # API 조회용 (YYYYMMDD)
    today_dash = today_dt.strftime('%Y-%m-%d')  # 중복 체크용 (YYYY-MM-DD)

    print(f"⏰ [{today_dash}] 채권 수익률 데일리 업데이트 프로세스를 가동합니다.")

    # GitHub Actions나 자동화 환경에서 주말/공휴일 패스
    if not cal.is_working_day(today_dt):
        print(f"🛑 오늘은 한국 거래소 휴장일(주말/공휴일)입니다. 작업을 종료합니다.")
        return

    # 2. KRX 환경변수 및 config 로그인 세션 바인딩
    set_krx_auth()

    # 3. 당일 채권 지표수익률 데이터 단일 수집 (시작일과 종료일을 오늘로 지정)
    print(f"⚡ KRX로부터 오늘자 채권 지표 데이터를 fetch합니다... (조회일: {today_str})")
    try:
        # 반환된 데이터프레임은 '일자'가 인덱스인 상태 (형식: YYYY-MM-DD)
        today_df = get_bond_indicator_yields(today_str, today_str)

        if today_df.empty:
            print(f"❌ 오늘자 수집된 채권 데이터가 존재하지 않습니다. (장 미마감 또는 API 지연)")
            return

        # 결측치나 가공 편의를 위해 인덱스를 일반 컬럼으로 해제
        today_df = today_df.reset_index()

    except Exception as e:
        print(f"❌ 채권 데이터 수집 단계에서 치명적 에러 발생: {e}")
        return

    # 4. Parquet 파티셔닝 정합성을 위한 연도 키('year') 추가
    # 주가 스크립트(daily_update.py)의 컬럼 레이아웃 매칭 규칙을 준수합니다.
    current_year = today_dt.year
    today_df['year'] = current_year

    # 5. 기존 구글 드라이브 동기화 데이터베이스 로드 및 인크리멘탈 머지(Merge)
    print(f"📂 기존 데이터베이스({db_path}) 상태를 점검하고 동기화 데이터 결합을 시도합니다.")

    if os.path.exists(db_path):
        try:
            # pyarrow가 상위 폴더를 읽어 하위 모든 year=YYYY 파티션을 단일 DF로 병합 로드합니다.
            existing_df = pd.read_parquet(db_path)

            # 중요 정합성 관리: 멱등성(Idempotency) 보장을 위해 오늘 날짜와 겹치는 데이터가 이미 있다면 제거
            if "일자" in existing_df.columns:
                existing_df = existing_df[existing_df['일자'] != today_dash]

            # 과거 역사적 데이터프레임 하단에 오늘 수집된 데이터 결합
            combined_df = pd.concat([existing_df, today_df], ignore_index=True)
            print(f"🔄 기존 데이터와 결합 완료. (당일 유효 행 1건 병합됨)")
        except Exception as e:
            print(f"⚠️ 기존 Parquet 파일 로드 중 에러 발생({e}). 신규 데이터셋으로 대치합니다.")
            combined_df = today_df
    else:
        print(f"ℹ️ {db_path} 저장소가 비어있습니다. 신규 베이스를 생성합니다.")
        combined_df = today_df

    # 6. 구글 드라이브(BondsData/) 폴더에 연도별 파티셔닝 최종 디렉토리 영속화
    try:
        combined_df.to_parquet(
            path=db_path,
            index=False,
            engine='pyarrow',
            compression='snappy',
            partition_cols=['year']  # 파일 시스템 레벨에서 year=YYYY 폴더로 쪼개어 저장
        )
        target_partition_path = os.path.join(db_path, f"year={current_year}")
        print(f"💾 [성공] 구글드라이브 동기화 폴더 적재 완료 -> {target_partition_path}")
        print(combined_df.tail(1))  # 최종 병합 상태 확인 로그

    except Exception as e:
        print(f"❌ Parquet 저장 중 파일 시스템 쓰기 오류 발생: {e}")


if __name__ == "__main__":
    # 데이터베이스 저장소 디렉토리명 지정
    run_bond_daily_update(db_path="BondsData")
