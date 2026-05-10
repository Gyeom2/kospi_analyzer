from datetime import datetime, timedelta
from pykrx import stock
import os


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


def get_business_day():
    target_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    # target_date = datetime.strptime("20251010", "%Y%m%d")
    # target_date = datetime.strptime("20260227", "%Y%m%d")
    # target_date = datetime.strptime("20260313", "%Y%m%d")
    start_dt = target_date - timedelta(days=target_date.weekday())
    # print(f"target_date.weekday(): {target_date.weekday()}")
    # print(f"start_dt: type:{type(start_dt)}/{start_dt}")
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

    next_start_dt = start_dt + timedelta(days=7)
    next_start_date = next_start_dt.strftime("%Y%m%d")
    b_next_start_date = stock.get_nearest_business_day_in_a_week(
        date=next_start_date)

    next_end_dt = end_dt + timedelta(days=7)
    next_end_date = next_end_dt.strftime("%Y%m%d")
    b_next_end_date = stock.get_nearest_business_day_in_a_week(
        date=next_end_date)

    return b_start_date, b_end_date, b_prev_start_date, b_prev_end_date, b_next_start_date, b_next_end_date


b_start_date, b_end_date, b_prev_start_date, b_prev_end_date, b_next_start_date, b_next_end_date = get_business_day()

print(f"b_start date: {b_start_date}")
print(f"b_end date: {b_end_date}")
print(f"b_prev_start_date: {b_prev_start_date}")
print(f"b_prev_end_date: {b_prev_end_date}")
print(f"b_next_start_date: {b_next_start_date}")
print(f"b_next_end_date: {b_next_end_date}")
