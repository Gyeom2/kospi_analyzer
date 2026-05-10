from playwright.sync_api import sync_playwright
import time


def capture_dashboard_big_viewport(url, output_path):
    with sync_playwright() as p:
        # 1. 브라우저 실행 (헤드리스 모드)
        browser = p.chromium.launch()

        # 2. 뷰포트를 아주 길게 설정 (데이터 양에 따라 9000~12000px 권장)
        # 이렇게 하면 스크롤 없이도 모든 섹션이 한 화면에 들어옵니다.
        context = browser.new_context(
            viewport={'width': 900, 'height': 11200},
            color_scheme='dark'
        )
        page = context.new_page()

        target_url = f"{url}?theme=dark"
        print(f"전체 로딩 시작: {target_url}")

        # 3. 접속 후 네트워크가 조용해질 때까지 대기
        page.goto(target_url, wait_until="networkidle")

        # 4. 데이터 로딩(Running...)이 완전히 끝날 때까지 대기
        print("모든 차트와 데이터를 불러오는 중입니다 (약 30~60초 소요)...")

        # 로딩 위젯이 사라질 때까지 끈질기게 대기 (최대 60초)
        try:
            page.wait_for_selector(
                '[data-testid="stStatusWidget"]', state="hidden", timeout=60000)
        except:
            pass

        # 마지막 섹션 텍스트가 나타날 때까지 한 번 더 확인[cite: 1]
        try:
            page.get_by_text("6. 🏛️ 국고채 금리").wait_for(
                state="visible", timeout=30000)
        except:
            print("마지막 섹션 확인 지연 중... 계속 진행합니다.")

        # 5. 차트 애니메이션이 멈추도록 최종 휴식
        time.sleep(5)

        # 6. 스크린샷 저장
        # 뷰포트 자체가 길기 때문에 full_page=False로 해도 전체가 담깁니다.
        page.screenshot(path=output_path, full_page=False)

        browser.close()
        print(f"완벽하게 저장되었습니다: {output_path}")


# 실행
capture_dashboard_big_viewport("http://localhost:8501", "kospi_full_view.png")
