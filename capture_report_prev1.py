import os
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager


def save_streamlit_to_custom_path(url):
    today_str = datetime.now().strftime("%Y-%m-%d-%H%M%S")
    file_name = f"KOSPI_Report_{today_str}.png"
    save_dir = "captures"
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
    save_path = os.path.join(save_dir, file_name)

    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    # 초기 창 크기 설정
    chrome_options.add_argument('--window-size=1200,1000')

    driver = webdriver.Chrome(service=Service(
        ChromeDriverManager().install()), options=chrome_options)

    try:
        print(f"🚀 리포트 접속 중: {url}")
        driver.get(url)

        # Streamlit 앱이 완전히 로드될 때까지 대기 (콘텐츠 양에 따라 조절)
        time.sleep(10)

        print("🔍 Streamlit 컨테이너 구조 조정 및 높이 계산...")

        # [핵심] CSS 주입: 내부 스크롤을 없애고 모든 콘텐츠를 위아래로 펼침
        # Streamlit의 메인 컨테이너들을 찾아 overflow를 visible로 강제 설정합니다.
        flatten_script = """
            const style = document.createElement('style');
            style.innerHTML = `
                /* 메인 컨테이너와 앱 뷰의 스크롤을 해제하여 전체 길이를 노출 */
                [data-testid="stAppViewContainer"], 
                [data-testid="stAppViewMain"], 
                .main, 
                .stApp {
                    overflow: visible !important;
                    height: auto !important;
                }
                /* 스크롤바 숨김 */
                ::-webkit-scrollbar {
                    display: none;
                }
            `;
            document.head.appendChild(style);
            
            // 실제 콘텐츠가 담긴 요소의 높이를 반환
            const mainContent = document.querySelector('[data-testid="stAppViewMain"]') || document.querySelector('.main');
            return mainContent ? mainContent.scrollHeight : document.body.scrollHeight;
        """

        total_height = driver.execute_script(flatten_script)
        print(f"📏 계산된 콘텐츠 총 높이: {total_height}px")

        # 브라우저 창 크기를 콘텐츠 높이에 맞춰 재설정 (+ 여유분 100px)
        driver.set_window_size(1200, total_height + 100)
        time.sleep(2)  # 렌더링 안정화 대기

        # 스크린샷 저장
        driver.save_screenshot(save_path)
        print(f"✅ [저장 완료] 경로: {os.path.abspath(save_path)}")

    except Exception as e:
        print(f"❌ 오류 발생: {e}")
    finally:
        driver.quit()


if __name__ == "__main__":
    # Streamlit 기본 포트 8501 사용
    save_streamlit_to_custom_path("http://localhost:8501")
