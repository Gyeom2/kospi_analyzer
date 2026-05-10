from jinja2 import Environment, FileSystemLoader
from weasyprint import HTML
import datetime


def generate_pdf_report(data_dict):
    # 1. Jinja2 환경 설정
    env = Environment(loader=FileSystemLoader('templates'))
    template = env.get_template('report.html')

    # 2. HTML 렌더링 (데이터 주입)
    html_out = template.render(
        date=datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
        max_pain=data_dict['max_pain'],
        calls=data_dict['calls'].to_dict('records'),  # DataFrame을 리스트로 변환
        puts=data_dict['puts'].to_dict('records')
    )

    # 3. PDF 저장
    file_path = f"reports/report_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.pdf"
    HTML(string=html_out).write_pdf(file_path)

    return file_path
