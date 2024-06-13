FROM python:3.11

# Python 출력 버퍼링 비활성화
ENV PYTHONUNBUFFERED 1

# 작업 디렉토리 설정
WORKDIR /app

# 필요 패키지 설치
COPY requirements.txt /app/

RUN pip install --no-cache-dir -r requirements.txt

COPY . /app/

# 환경 변수 설정
COPY .env /app/.env

# 카프카 컨슈머 실행 스크립트
COPY kafka_consumer.py /app/kafka_consumer.py

# Django management command 추가
COPY diary/management/commands/consume_diary.py /app/diary/management/commands/consume_diary.py

# expose 포트
EXPOSE 8000

CMD ["sh", "-c", "python manage.py migrate && python manage.py runserver 0.0.0.0:8000 & python manage.py consume_diary"]
