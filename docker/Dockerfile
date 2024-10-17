FROM python:3.9.19-alpine

WORKDIR /app
COPY requirements.txt requirements.txt
COPY main.py main.py
COPY stdl stdl
COPY docker/ffmpeg /usr/local/bin/ffmpeg

RUN pip install -r requirements.txt

ENTRYPOINT ["python", "-u", "main.py"]
