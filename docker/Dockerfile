FROM python:3.13.2-alpine

WORKDIR /app

COPY requirements.txt requirements.txt
COPY stdl stdl

RUN apk add git

RUN pip install -r requirements.txt

ENTRYPOINT ["python", "-m", "stdl", "batch"]
