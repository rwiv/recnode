FROM ubuntu:24.10

WORKDIR /app
COPY requirements.txt requirements.txt
COPY main.py main.py
COPY stdl stdl

RUN apt-get update
RUN apt-get install python3 python3-pip ffmpeg -y

RUN pip install -r requirements.txt

ENTRYPOINT ["python3", "-u", "main.py"]
