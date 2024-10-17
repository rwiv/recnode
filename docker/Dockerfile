FROM ubuntu:24.10

WORKDIR /app
COPY requirements.txt requirements.txt
COPY main.py main.py
COPY stdl stdl

RUN apt-get update
RUN apt-get install python3-venv ffmpeg -y

RUN python3 -m venv /app/venv
RUN /app/venv/bin/pip3 install -r requirements.txt

ENTRYPOINT ["/app/venv/bin/python3", "-u", "main.py"]
