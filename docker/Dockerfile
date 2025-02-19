FROM python:3.9.19-alpine

WORKDIR /app

COPY requirements.txt requirements.txt
COPY main_batch.py main_batch.py
COPY main_server.py main_server.py
COPY stdl stdl

RUN wget https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz && \
    tar -xvf ffmpeg-release-amd64-static.tar.xz && \
    chmod +x ffmpeg-7.0.2-amd64-static/ffmpeg && \
    mv ffmpeg-7.0.2-amd64-static/ffmpeg /usr/local/bin/ffmpeg && \
    rm -rf ffmpeg-release-amd64-static.tar.xz ffmpeg-7.0.2-amd64-static

RUN pip install -r requirements.txt
RUN pip install git+https://github.com/rwiv/pyutils.git@v0.1.2
RUN pip install git+https://github.com/rwiv/pynifs.git@v0.1.1

ENTRYPOINT ["python", "-u", "-m", "stdl", "batch"]
