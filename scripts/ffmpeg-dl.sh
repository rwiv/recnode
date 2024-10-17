#!/bin/sh

wget https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz
tar -xvf ffmpeg-release-amd64-static.tar.xz

chmod +x ffmpeg-7.0.2-amd64-static/ffmpeg
sudo mv ffmpeg-7.0.2-amd64-static/ffmpeg ../docker/ffmpeg

rm -rf ffmpeg-release-amd64-static.tar.xz
rm -rf ffmpeg-7.0.2-amd64-static
