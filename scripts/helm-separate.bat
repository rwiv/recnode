cd ..

helm install "stdl-sep-video-ch2" ./kube/separate-job -f ./dev/values.yaml -n media
pause