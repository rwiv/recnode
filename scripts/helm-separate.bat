cd ..

helm install "stdl-sep-video-af" ./kube/separate-job -f ./dev/values.yaml -n media
pause