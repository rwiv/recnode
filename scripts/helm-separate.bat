cd ..

helm install "stdl-sep-video" ./kube/separate-job -f ./dev/values.yaml -n media
pause