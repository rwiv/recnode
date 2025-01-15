cd ..
helm install "stdl-sep-video" ./kube/separate -f ./dev/values.yaml -n media
pause