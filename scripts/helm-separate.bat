cd ..

helm install "stdl-sep-me" ./kube/separate-job -f ./dev/values.yaml -n media
pause