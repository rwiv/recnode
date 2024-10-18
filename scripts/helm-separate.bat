cd ..

helm install "stdl-sep-dsk" ./kube/separate-job -f ./dev/values.yaml -n media
pause