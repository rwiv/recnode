prod-run: helm-prod-server1-run helm-prod-server2-run
prod-rm: helm-prod-server1-rm helm-prod-server2-rm
stage-run: helm-stage-server1-run helm-stage-server2-run
stage-rm: helm-stage-server1-rm helm-stage-server2-rm

argo-run:
	kubectl create -f ../kube/argocd/application.yaml


helm-redis-dep:
	@helm dependency update ./redis

helm-prod-redis-run:
	helm install stdl-redis ../kube/redis -n media -f ../dev/redis-values.yaml

helm-prod-redis-rm:
	helm uninstall stdl-redis -n media

delete-prod-redis-pvc:
	kubectl delete pvc redis-data-stdl-redis-master-0 -n media

helm-stage-redis-run:
	helm install stdl-redis ../kube/redis -n stage -f ../dev/redis-values-stage.yaml \
		--set metadata.namespace=stage \
		--set domainName="stdl-redis-stage.rwiv.xyz" \
		--set redis.master.service.nodePorts.redis=32133

helm-stage-redis-rm:
	helm uninstall stdl-redis -n stage

delete-stage-redis-pvc:
	kubectl delete pvc redis-data-stdl-redis-master-0 -n stage

print-redis-ca:
	kubectl get secret stdl-redis-tls -n media -o jsonpath="{['data']['ca\.crt']}" | base64 --decode && echo


helm-prod-server1-run:
	helm install stdl-server1 ../kube/server -n media \
		--set metadata.name=stdl-server1 \
		--set image.tag=0.5.4 \
		--set server.service.nodePort=30215

helm-prod-server1-rm:
	helm uninstall stdl-server1 -n media

helm-prod-server2-run:
	helm install stdl-server2 ../kube/server -n media \
		--set metadata.name=stdl-server2 \
		--set image.tag=0.5.4 \
		--set server.service.nodePort=30216

helm-prod-server2-rm:
	helm uninstall stdl-server2 -n media


helm-stage-server1-run:
	helm install stdl-server1 ../kube/server -n stage \
		--set metadata.namespace=stage \
		--set metadata.name=stdl-server1 \
		--set image.repository=harbor.rwiv.xyz/test/stdl \
		--set image.tag=latest \
		--set server.service.nodePort=32121

helm-stage-server1-rm:
	helm uninstall stdl-server1 -n stage

helm-stage-server2-run:
	helm install stdl-server2 ../kube/server -n stage \
		--set metadata.namespace=stage \
		--set metadata.name=stdl-server2 \
		--set image.repository=harbor.rwiv.xyz/test/stdl \
		--set image.tag=latest \
		--set server.service.nodePort=32122

helm-stage-server2-rm:
	helm uninstall stdl-server2 -n stage


proton-run:
	helm install stdl-proxy-proton ../kube/proxy-cloud -n media \
		--set gluetun.provider=protonvpn \
		--set gluetun.countries=Korea

proton-rm:
	helm uninstall stdl-proxy-proton -n media


nordvpn-run:
	helm install stdl-proxy-nordvpn ../kube/proxy-ovpn -n media \
		--set ovpn.confFilePath=conf/client1_kr102.ovpn

nordvpn-rm:
	helm uninstall stdl-proxy-nordvpn -n media
