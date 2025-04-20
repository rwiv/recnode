prod-run: helm-prod1-run helm-prod2-run
prod-rm: helm-prod1-rm helm-prod2-rm
stage-run: helm-stage1-run helm-stage2-run
stage-rm: helm-stage1-rm helm-stage2-rm

argo-run:
	kubectl create -f ../kube/argocd/application.yaml


helm-redis-dep:
	@helm dependency update ./redis

helm-redis-run:
	helm install stdl-redis ../kube/redis -n media \
	  -f ../dev/redis-values.yaml \

helm-redis-rm:
	helm uninstall stdl-redis -n media

delete-redis-pvc:
	kubectl delete pvc redis-data-stdl-redis-master-0 -n media

print-redis-ca:
	kubectl get secret stdl-redis-tls -n media -o jsonpath="{['data']['ca\.crt']}" | base64 --decode && echo


helm-prod1-run:
	helm install stdl-server1 ../kube/server -n media \
		--set metadata.name=stdl-server1 \
		--set image.tag=0.5.4 \
		--set server.service.nodePort=30215

helm-prod1-rm:
	helm uninstall stdl-server1 -n media

helm-prod2-run:
	helm install stdl-server2 ../kube/server -n media \
		--set metadata.name=stdl-server2 \
		--set image.tag=0.5.4 \
		--set server.service.nodePort=30216

helm-prod2-rm:
	helm uninstall stdl-server2 -n media


helm-stage1-run:
	helm install stdl-server1 ../kube/server -n stage \
		--set metadata.namespace=stage \
		--set metadata.name=stdl-server1 \
		--set image.repository=harbor.rwiv.xyz/test/stdl \
		--set image.tag=latest \
		--set server.service.nodePort=32121

helm-stage1-rm:
	helm uninstall stdl-server1 -n stage

helm-stage2-run:
	helm install stdl-server2 ../kube/server -n stage \
		--set metadata.namespace=stage \
		--set metadata.name=stdl-server2 \
		--set image.repository=harbor.rwiv.xyz/test/stdl \
		--set image.tag=latest \
		--set server.service.nodePort=32122

helm-stage2-rm:
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
