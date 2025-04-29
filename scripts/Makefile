APP_VERSION=0.5.5

prod-run: helm-prod-server1-run helm-prod-server2-run
prod-rm: helm-prod-server1-rm helm-prod-server2-rm
stage-run: helm-stage-server1-run helm-stage-server2-run
stage-rm: helm-stage-server1-rm helm-stage-server2-rm

# infra
## argo
argo-run:
	kubectl create -f ../kube/argocd/application.yaml

## secrets
print-redis-ca:
	kubectl get secret stdl-redis-tls -n media -o jsonpath="{['data']['ca\.crt']}" | base64 --decode && echo

create-prod-secrets:
	-@kubectl create secret generic stdl-redis-auth -n media \
	  --from-file=password=../dev/secrets/redis/redis_password.txt
	-@kubectl create secret generic stdl-fs -n media \
	  --from-file=fs-name=../dev/secrets/fs/fs-name.txt \
	  --from-file=fs_conf.yaml=../dev/secrets/fs/fs_conf.yaml
	-@kubectl create secret generic nordvpn-credentials -n media \
	  --from-file=username=../dev/secrets/nordvpn-credentials/username.txt \
	  --from-file=password=../dev/secrets/nordvpn-credentials/password.txt
	-@kubectl create secret generic nordvpn-vpn -n media \
	  --from-file=client.ovpn=../dev/secrets/nordvpn-vpn/client.ovpn

delete-prod-secrets:
	-@kubectl delete secret stdl-redis-auth -n media
	-@kubectl delete secret stdl-fs -n media
	-@kubectl delete secret nordvpn-credentials -n media
	-@kubectl delete secret nordvpn-vpn -n media

create-stage-secrets:
	-@kubectl create secret generic stdl-redis-auth -n stage \
	  --from-file=password=../dev/secrets/redis/redis_password.txt
	-@kubectl create secret generic stdl-fs -n stage \
	  --from-file=fs-name=../dev/secrets/fs/fs-name.txt \
	  --from-file=fs_conf.yaml=../dev/secrets/fs/fs_conf.yaml
	-@kubectl create secret generic nordvpn-credentials -n stage \
	  --from-file=username=../dev/secrets/nordvpn-credentials/username.txt \
	  --from-file=password=../dev/secrets/nordvpn-credentials/password.txt
	-@kubectl create secret generic nordvpn-vpn -n stage \
	  --from-file=client.ovpn=../dev/secrets/nordvpn-vpn/client.ovpn

delete-stage-secrets:
	-@kubectl delete secret stdl-redis-auth -n stage
	-@kubectl delete secret stdl-fs -n stage
	-@kubectl delete secret nordvpn-credentials -n stage
	-@kubectl delete secret nordvpn-vpn -n stage

## redis
helm-redis-dep:
	@helm dependency update ./redis

helm-prod-redis-run:
	helm install stdl-redis ../kube/redis -n media

helm-prod-redis-rm:
	helm uninstall stdl-redis -n media

delete-prod-redis-pvc:
	kubectl delete pvc redis-data-stdl-redis-master-0 -n media

helm-stage-redis-run:
	helm install stdl-redis ../kube/redis -n stage -f ../dev/redis-values-stage.yaml \
		--set metadata.namespace=stage \
		--set domainName=stdl-redis-stage.rwiv.xyz \
		--set redis.master.service.nodePorts.redis=32133

helm-stage-redis-rm:
	helm uninstall stdl-redis -n stage

delete-stage-redis-pvc:
	kubectl delete pvc redis-data-stdl-redis-master-0 -n stage


# server
helm-prod-server1-run:
	helm install stdl-server1 ../kube/server -n media \
		--set metadata.name=stdl-server1 \
		--set image.tag=$(APP_VERSION) \
		--set server.service.nodePort=30215

helm-prod-server1-rm:
	helm uninstall stdl-server1 -n media

helm-prod-server2-run:
	helm install stdl-server2 ../kube/server -n media \
		--set metadata.name=stdl-server2 \
		--set image.tag=$(APP_VERSION) \
		--set server.service.nodePort=30216

helm-prod-server2-rm:
	helm uninstall stdl-server2 -n media


helm-stage-server1-run:
	helm install stdl-server1 ../kube/server -n stage \
		--set metadata.namespace=stage \
		--set metadata.name=stdl-server1 \
		--set image.repository=harbor.rwiv.xyz/test/stdl \
		--set image.tag=latest \
		--set server.service.nodePort=32121 \
		--set redis.host=stdl-redis-stage.rwiv.xyz \
		--set redis.port=6379

helm-stage-server1-rm:
	helm uninstall stdl-server1 -n stage

helm-stage-server2-run:
	helm install stdl-server2 ../kube/server -n stage \
		--set metadata.namespace=stage \
		--set metadata.name=stdl-server2 \
		--set image.repository=harbor.rwiv.xyz/test/stdl \
		--set image.tag=latest \
		--set server.service.nodePort=32122 \
		--set redis.host=stdl-redis-stage.rwiv.xyz \
		--set redis.port=6379

helm-stage-server2-rm:
	helm uninstall stdl-server2 -n stage


# server-vpn
helm-stage-vpn-run:
	helm install stdl-server-vpn ../kube/server-vpn -n stage \
		--set metadata.namespace=stage \
		--set metadata.name=stdl-server52 \
		--set vpn.name=stdl-server52-vpn \
		--set image.repository=harbor.rwiv.xyz/test/stdl \
		--set image.tag=latest \
		--set server.service.nodePort=32122 \
		--set vpn.service.nodePort=32121 \
		--set redis.host=stdl-redis-stage.rwiv.xyz \
		--set redis.port=6379 \
		--set server.proxyEndpoint=http://stdl-proxy52.stage.svc.cluster.local \
		--set nodeName=vm-52

helm-stage-vpn-rm:
	helm uninstall stdl-server-vpn -n stage

helm-stage-proxy-run:
	helm install stdl-proxy ../kube/proxy -n stage \
		--set metadata.namespace=stage \
		--set metadata.name=stdl-proxy52 \
		--set image.repository=harbor.rwiv.xyz/test/stdl \
		--set image.tag=latest \
		--set server.service.nodePort=32124 \
		--set nodeName=vm-52

helm-stage-proxy-rm:
	helm uninstall stdl-proxy -n stage
