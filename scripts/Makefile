APP_VERSION=0.6.3

stage-run: helm-stage-server-run
stage-rm: helm-stage-server-rm
71-run: helm-prod-71-run
71-rm: helm-prod-71-rm
72-run: helm-prod-72-run
72-rm: helm-prod-72-rm
73-run: helm-prod-73-run
73-rm: helm-prod-73-rm

# infra
## argo
argo-run:
	-@kubectl create -f ../kube/argocd/application.yaml

## secrets
print-redis-ca:
	-@kubectl get secret stdl-redis-tls -n media -o jsonpath="{['data']['ca\.crt']}" | base64 --decode && echo

create-home-prod-secrets:
	-@kubectl create secret generic stdl-fs -n media \
	  --from-file=fs-name=../dev/secrets/fs/fs-name.txt \
	  --from-file=fs_conf.yaml=../dev/secrets/fs/fs_conf.yaml
	-@kubectl create secret generic stdl-redis-auth -n media \
	  --from-file=password=../dev/secrets/redis-auth/password.txt
	-@kubectl create secret generic stdl-redis-tls -n media \
	  --from-file=ca.crt=../dev/secrets/redis-tls/ca.crt

delete-home-prod-secrets:
	-@kubectl delete secret stdl-fs -n media
	-@kubectl delete secret stdl-redis-auth -n media
	-@kubectl delete secret stdl-redis-tls -n media

create-cloud-prod-secrets:
	-@kubectl create secret generic stdl-fs -n media \
	  --from-file=fs-name=../dev/secrets/fs/fs-name.txt \
	  --from-file=fs_conf.yaml=../dev/secrets/fs/fs_conf.yaml
	-@kubectl create secret generic stdl-redis-auth -n media \
	  --from-file=password=../dev/secrets/redis-auth/password.txt
	-@kubectl create secret generic nordvpn-credentials -n media \
	  --from-file=username=../dev/secrets/nordvpn-credentials/username.txt \
	  --from-file=password=../dev/secrets/nordvpn-credentials/password.txt
	-@kubectl create secret generic nordvpn-vpn -n media \
	  --from-file=client.ovpn=../dev/secrets/nordvpn-vpn/client.ovpn

delete-cloud-prod-secrets:
	-@kubectl delete secret stdl-fs -n media
	-@kubectl delete secret stdl-redis-auth -n media
	-@kubectl delete secret nordvpn-credentials -n media
	-@kubectl delete secret nordvpn-vpn -n media

create-stage-secrets:
	-@kubectl create secret generic stdl-fs -n stage \
	  --from-file=fs-name=../dev/secrets/fs/fs-name.txt \
	  --from-file=fs_conf.yaml=../dev/secrets/fs/fs_conf.yaml
	-@kubectl create secret generic stdl-redis-auth -n stage \
	  --from-file=password=../dev/secrets/redis-auth/password.txt
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
	-@helm dependency update ./redis

helm-prod-redis-run:
	-@helm install stdl-redis ../kube/redis -n media \
	  --set redis.master.persistence.enabled=false

helm-prod-redis-rm:
	-@helm uninstall stdl-redis -n media

delete-prod-redis-pvc:
	-@kubectl delete pvc redis-data-stdl-redis-master-0 -n media

helm-stage-redis-run:
	-@helm install stdl-redis ../kube/redis -n stage -f ../dev/redis-values-stage.yaml \
		--set metadata.namespace=stage \
		--set domainName=stdl-redis-stage.rwiv.xyz \
		--set redis.master.service.nodePorts.redis=32133

helm-stage-redis-rm:
	-@helm uninstall stdl-redis -n stage

delete-stage-redis-pvc:
	-@kubectl delete pvc redis-data-stdl-redis-master-0 -n stage


# server
helm-prod-71-run:
	-@helm install stdl-server-711 ../kube/server -n media \
		--set image.tag=$(APP_VERSION) \
		--set plain.enabled=true \
		--set plain.name=stdl-server-plain-711 \
		--set plain.hostName=stdl-plain-711.netcup.rwiv.xyz \
		--set vpn.enabled=true \
		--set vpn.name=stdl-server-vpn-711 \
		--set vpn.hostName=stdl-vpn-711.netcup.rwiv.xyz \
		--set vpn.proxyEndpoint=http://stdl-proxy-71.media.svc.cluster.local \
		--set gateway=istio-system/external-gateway \
		--set redis.host=stdl-redis.rwiv.xyz \
		--set redis.port=30211 \
		--set nodeName=host71
	-@helm install stdl-server-712 ../kube/server -n media \
		--set image.tag=$(APP_VERSION) \
		--set plain.enabled=false \
		--set vpn.enabled=true \
		--set vpn.name=stdl-server-vpn-712 \
		--set vpn.hostName=stdl-vpn-712.netcup.rwiv.xyz \
		--set vpn.proxyEndpoint=http://stdl-proxy-71.media.svc.cluster.local \
		--set gateway=istio-system/external-gateway \
		--set redis.host=stdl-redis.rwiv.xyz \
		--set redis.port=30211 \
		--set nodeName=host71
	-@helm install stdl-server-713 ../kube/server -n media \
		--set image.tag=$(APP_VERSION) \
		--set plain.enabled=false \
		--set vpn.enabled=true \
		--set vpn.name=stdl-server-vpn-713 \
		--set vpn.hostName=stdl-vpn-713.netcup.rwiv.xyz \
		--set vpn.proxyEndpoint=http://stdl-proxy-71.media.svc.cluster.local \
		--set gateway=istio-system/external-gateway \
		--set redis.host=stdl-redis.rwiv.xyz \
		--set redis.port=30211 \
		--set nodeName=host71

helm-prod-71-rm:
	-@helm uninstall stdl-server-711 -n media
	-@helm uninstall stdl-server-712 -n media
	-@helm uninstall stdl-server-713 -n media

helm-prod-72-run:
	-@helm install stdl-server-721 ../kube/server -n media \
		--set image.tag=$(APP_VERSION) \
		--set plain.enabled=true \
		--set plain.name=stdl-server-plain-721 \
		--set plain.hostName=stdl-plain-721.netcup.rwiv.xyz \
		--set vpn.enabled=true \
		--set vpn.name=stdl-server-vpn-721 \
		--set vpn.hostName=stdl-vpn-721.netcup.rwiv.xyz \
		--set vpn.proxyEndpoint=http://stdl-proxy-72.media.svc.cluster.local \
		--set gateway=istio-system/external-gateway \
		--set redis.host=stdl-redis.rwiv.xyz \
		--set redis.port=30211 \
		--set nodeName=host72
	-@helm install stdl-server-722 ../kube/server -n media \
		--set image.tag=$(APP_VERSION) \
		--set plain.enabled=false \
		--set vpn.enabled=true \
		--set vpn.name=stdl-server-vpn-722 \
		--set vpn.hostName=stdl-vpn-722.netcup.rwiv.xyz \
		--set vpn.proxyEndpoint=http://stdl-proxy-72.media.svc.cluster.local \
		--set gateway=istio-system/external-gateway \
		--set redis.host=stdl-redis.rwiv.xyz \
		--set redis.port=30211 \
		--set nodeName=host72
	-@helm install stdl-server-723 ../kube/server -n media \
		--set image.tag=$(APP_VERSION) \
		--set plain.enabled=false \
		--set vpn.enabled=true \
		--set vpn.name=stdl-server-vpn-723 \
		--set vpn.hostName=stdl-vpn-723.netcup.rwiv.xyz \
		--set vpn.proxyEndpoint=http://stdl-proxy-72.media.svc.cluster.local \
		--set gateway=istio-system/external-gateway \
		--set redis.host=stdl-redis.rwiv.xyz \
		--set redis.port=30211 \
		--set nodeName=host72

helm-prod-72-rm:
	-@helm uninstall stdl-server-721 -n media
	-@helm uninstall stdl-server-722 -n media
	-@helm uninstall stdl-server-723 -n media

helm-prod-73-run:
	-@helm install stdl-server-731 ../kube/server -n media \
		--set image.tag=$(APP_VERSION) \
		--set plain.enabled=true \
		--set plain.name=stdl-server-plain-731 \
		--set plain.hostName=stdl-plain-731.netcup.rwiv.xyz \
		--set vpn.enabled=true \
		--set vpn.name=stdl-server-vpn-731 \
		--set vpn.hostName=stdl-vpn-731.netcup.rwiv.xyz \
		--set vpn.proxyEndpoint=http://stdl-proxy-73.media.svc.cluster.local \
		--set gateway=istio-system/external-gateway \
		--set redis.host=stdl-redis.rwiv.xyz \
		--set redis.port=30211 \
		--set nodeName=host73
	-@helm install stdl-server-732 ../kube/server -n media \
		--set image.tag=$(APP_VERSION) \
		--set plain.enabled=false \
		--set vpn.enabled=true \
		--set vpn.name=stdl-server-vpn-732 \
		--set vpn.hostName=stdl-vpn-732.netcup.rwiv.xyz \
		--set vpn.proxyEndpoint=http://stdl-proxy-73.media.svc.cluster.local \
		--set gateway=istio-system/external-gateway \
		--set redis.host=stdl-redis.rwiv.xyz \
		--set redis.port=30211 \
		--set nodeName=host73
	-@helm install stdl-server-733 ../kube/server -n media \
		--set image.tag=$(APP_VERSION) \
		--set plain.enabled=false \
		--set vpn.enabled=true \
		--set vpn.name=stdl-server-vpn-733 \
		--set vpn.hostName=stdl-vpn-733.netcup.rwiv.xyz \
		--set vpn.proxyEndpoint=http://stdl-proxy-73.media.svc.cluster.local \
		--set gateway=istio-system/external-gateway \
		--set redis.host=stdl-redis.rwiv.xyz \
		--set redis.port=30211 \
		--set nodeName=host73

helm-prod-73-rm:
	-@helm uninstall stdl-server-731 -n media
	-@helm uninstall stdl-server-732 -n media
	-@helm uninstall stdl-server-733 -n media

helm-prod-proxy-run:
	-@helm install stdl-proxy-71 ../kube/proxy -n media \
		--set image.tag=$(APP_VERSION) \
		--set metadata.name=stdl-proxy-71 \
		--set nodeName=host71
	-@helm install stdl-proxy-72 ../kube/proxy -n media \
		--set image.tag=$(APP_VERSION) \
		--set metadata.name=stdl-proxy-72 \
		--set nodeName=host72
	-@helm install stdl-proxy-73 ../kube/proxy -n media \
		--set image.tag=$(APP_VERSION) \
		--set metadata.name=stdl-proxy-73 \
		--set nodeName=host73

helm-prod-proxy-rm:
	-@helm uninstall stdl-proxy-71 -n media
	-@helm uninstall stdl-proxy-72 -n media
	-@helm uninstall stdl-proxy-73 -n media

helm-stage-server-run:
	-@helm install stdl-server-521 ../kube/server -n stage \
		--set metadata.namespace=stage \
		--set image.repository=harbor.rwiv.xyz/test/stdl \
		--set image.tag=latest \
		--set plain.enabled=true \
		--set plain.name=stdl-server-plain-521 \
		--set plain.hostName=stdl-plain-521.home.rwiv.xyz \
		--set vpn.enabled=true \
		--set vpn.name=stdl-server-vpn-521 \
		--set vpn.hostName=stdl-vpn-521.home.rwiv.xyz \
		--set vpn.proxyEndpoint=http://stdl-proxy.stage.svc.cluster.local \
		--set gateway=istio-system/internal-gateway \
		--set redis.host=stdl-redis-stage.rwiv.xyz \
		--set redis.port=6379 \
		--set redis.data.liveExpireSec=60 \
		--set redis.data.segExpireSec=60 \
		--set nodeName=vm-52
	-@helm install stdl-server-522 ../kube/server -n stage \
		--set metadata.namespace=stage \
		--set image.repository=harbor.rwiv.xyz/test/stdl \
		--set image.tag=latest \
		--set plain.enabled=true \
		--set plain.name=stdl-server-plain-522 \
		--set plain.hostName=stdl-plain-522.home.rwiv.xyz \
		--set vpn.enabled=false \
		--set gateway=istio-system/internal-gateway \
		--set redis.host=stdl-redis-stage.rwiv.xyz \
		--set redis.port=6379 \
		--set redis.data.liveExpireSec=60 \
		--set redis.data.segExpireSec=60 \
		--set nodeName=vm-52

helm-stage-server-rm:
	-@helm uninstall stdl-server-521 -n stage
	-@helm uninstall stdl-server-522 -n stage

helm-stage-proxy-run:
	-@helm install stdl-proxy ../kube/proxy -n stage \
		--set metadata.namespace=stage \
		--set image.repository=harbor.rwiv.xyz/test/stdl \
		--set image.tag=latest \
		--set server.replicas=1

helm-stage-proxy-rm:
	-@helm uninstall stdl-proxy -n stage
