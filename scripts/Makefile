APP_VERSION=0.9.3

stage-run: helm-stage-server-run
stage-rm: helm-stage-server-rm
71-run: helm-prod-71-run
71-rm: helm-prod-71-rm
72-run: helm-prod-72-run
72-rm: helm-prod-72-rm
73-run: helm-prod-73-run
73-rm: helm-prod-73-rm
74-run: helm-prod-74-run
74-rm: helm-prod-74-rm


# infra
helm-infra-run:
	-@helm install stdl-infra ../kube/infra -n media

helm-infra-rm:
	-@helm uninstall stdl-infra -n media


# expose
helm-expose-run:
	-@helm install stdl-expose ../kube/expose -n media

helm-expose-rm:
	-@helm uninstall stdl-expose -n media


# redis
helm-redis-dep:
	-@helm dependency update ./redis

helm-prod-redis-run:
	-@helm install stdl-redis ../kube/redis -n media

helm-prod-redis-rm:
	-@helm uninstall stdl-redis -n media

delete-prod-redis-pvc:
	-@kubectl delete pvc redis-data-stdl-redis-master-0 -n media
	-@kubectl delete pvc redis-data-stdl-redis-replicas-0 -n media
	-@kubectl delete pvc redis-data-stdl-redis-replicas-1 -n media
	-@kubectl delete pvc redis-data-stdl-redis-replicas-2 -n media
	-@kubectl delete pvc redis-data-stdl-redis-replicas-3 -n media

helm-stage-redis-run:
	-@helm install stdl-redis ../kube/redis -n stage \
		--set metadata.namespace=stage \
		--set redis.master.service.type=NodePort \
		--set redis.master.service.nodePorts.redis=32133 \
		--set redis.master.persistence.enabled=false \
		--set redis.replica.service.type=NodePort \
		--set redis.replica.service.nodePorts.redis=32134 \
		--set redis.replica.persistence.enabled=false \
		--set redis.replica.replicaCount=1

helm-stage-redis-rm:
	-@helm uninstall stdl-redis -n stage


# server
helm-prod-71-rm:
	-@helm uninstall stdl-server-711 -n media
	-@helm uninstall stdl-server-712 -n media
	-@helm uninstall stdl-server-713 -n media

helm-prod-72-rm:
	-@helm uninstall stdl-server-721 -n media
	-@helm uninstall stdl-server-722 -n media
	-@helm uninstall stdl-server-723 -n media

helm-prod-73-rm:
	-@helm uninstall stdl-server-731 -n media
	-@helm uninstall stdl-server-732 -n media
	-@helm uninstall stdl-server-733 -n media

helm-prod-74-rm:
	-@helm uninstall stdl-server-741 -n media
	-@helm uninstall stdl-server-742 -n media
	-@helm uninstall stdl-server-743 -n media


#helm-prod-71-run:
#	-@helm install stdl-server-711 ../kube/server -n media \
#		--set image.tag=$(APP_VERSION) \
#		--set metadata.name=stdl-server-711 \
#		--set ingress.hostName=stdl-711.rwiv.xyz \
#		--set vpn.enabled=false \
#		--set nodeName=host-71
#	-@helm install stdl-server-712 ../kube/server -n media \
#		--set image.tag=$(APP_VERSION) \
#		--set metadata.name=stdl-server-712 \
#		--set ingress.hostName=stdl-712.rwiv.xyz \
#		--set vpn.enabled=false \
#		--set nodeName=host-71
#	-@helm install stdl-server-713 ../kube/server -n media \
#		--set image.tag=$(APP_VERSION) \
#		--set metadata.name=stdl-server-713 \
#		--set ingress.hostName=stdl-713.rwiv.xyz \
#		--set vpn.enabled=false \
#		--set nodeName=host-71
#
#helm-prod-72-run:
#	-@helm install stdl-server-721 ../kube/server -n media \
#		--set image.tag=$(APP_VERSION) \
#		--set metadata.name=stdl-server-721 \
#		--set ingress.hostName=stdl-721.rwiv.xyz \
#		--set vpn.enabled=false \
#		--set nodeName=host-72
#	-@helm install stdl-server-722 ../kube/server -n media \
#		--set image.tag=$(APP_VERSION) \
#		--set metadata.name=stdl-server-722 \
#		--set ingress.hostName=stdl-722.rwiv.xyz \
#		--set vpn.enabled=false \
#		--set nodeName=host-72
#	-@helm install stdl-server-723 ../kube/server -n media \
#		--set image.tag=$(APP_VERSION) \
#		--set metadata.name=stdl-server-723 \
#		--set ingress.hostName=stdl-723.rwiv.xyz \
#		--set vpn.enabled=false \
#		--set nodeName=host-72
#
#helm-prod-73-run:
#	-@helm install stdl-server-731 ../kube/server -n media \
#		--set image.tag=$(APP_VERSION) \
#		--set metadata.name=stdl-server-731 \
#		--set ingress.hostName=stdl-731.rwiv.xyz \
#		--set vpn.enabled=false \
#		--set nodeName=host-73
#	-@helm install stdl-server-732 ../kube/server -n media \
#		--set image.tag=$(APP_VERSION) \
#		--set metadata.name=stdl-server-732 \
#		--set ingress.hostName=stdl-732.rwiv.xyz \
#		--set vpn.enabled=false \
#		--set nodeName=host-73
#	-@helm install stdl-server-733 ../kube/server -n media \
#		--set image.tag=$(APP_VERSION) \
#		--set metadata.name=stdl-server-733 \
#		--set ingress.hostName=stdl-733.rwiv.xyz \
#		--set vpn.enabled=false \
#		--set nodeName=host-73
#
#helm-prod-74-run:
#	-@helm install stdl-server-741 ../kube/server -n media \
#		--set image.tag=$(APP_VERSION) \
#		--set metadata.name=stdl-server-741 \
#		--set ingress.hostName=stdl-741.rwiv.xyz \
#		--set vpn.enabled=false \
#		--set nodeName=host-74
#	-@helm install stdl-server-742 ../kube/server -n media \
#		--set image.tag=$(APP_VERSION) \
#		--set metadata.name=stdl-server-742 \
#		--set ingress.hostName=stdl-742.rwiv.xyz \
#		--set vpn.enabled=false \
#		--set nodeName=host-74
#	-@helm install stdl-server-743 ../kube/server -n media \
#		--set image.tag=$(APP_VERSION) \
#		--set metadata.name=stdl-server-743 \
#		--set ingress.hostName=stdl-743.rwiv.xyz \
#		--set vpn.enabled=false \
#		--set nodeName=host-74


helm-prod-71-run:
	-@helm install stdl-server-711 ../kube/server -n media \
		--set image.tag=$(APP_VERSION) \
		--set metadata.name=stdl-server-711 \
		--set ingress.hostName=stdl-711.rwiv.xyz \
		--set vpn.enabled=true \
		--set vpn.proxyEndpoint=http://stdl-proxy-71.media.svc.cluster.local \
		--set nodeName=host-71
	-@helm install stdl-server-712 ../kube/server -n media \
		--set image.tag=$(APP_VERSION) \
		--set metadata.name=stdl-server-712 \
		--set ingress.hostName=stdl-712.rwiv.xyz \
		--set vpn.enabled=true \
		--set vpn.proxyEndpoint=http://stdl-proxy-71.media.svc.cluster.local \
		--set nodeName=host-71
	-@helm install stdl-server-713 ../kube/server -n media \
		--set image.tag=$(APP_VERSION) \
		--set metadata.name=stdl-server-713 \
		--set ingress.hostName=stdl-713.rwiv.xyz \
		--set vpn.enabled=true \
		--set vpn.proxyEndpoint=http://stdl-proxy-71.media.svc.cluster.local \
		--set nodeName=host-71

helm-prod-72-run:
	-@helm install stdl-server-721 ../kube/server -n media \
		--set image.tag=$(APP_VERSION) \
		--set metadata.name=stdl-server-721 \
		--set ingress.hostName=stdl-721.rwiv.xyz \
		--set vpn.enabled=true \
		--set vpn.proxyEndpoint=http://stdl-proxy-72.media.svc.cluster.local \
		--set nodeName=host-72
	-@helm install stdl-server-722 ../kube/server -n media \
		--set image.tag=$(APP_VERSION) \
		--set metadata.name=stdl-server-722 \
		--set ingress.hostName=stdl-722.rwiv.xyz \
		--set vpn.enabled=true \
		--set vpn.proxyEndpoint=http://stdl-proxy-72.media.svc.cluster.local \
		--set nodeName=host-72
	-@helm install stdl-server-723 ../kube/server -n media \
		--set image.tag=$(APP_VERSION) \
		--set metadata.name=stdl-server-723 \
		--set ingress.hostName=stdl-723.rwiv.xyz \
		--set vpn.enabled=true \
		--set vpn.proxyEndpoint=http://stdl-proxy-72.media.svc.cluster.local \
		--set nodeName=host-72


helm-prod-73-run:
	-@helm install stdl-server-731 ../kube/server -n media \
		--set image.tag=$(APP_VERSION) \
		--set metadata.name=stdl-server-731 \
		--set ingress.hostName=stdl-731.rwiv.xyz \
		--set vpn.enabled=true \
		--set vpn.proxyEndpoint=http://stdl-proxy-73.media.svc.cluster.local \
		--set nodeName=host-73
	-@helm install stdl-server-732 ../kube/server -n media \
		--set image.tag=$(APP_VERSION) \
		--set metadata.name=stdl-server-732 \
		--set ingress.hostName=stdl-732.rwiv.xyz \
		--set vpn.enabled=true \
		--set vpn.proxyEndpoint=http://stdl-proxy-73.media.svc.cluster.local \
		--set nodeName=host-73
	-@helm install stdl-server-733 ../kube/server -n media \
		--set image.tag=$(APP_VERSION) \
		--set metadata.name=stdl-server-733 \
		--set ingress.hostName=stdl-733.rwiv.xyz \
		--set vpn.enabled=true \
		--set vpn.proxyEndpoint=http://stdl-proxy-73.media.svc.cluster.local \
		--set nodeName=host-73

helm-prod-74-run:
	-@helm install stdl-server-741 ../kube/server -n media \
		--set image.tag=$(APP_VERSION) \
		--set metadata.name=stdl-server-741 \
		--set ingress.hostName=stdl-741.rwiv.xyz \
		--set vpn.enabled=true \
		--set vpn.proxyEndpoint=http://stdl-proxy-74.media.svc.cluster.local \
		--set nodeName=host-74
	-@helm install stdl-server-742 ../kube/server -n media \
		--set image.tag=$(APP_VERSION) \
		--set metadata.name=stdl-server-742 \
		--set ingress.hostName=stdl-742.rwiv.xyz \
		--set vpn.enabled=true \
		--set vpn.proxyEndpoint=http://stdl-proxy-74.media.svc.cluster.local \
		--set nodeName=host-74
	-@helm install stdl-server-743 ../kube/server -n media \
		--set image.tag=$(APP_VERSION) \
		--set metadata.name=stdl-server-743 \
		--set ingress.hostName=stdl-743.rwiv.xyz \
		--set vpn.enabled=true \
		--set vpn.proxyEndpoint=http://stdl-proxy-74.media.svc.cluster.local \
		--set nodeName=host-74


helm-prod-proxy-run:
	-@helm install stdl-proxy-71 ../kube/proxy -n media \
		--set image.tag=$(APP_VERSION) \
		--set metadata.name=stdl-proxy-71 \
		--set nodeName=host-71
	-@helm install stdl-proxy-72 ../kube/proxy -n media \
		--set image.tag=$(APP_VERSION) \
		--set metadata.name=stdl-proxy-72 \
		--set nodeName=host-72
	-@helm install stdl-proxy-73 ../kube/proxy -n media \
		--set image.tag=$(APP_VERSION) \
		--set metadata.name=stdl-proxy-73 \
		--set nodeName=host-73
	-@helm install stdl-proxy-74 ../kube/proxy -n media \
		--set image.tag=$(APP_VERSION) \
		--set metadata.name=stdl-proxy-74 \
		--set nodeName=host-74

helm-prod-proxy-rm:
	-@helm uninstall stdl-proxy-71 -n media
	-@helm uninstall stdl-proxy-72 -n media
	-@helm uninstall stdl-proxy-73 -n media
	-@helm uninstall stdl-proxy-74 -n media


helm-stage-server-run:
	-@helm install stdl-server-521 ../kube/server -n stage \
		--set metadata.namespace=stage \
		--set metadata.name=stdl-server-521 \
		--set image.repository=harbor.rwiv.xyz/test/stdl \
		--set image.tag=latest \
		--set ingress.hostName=stdl-521.rwiv.xyz \
		--set vpn.enabled=false \
		--set redis.masterHost=stdl-redis-master.stage.svc.cluster.local \
		--set redis.masterPort=6379 \
		--set redis.replicaHost=stdl-redis-replicas.stage.svc.cluster.local \
		--set redis.replicaPort=6379 \
		--set nodeName=vm-52
	-@helm install stdl-server-522 ../kube/server -n stage \
		--set metadata.namespace=stage \
		--set metadata.name=stdl-server-522 \
		--set image.repository=harbor.rwiv.xyz/test/stdl \
		--set image.tag=latest \
		--set ingress.hostName=stdl-522.rwiv.xyz \
		--set vpn.enabled=true \
		--set vpn.proxyEndpoint=http://stdl-proxy.stage.svc.cluster.local \
		--set redis.masterHost=stdl-redis-master.stage.svc.cluster.local \
		--set redis.masterPort=6379 \
		--set redis.replicaHost=stdl-redis-replicas.stage.svc.cluster.local \
		--set redis.replicaPort=6379 \
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


helm-template:
	helm template ../kube/server --set vpn.enabled=true --set image.tag=$(APP_VERSION) > ../dev/main.yaml
