stage-run: helm-stage-server-run
stage-rm: helm-stage-server-rm

71-run: helm-prod-71-run
71-rm: helm-prod-71-rm
72-run: helm-prod-72-run
72-rm: helm-prod-72-rm
73-run: helm-prod-73-run
73-rm: helm-prod-73-rm
7-all-run: 71-run 72-run 73-run
7-all-rm: 71-rm 72-rm 73-rm

81-run: helm-prod-81-run
81-rm: helm-prod-81-rm
82-run: helm-prod-82-run
82-rm: helm-prod-82-rm
83-run: helm-prod-83-run
83-rm: helm-prod-83-rm
8-all-run: 81-run 82-run 83-run
8-all-rm: 81-rm 82-rm 83-rm


# infra
helm-infra-run:
	-@helm install stdl-infra ../kube/infra -n media

helm-infra-rm:
	-@helm uninstall stdl-infra -n media


# redis
helm-redis-dep:
	-@helm dependency update ./redis

helm-prod-redis-run:
	-@helm install stdl-redis ../kube/redis -n media --set redis.replica.replicaCount=3

helm-prod-redis-rm:
	-@helm uninstall stdl-redis -n media

delete-prod-redis-pvc:
	-@kubectl delete pvc redis-data-stdl-redis-master-0 -n media
	-@kubectl delete pvc redis-data-stdl-redis-replicas-0 -n media
	-@kubectl delete pvc redis-data-stdl-redis-replicas-1 -n media
	-@kubectl delete pvc redis-data-stdl-redis-replicas-2 -n media

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
	-@kubectx netcup
	-@helm uninstall stdl-server-711 -n media
	-@helm uninstall stdl-server-712 -n media

helm-prod-72-rm:
	-@kubectx netcup
	-@helm uninstall stdl-server-721 -n media
	-@helm uninstall stdl-server-722 -n media

helm-prod-73-rm:
	-@kubectx netcup
	-@helm uninstall stdl-server-731 -n media
	-@helm uninstall stdl-server-732 -n media


helm-prod-71-run:
	-@kubectx netcup
	-@helm install stdl-server-711 ../kube/server -n media \
		--set metadata.name=stdl-server-711 \
		--set ingress.hostName=stdl-711.rwiv.xyz \
		--set vpn.enabled=false \
		--set redis.isInternal=true \
		--set nodeAlias=node1
	-@helm install stdl-server-712 ../kube/server -n media \
		--set metadata.name=stdl-server-712 \
		--set ingress.hostName=stdl-712.rwiv.xyz \
		--set vpn.enabled=false \
		--set redis.isInternal=true \
		--set nodeAlias=node1

helm-prod-72-run:
	-@kubectx netcup
	-@helm install stdl-server-721 ../kube/server -n media \
		--set metadata.name=stdl-server-721 \
		--set ingress.hostName=stdl-721.rwiv.xyz \
		--set vpn.enabled=false \
		--set redis.isInternal=true \
		--set nodeAlias=node2
	-@helm install stdl-server-722 ../kube/server -n media \
		--set metadata.name=stdl-server-722 \
		--set ingress.hostName=stdl-722.rwiv.xyz \
		--set vpn.enabled=false \
		--set redis.isInternal=true \
		--set nodeAlias=node2

helm-prod-73-run:
	-@kubectx netcup
	-@helm install stdl-server-731 ../kube/server -n media \
		--set metadata.name=stdl-server-731 \
		--set ingress.hostName=stdl-731.rwiv.xyz \
		--set vpn.enabled=false \
		--set redis.isInternal=true \
		--set nodeAlias=node3
	-@helm install stdl-server-732 ../kube/server -n media \
		--set metadata.name=stdl-server-732 \
		--set ingress.hostName=stdl-732.rwiv.xyz \
		--set vpn.enabled=false \
		--set redis.isInternal=true \
		--set nodeAlias=node3


helm-prod-81-rm:
	-@kubectx ovh
	-@helm uninstall stdl-server-811 -n media
	-@helm uninstall stdl-server-812 -n media
	-@helm uninstall stdl-server-813 -n media
	-@helm uninstall stdl-server-814 -n media

helm-prod-82-rm:
	-@kubectx ovh
	-@helm uninstall stdl-server-821 -n media
	-@helm uninstall stdl-server-822 -n media
	-@helm uninstall stdl-server-823 -n media
	-@helm uninstall stdl-server-824 -n media

helm-prod-83-rm:
	-@kubectx ovh
	-@helm uninstall stdl-server-831 -n media
	-@helm uninstall stdl-server-832 -n media
	-@helm uninstall stdl-server-833 -n media
	-@helm uninstall stdl-server-834 -n media


helm-prod-81-run:
	-@kubectx ovh
	-@helm install stdl-server-811 ../kube/server -n media \
		--set metadata.name=stdl-server-811 \
		--set ingress.hostName=stdl-811.rwiv.xyz \
		--set vpn.enabled=false \
		--set redis.isInternal=false \
		--set nodeAlias=node1
	-@helm install stdl-server-812 ../kube/server -n media \
		--set metadata.name=stdl-server-812 \
		--set ingress.hostName=stdl-812.rwiv.xyz \
		--set vpn.enabled=false \
		--set redis.isInternal=false \
		--set nodeAlias=node1
	-@helm install stdl-server-813 ../kube/server -n media \
		--set metadata.name=stdl-server-813 \
		--set ingress.hostName=stdl-813.rwiv.xyz \
		--set vpn.enabled=false \
		--set redis.isInternal=false \
		--set nodeAlias=node1
	-@helm install stdl-server-814 ../kube/server -n media \
		--set metadata.name=stdl-server-814 \
		--set ingress.hostName=stdl-814.rwiv.xyz \
		--set vpn.enabled=false \
		--set redis.isInternal=false \
		--set nodeAlias=node1

helm-prod-82-run:
	-@kubectx ovh
	-@helm install stdl-server-821 ../kube/server -n media \
		--set metadata.name=stdl-server-821 \
		--set ingress.hostName=stdl-821.rwiv.xyz \
		--set vpn.enabled=false \
		--set redis.isInternal=false \
		--set nodeAlias=node2
	-@helm install stdl-server-822 ../kube/server -n media \
		--set metadata.name=stdl-server-822 \
		--set ingress.hostName=stdl-822.rwiv.xyz \
		--set vpn.enabled=false \
		--set redis.isInternal=false \
		--set nodeAlias=node2
	-@helm install stdl-server-823 ../kube/server -n media \
		--set metadata.name=stdl-server-823 \
		--set ingress.hostName=stdl-823.rwiv.xyz \
		--set vpn.enabled=false \
		--set redis.isInternal=false \
		--set nodeAlias=node2
	-@helm install stdl-server-824 ../kube/server -n media \
		--set metadata.name=stdl-server-824 \
		--set ingress.hostName=stdl-824.rwiv.xyz \
		--set vpn.enabled=false \
		--set redis.isInternal=false \
		--set nodeAlias=node2

helm-prod-83-run:
	-@kubectx ovh
	-@helm install stdl-server-831 ../kube/server -n media \
		--set metadata.name=stdl-server-831 \
		--set ingress.hostName=stdl-831.rwiv.xyz \
		--set vpn.enabled=false \
		--set redis.isInternal=false \
		--set nodeAlias=node3
	-@helm install stdl-server-832 ../kube/server -n media \
		--set metadata.name=stdl-server-832 \
		--set ingress.hostName=stdl-832.rwiv.xyz \
		--set vpn.enabled=false \
		--set redis.isInternal=false \
		--set nodeAlias=node3
	-@helm install stdl-server-833 ../kube/server -n media \
		--set metadata.name=stdl-server-833 \
		--set ingress.hostName=stdl-833.rwiv.xyz \
		--set vpn.enabled=false \
		--set redis.isInternal=false \
		--set nodeAlias=node3
	-@helm install stdl-server-834 ../kube/server -n media \
		--set metadata.name=stdl-server-834 \
		--set ingress.hostName=stdl-834.rwiv.xyz \
		--set vpn.enabled=false \
		--set redis.isInternal=false \
		--set nodeAlias=node3


#helm-prod-71-run:
#	-@helm install stdl-server-711 ../kube/server -n media \
#		--set metadata.name=stdl-server-711 \
#		--set ingress.hostName=stdl-711.rwiv.xyz \
#		--set vpn.enabled=true \
#		--set vpn.proxyEndpoint=http://stdl-proxy-71.media.svc.cluster.local \
#		--set nodeAlias=node1
#	-@helm install stdl-server-712 ../kube/server -n media \
#		--set metadata.name=stdl-server-712 \
#		--set ingress.hostName=stdl-712.rwiv.xyz \
#		--set vpn.enabled=true \
#		--set vpn.proxyEndpoint=http://stdl-proxy-71.media.svc.cluster.local \
#		--set nodeAlias=node1
#	-@helm install stdl-server-713 ../kube/server -n media \
#		--set metadata.name=stdl-server-713 \
#		--set ingress.hostName=stdl-713.rwiv.xyz \
#		--set vpn.enabled=true \
#		--set vpn.proxyEndpoint=http://stdl-proxy-71.media.svc.cluster.local \
#		--set nodeAlias=node1


#helm-prod-service-monitor-run:
#	-@helm install stdl-service-monitor-711 ../kube/service-monitor -n media --set metadata.name=stdl-server-711 --set nodeName=host-71
#	-@helm install stdl-service-monitor-712 ../kube/service-monitor -n media --set metadata.name=stdl-server-712 --set nodeName=host-71
#	-@helm install stdl-service-monitor-713 ../kube/service-monitor -n media --set metadata.name=stdl-server-713 --set nodeName=host-71
#	-@helm install stdl-service-monitor-721 ../kube/service-monitor -n media --set metadata.name=stdl-server-721 --set nodeName=host-72
#	-@helm install stdl-service-monitor-722 ../kube/service-monitor -n media --set metadata.name=stdl-server-722 --set nodeName=host-72
#	-@helm install stdl-service-monitor-723 ../kube/service-monitor -n media --set metadata.name=stdl-server-723 --set nodeName=host-72
#	-@helm install stdl-service-monitor-731 ../kube/service-monitor -n media --set metadata.name=stdl-server-731 --set nodeName=host-73
#	-@helm install stdl-service-monitor-732 ../kube/service-monitor -n media --set metadata.name=stdl-server-732 --set nodeName=host-73
#	-@helm install stdl-service-monitor-733 ../kube/service-monitor -n media --set metadata.name=stdl-server-733 --set nodeName=host-73
#
#
#helm-prod-service-monitor-rm:
#	-@helm uninstall stdl-service-monitor-711 -n media
#	-@helm uninstall stdl-service-monitor-712 -n media
#	-@helm uninstall stdl-service-monitor-713 -n media
#	-@helm uninstall stdl-service-monitor-721 -n media
#	-@helm uninstall stdl-service-monitor-722 -n media
#	-@helm uninstall stdl-service-monitor-723 -n media
#	-@helm uninstall stdl-service-monitor-731 -n media
#	-@helm uninstall stdl-service-monitor-732 -n media
#	-@helm uninstall stdl-service-monitor-733 -n media


helm-prod-proxy-run:
	-@helm install stdl-proxy-71 ../kube/proxy -n media \
		--set metadata.name=stdl-proxy-71 \
		--set nodeName=host-71
	-@helm install stdl-proxy-72 ../kube/proxy -n media \
		--set metadata.name=stdl-proxy-72 \
		--set nodeName=host-72
	-@helm install stdl-proxy-73 ../kube/proxy -n media \
		--set metadata.name=stdl-proxy-73 \
		--set nodeName=host-73
	-@helm install stdl-proxy-74 ../kube/proxy -n media \
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
	helm template ../kube/server --set vpn.enabled=true > ../dev/main.yaml
