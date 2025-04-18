server-run: helm-server1-run helm-server2-run
server-rm: helm-server1-rm helm-server2-rm
proxy-run: proton-run nordvpn-run
proxy-rm: proton-rm nordvpn-rm

argo-run:
	kubectl create -f ../kube/argocd/application.yaml


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


helm-server1-run:
	helm install stdl-server1 ../kube/server -n media \
		--set metadata.name=stdl-server1 \
		--set image.tag=0.5.3 \
		--set server.service.nodePort=30215

helm-server1-rm:
	helm uninstall stdl-server1 -n media

helm-server2-run:
	helm install stdl-server2 ../kube/server -n media \
		--set metadata.name=stdl-server2 \
		--set image.tag=0.5.3 \
		--set server.service.nodePort=30216

helm-server2-rm:
	helm uninstall stdl-server2 -n media
