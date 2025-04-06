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
