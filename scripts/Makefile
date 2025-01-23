proxy-run: proton-run nordvpn1-run nordvpn2-run
proxy-rm: proton-rm nordvpn1-rm nordvpn2-rm

infra-run:
	helm install stdl-infra ../kube/infra -n media

infra-rm:
	helm uninstall stdl-infra -n media

infra-up:
	helm upgrade stdl-infra ../kube/infra -n media


proton-run:
	helm install stdl-proxy-proton ../kube/proxy-cloud -n media \
		--set global.suffix=proton \
		--set gluetun.provider=protonvpn \
		--set gluetun.countries=Korea \
		--set secret.name=stdl-proton-secret

proton-rm:
	helm uninstall stdl-proxy-proton -n media


nordvpn1-run:
	helm install stdl-proxy-nordvpn1 ../kube/proxy-ovpn -n media \
		--set global.suffix=nordvpn1 \
		--set ovpn.confFilePath=conf/client1_95.ovpn \
		--set ovpn.secretName=stdl-nordvpn1-secret

nordvpn1-rm:
	helm uninstall stdl-proxy-nordvpn1 -n media


nordvpn2-run:
	helm install stdl-proxy-nordvpn2 ../kube/proxy-ovpn -n media \
		--set global.suffix=nordvpn2 \
		--set ovpn.confFilePath=conf/client2_110.ovpn \
		--set ovpn.secretName=stdl-nordvpn2-secret

nordvpn2-rm:
	helm uninstall stdl-proxy-nordvpn2 -n media


sub-run:
	helm install stdl-proxy-sub1 ../kube/proxy-ovpn -n media \
		--set global.suffix=sub1 \
		--set ovpn.confFilePath=conf/client1.ovpn

sub-rm:
	helm uninstall stdl-proxy-sub1 -n media
