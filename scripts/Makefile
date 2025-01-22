APP_VERSION=0.2.2

infra-run:
	helm install stdl-infra ../kube/infra -n media

infra-rm:
	helm uninstall stdl-infra -n media

infra-up:
	helm upgrade stdl-infra ../kube/infra -n media

proton-run:
	helm install stdl-proxy-proton ../kube/proxy-cloud -n media \
		--set image.tag=$(APP_VERSION) \
		--set global.suffix=proton \
		--set gluetun.provider=protonvpn \
		--set gluetun.countries=Korea \
		--set secret.name=stdl-proton-secret

proton-rm:
	helm uninstall stdl-proxy-proton -n media

nordvpn-run:
	helm install stdl-proxy-nordvpn ../kube/proxy-ovpn -n media \
		--set image.tag=$(APP_VERSION) \
		--set global.suffix=nordvpn \
		--set ovpn.confFilePath=conf/kr95.nordvpn.com.udp.ovpn \
		--set ovpn.secretName=stdl-nordvpn-secret

nordvpn-rm:
	helm uninstall stdl-proxy-nordvpn -n media

sub-run:
	helm install stdl-proxy-sub1 ../kube/proxy-ovpn -n media \
		--set image.tag=$(APP_VERSION) \
		--set global.suffix=sub1 \
		--set ovpn.confFilePath=conf/client1.ovpn

sub-rm:
	helm uninstall stdl-proxy-sub1 -n media

test-run:
	helm install stdl ../kube/chart \
		--set global.namespace=stage \
		--set image.repository=harbor.rwiv.xyz/test/stdl

test-rm:
	helm uninstall stdl
