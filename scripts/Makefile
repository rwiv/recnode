infra-run:
	helm install stdl-infra ../kube/infra -n media

infra-rm:
	helm uninstall stdl-infra -n media

infra-up:
	helm upgrade stdl-infra ../kube/infra -n media

proton-run:
	helm install stdl-proxy-proton ../kube/proxy-proton -n media

proton-rm:
	helm uninstall stdl-proxy-proton -n media

sub1-run:
	helm install stdl-proxy-sub1 ../kube/proxy-ovpn -n media \
		--set global.suffix=sub1 \
		--set configmap.confFilePath=conf/client1.ovpn

sub1-rm:
	helm uninstall stdl-proxy-sub1 -n media


sub2-run:
	helm install stdl-proxy-sub2 ../kube/proxy-ovpn -n media \
		--set global.suffix=sub2 \
		--set configmap.confFilePath=conf/client2.ovpn

sub2-rm:
	helm uninstall stdl-proxy-sub2 -n media

test-run:
	helm install stdl ../kube/chart \
		--set global.namespace=stage \
		--set image.repository=harbor.rwiv.xyz/test/stdl

test-rm:
	helm uninstall stdl
