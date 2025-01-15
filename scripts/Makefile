infra-run:
	helm install stdl-infra ../kube/infra -n media

infra-rm:
	helm uninstall stdl-infra -n media

proton-run:
	helm install stdl-proxy-proton ../kube/proxy-proton -n media

proton-rm:
	helm uninstall stdl-proxy-proton -n media

sep-infra-up:
	helm upgrade stdl-sep-infra ../kube/separate-infra -n media

test-run:
	helm install stdl ../kube/chart \
		--set global.namespace=stage \
		--set image.repository=harbor.rwiv.xyz/test/stdl

test-rm:
	helm uninstall stdl
