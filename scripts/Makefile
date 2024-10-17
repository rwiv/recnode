test-run:
	helm install stdl ../kube/chart \
		--set global.namespace=stage \
		--set image.repository=harbor.rwiv.xyz/test/stdl

test-rm:
	helm uninstall stdl
