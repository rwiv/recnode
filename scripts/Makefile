sep-infra-run:
	helm install stdl-sep-infra ../kube/separate-infra -n media

sep-infra-rm:
	helm uninstall stdl-sep-infra -n media

sep-infra-up:
	helm upgrade stdl-sep-infra ../kube/separate-infra -n media

test-run:
	helm install stdl ../kube/chart \
		--set global.namespace=stage \
		--set image.repository=harbor.rwiv.xyz/test/stdl

test-rm:
	helm uninstall stdl
