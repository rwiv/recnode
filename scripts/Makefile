proxy-run: proton-run nordvpn-run
proxy-rm: proton-rm nordvpn-rm

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


nordvpn-run:
	helm install stdl-proxy-nordvpn ../kube/proxy-ovpn -n media \
		--set global.suffix=nordvpn \
		--set ovpn.confFilePath=conf/client1_kr102.ovpn \
		--set ovpn.secretName=stdl-nordvpn-secret

nordvpn-rm:
	helm uninstall stdl-proxy-nordvpn -n media


sub-run:
	helm install stdl-proxy-sub1 ../kube/proxy-ovpn -n media \
		--set global.suffix=sub1 \
		--set ovpn.confFilePath=conf/client1.ovpn

sub-rm:
	helm uninstall stdl-proxy-sub1 -n media


server-up:
	sudo docker compose -f ../docker/docker-compose-server.yml --env-file ../secret/.env rm
	sudo docker compose -f ../docker/docker-compose-server.yml --env-file ../secret/.env up -d

server-rm:
	sudo docker compose -f ../docker/docker-compose-server.yml --env-file ../secret/.env stop
	sudo docker compose -f ../docker/docker-compose-server.yml --env-file ../secret/.env rm -f

server-logs:
	sudo docker logs vidconv-worker --tail 20
