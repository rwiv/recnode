cd ..
set IMG=harbor.rwiv.xyz/private/stdl:0.6.5
set DOCKERFILE=./docker/Dockerfile

docker buildx build --platform linux/amd64,linux/arm64 -t %IMG% -f %DOCKERFILE% --push .

docker stop buildx_buildkit_multi-arch-builder0
docker rm buildx_buildkit_multi-arch-builder0
pause