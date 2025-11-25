cd ..
set IMG=harbor.rwiv.xyz/private/recnode:1.0.5
set DOCKERFILE=./docker/Dockerfile

docker build -t %IMG% -f %DOCKERFILE% .
docker push %IMG%

docker rmi %IMG%
pause