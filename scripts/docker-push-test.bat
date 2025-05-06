cd ..
set IMG=harbor.rwiv.xyz/test/stdl:latest
set DOCKERFILE=./docker/Dockerfile

docker build -t %IMG% -f %DOCKERFILE% .
docker push %IMG%

docker rmi %IMG%
pause