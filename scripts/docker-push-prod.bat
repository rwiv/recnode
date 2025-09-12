cd ..
set IMG=harbor.rwiv.xyz/private/stdl:1.0.1
set DOCKERFILE=./docker/Dockerfile

docker build -t %IMG% -f %DOCKERFILE% .
docker push %IMG%

docker rmi %IMG%
pause