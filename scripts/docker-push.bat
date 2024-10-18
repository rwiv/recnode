cd ..
set IMG=harbor.rwiv.xyz/test/stdl:0.1.6
set DOCKERFILE=./docker/Dockerfile

docker rmi %IMG%

docker build -t %IMG% -f %DOCKERFILE% .
docker push %IMG%

docker rmi %IMG%
pause
