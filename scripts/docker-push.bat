cd ..
set IMG1=stdl:latest
set IMG2=ghcr.io/rwiv/stdl:latest
set DOCKERFILE=./docker/Dockerfile

docker rmi %IMG1%
docker rmi %IMG2%

docker build -t %IMG1% -f %DOCKERFILE% .

docker tag %IMG1% %IMG2%
docker push %IMG2%

docker rmi %IMG1%
docker rmi %IMG2%
pause