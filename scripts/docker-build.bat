cd ..
set IMG=stdl:latest
set DOCKERFILE=./docker/Dockerfile-dev

docker rmi %IMG%
docker build -t %IMG% -f %DOCKERFILE% .
pause
