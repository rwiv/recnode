cd ..

for /f "delims=" %%A in ('powershell -Command "[guid]::NewGuid().ToString()"') do set "uuid=%%A"
for /f "tokens=1 delims=-" %%B in ("%uuid%") do set "firstpart=%%B"

helm install "stdl-%firstpart%" ./chart -f ./dev/values.yaml
pause