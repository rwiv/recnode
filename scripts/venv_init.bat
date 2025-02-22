cd ..
python -m venv .venv
.venv\Scripts\pip.exe install -r requirements.txt
.venv\Scripts\pip.exe install -r requirements-dev.txt
.venv\Scripts\pip.exe install git+https://github.com/rwiv/pyutils.git@v0.1.3
pause