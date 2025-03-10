@echo off
REM Navigate to the Python project directory
cd /d "C:\Users\pgorade\PycharmProjects\airflow_lab"
if %errorlevel% neq 0 (
    echo Failed to navigate to Python project directory.
    exit /b
)

REM Activate the virtual environment (if applicable)
call .venv\Scripts\activate
if %errorlevel% neq 0 (
    echo Failed to activate the virtual environment.
    exit /b
)

REM Run the Python script in a new process (daemon mode)
start "" python -m main
if %errorlevel% neq 0 (
    echo Failed to start the Python script in daemon mode.
    exit /b
)


@REM @echo off
@REM start cmd /k "C:\Users\pgorade\kubectl.exe config use-context p-lightops-20611-2-az-weu-2 && C:\Users\pgorade\kubectl.exe port-forward svc/airflow-app-web 8100:8080 -n prod-sdfs-canary"


@echo off
REM Launch Internet Explorer and open localhost:4200
start "" "C:\Program Files\Google\Chrome\Application\chrome.exe" http://localhost:8200/docs

echo All tasks completed successfully!
