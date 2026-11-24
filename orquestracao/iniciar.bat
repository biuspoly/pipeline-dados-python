@echo off
REM Roda o pipeline completo UMA vez -- e' o que o Agendador de Tarefas do
REM Windows chama de madrugada (1h) no servidor. Log com data/hora no nome.
cd /d "%~dp0"
if not exist logs mkdir logs

REM Carimbo de data/hora do nome do log. Era "wmic", que o Windows 11 removeu
REM (build 26100+) -- sem ele o nome saia invalido e o log inteiro se perdia.
REM PowerShell nao depende do idioma da maquina, diferente de %DATE%/%TIME%.
for /f "delims=" %%I in ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-dd_HH-mm-ss"') do set "timestamp=%%I"
REM Rede de seguranca: nome invalido faz o Add-Content de baixo falhar linha a
REM linha, sem derrubar a execucao -- ou seja, perde o log em silencio.
if not defined timestamp set "timestamp=sem-data"

REM Python do ambiente virtual da raiz do repositorio (.venv), se existir.
set "PY=%~dp0..\.venv\Scripts\python.exe"
if not exist "%PY%" set "PY=python"

"%PY%" -u flows.py --uma-vez 2>&1 | powershell -NoProfile -Command "$input | ForEach-Object { Write-Output $_; Add-Content -Path 'logs\execucao_%timestamp%.log' -Value $_ -Encoding utf8 }"
