@echo off
REM Gera SO os arquivos .parquet finais (Comercial e Logistico).
REM
REM - Nao roda a camada bruta (dados_brutos/) nem a intermediaria: no servidor
REM   elas ja' sao atualizadas pelo agendamento da madrugada.
REM - Nao grava nada no "MySQL": SOMENTE_PARQUET=1 desliga a gravacao em
REM   core/parquet.py::sobrescreve_mysql, unico ponto por onde todos os
REM   scripts abaixo gravam no banco.
REM
REM Um script que falha nao impede os seguintes -- o resumo no final lista
REM quais deram erro. Saida detalhada vai pro log (logs\gerar_parquet.log,
REM sobrescrito a cada execucao); na tela fica so o andamento.

setlocal
cd /d "%~dp0"
set "SOMENTE_PARQUET=1"
set "PY=%~dp0.venv\Scripts\python.exe"
if not exist "%PY%" set "PY=python"
set "LOG=%~dp0logs\gerar_parquet.log"
set "FALHAS="

if not exist "%~dp0logs" mkdir "%~dp0logs"
> "%LOG%" echo === gerar_parquet.bat -- inicio: %date% %time% ===

echo Log detalhado: "%LOG%"
echo.
echo === Acompanhamento Comercial ===
call :roda "Acompanhamento Comercial" atualiza_pedidos_bonificados.py
call :roda "Acompanhamento Comercial" atualiza_pedidos_cancelados.py

echo.
echo === Acompanhamento Logistico ===
call :roda "Acompanhamento Logistico" atualiza_comodato_parquet.py

echo.
if defined FALHAS (
    >> "%LOG%" echo === fim: %date% %time% -- ERRO em:%FALHAS% ===
    echo CONCLUIDO COM ERRO em:%FALHAS%
    echo Veja o motivo em "%LOG%"
    endlocal
    exit /b 1
)
>> "%LOG%" echo === fim: %date% %time% -- todos os parquet gerados ===
echo CONCLUIDO -- todos os parquet foram gerados
endlocal
exit /b 0

:roda
echo   %~2 ...
>> "%LOG%" echo.
>> "%LOG%" echo ---------- %~2 (%time%) ----------
pushd "%~dp0%~1"
"%PY%" "scripts\%~2" >> "%LOG%" 2>&1
if errorlevel 1 (
    set "FALHAS=%FALHAS% %~2"
    echo     FALHOU -- ver "%LOG%"
)
popd
exit /b 0
