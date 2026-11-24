@echo off
REM Gera o aviso de prazo de comodato fora do horario do agendamento (que roda
REM sozinho todo dia ao meio-dia -- ver orquestracao/agendamento.py,
REM AVISO_PRAZO_CRON).
REM
REM DEMONSTRACAO: nao envia nada -- grava o e-mail que sairia em
REM saida\emails\<revenda>.eml e mostra o texto no log. Um e-mail por revenda,
REM com os comodatos que entraram na faixa amarela (6 a 3 dias) ou vermelha
REM (2 a 0, negativo, "Esgotado"). Quem recebe esta' em
REM config\avisos_prazo_comodato.json (enderecos ficticios).
REM
REM   avisar_prazo_comodato.bat                     -> todas as revendas
REM   avisar_prazo_comodato.bat --revenda Aurora    -> so uma revenda
REM   avisar_prazo_comodato.bat --reenviar          -> ignora o historico
REM
REM O mesmo comodato nunca e' avisado duas vezes na mesma faixa (controle em
REM bd_comodato.tb_comod_aviso_prazo): rodar duas vezes seguidas nao repete.

setlocal
cd /d "%~dp0"
set "PY=%~dp0.venv\Scripts\python.exe"
if not exist "%PY%" set "PY=python"
set "LOG=%~dp0logs\avisar_prazo_comodato.log"

if not exist "%~dp0logs" mkdir "%~dp0logs"
> "%LOG%" echo === avisar_prazo_comodato.bat -- inicio: %date% %time% ===

echo Log detalhado: "%LOG%"
pushd "%~dp0Acompanhamento Logistico"
REM %* repassa o que veio na linha de comando (--revenda, --reenviar...).
"%PY%" "scripts\avisa_prazo_comodato.py" --executar %* >> "%LOG%" 2>&1
REM O errorlevel e' guardado ANTES do popd -- popd zera o codigo de erro.
set "ERRO=%errorlevel%"
popd

if not "%ERRO%"=="0" (
    echo CONCLUIDO COM ERRO -- veja o motivo em "%LOG%"
    endlocal
    exit /b 1
)
for /f "usebackq delims=" %%L in (`findstr /c:"Resumo:" "%LOG%"`) do echo   %%L
echo CONCLUIDO
endlocal
exit /b 0
