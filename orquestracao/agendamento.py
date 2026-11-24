"""
Processo de longa duracao que mantem os agendamentos ativos -- nao precisa de
nenhum servidor Prefect separado rodando: a propria chamada abaixo (serve) sobe
um agendador local, guardando estado (historico de execucoes, retries) num
arquivo SQLite proprio do Prefect -- nao um servico de rede, so' um arquivo
local.

Dois horarios no MESMO processo:

- pipeline_completo_producao() -- de madrugada (ORQUESTRACAO_CRON, padrao 3h);
- aviso_prazo_comodato()       -- ao meio-dia (AVISO_PRAZO_CRON, padrao 12h).

Pra manter sempre ativo em producao, rodar este arquivo como servico do Windows
(NSSM), mesmo padrao dos apps Shiny. No servidor do projeto original a
madrugada acabou ficando com o Agendador de Tarefas do Windows chamando
iniciar.bat (1h), que roda flows.py uma vez -- as duas formas chamam o mesmo
flow.

    cd orquestracao
    python agendamento.py
"""

import os

from prefect import serve

from flows import aviso_prazo_comodato, pipeline_completo_producao

# ORQUESTRACAO_CRON vem do orquestracao/.env (carregado por flows.py, import
# acima) -- padrao "0 3 * * *" (3h da manha) se a variavel nao estiver definida.
CRON = os.getenv("ORQUESTRACAO_CRON") or "0 3 * * *"

# Aviso de prazo de comodato: agenda PROPRIA, ao meio-dia todos os dias. E'
# mensagem pra gente, nao geracao de dado: sai no meio do dia, na frente de quem
# esta' trabalhando, em vez de no fim da fila da madrugada.
CRON_AVISO_PRAZO = os.getenv("AVISO_PRAZO_CRON") or "0 12 * * *"

if __name__ == "__main__":
    # serve() com os agendamentos no MESMO processo -- em vez de um servico, um
    # SQLite de estado e um lugar pra olhar por agendamento. O .serve() de um
    # flow so' bloqueia o processo e nao deixaria os outros nem comecar.
    serve(
        pipeline_completo_producao.to_deployment(name="pipeline-completo-producao", cron=CRON),
        aviso_prazo_comodato.to_deployment(name="aviso-prazo-comodato", cron=CRON_AVISO_PRAZO),
    )
