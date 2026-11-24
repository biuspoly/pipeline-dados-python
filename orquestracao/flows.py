"""
Orquestracao dos pipelines de atualizacao (scripts/atualiza_*.py dos apps e da
camada bruta compartilhada em dados_brutos/) -- cada um continua podendo ser
rodado manualmente do jeito de sempre (nada muda nos scripts em si).

Dois flows:

- pipeline_completo_producao() -- o pipeline INTEIRO, na ordem de dependencia
  (camada bruta -> intermediaria -> .parquet final). E' o da madrugada.
- aviso_prazo_comodato() -- o aviso por e-mail de prazo de comodato, com agenda
  PROPRIA ao meio-dia (ver agendamento.py).

**Sequencial, nunca em paralelo entre scripts.** Esta pasta ja' passou por tres
formatos no projeto original: (1) Prefect com "camadas" paralelas via
`rodar_script.submit()` -- travou a maquina de teste real, rodando varios
pipelines pesados de ODBC ao mesmo tempo (4 scripts, cada um ja' abrindo uma
conexao por revenda, ~44 conexoes simultaneas); (2) um `.bat` puro, sem Prefect
nenhum; (3) Prefect de novo, mas 100% sequencial (esta versao) -- com os
proprios scripts corrigidos (troca por revenda em vez de recriar a tabela
inteira, gravacao em lotes, janela recente em vez de reprocessar todo o
historico), a suspeita confirmada foi que o travamento vinha do volume de cada
execucao rodando ao mesmo tempo, nao do Prefect. O unico paralelismo que ficou
e' o INTERNO de cada script, entre as revendas.

Rodar UMA vez, localmente, sem servidor nenhum (ver README):

    python orquestracao/flows.py --uma-vez          # pipeline completo, via Prefect
    python orquestracao/flows.py --uma-vez --aviso  # o aviso do meio-dia
    python orquestracao/flows.py --uma-vez --sem-prefect   # mesma ordem, sem Prefect

O Prefect 3 roda o flow em modo efemero (estado num SQLite local, em
PREFECT_HOME) -- nao precisa de `prefect server` no ar. "--sem-prefect" troca os
decoradores por versoes vazias: mesma ordem, mesma regra de dependencia, sem
retry.
"""

import argparse
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
from dotenv import load_dotenv

# Idioma/codepage do Windows varia por maquina -- sem isso, acentos podem sair
# como caractere errado ou quebrar com erro, tanto nos print() daqui quanto nas
# linhas de cada atualiza_*.py repassadas por rodar_script() abaixo.
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

RAIZ_PROJETO = Path(__file__).resolve().parent.parent
# O mesmo interpretador que esta' rodando este arquivo (no original: o .venv
# compartilhado do projeto, RAIZ_PROJETO / ".venv" / "Scripts" / "python.exe").
PYTHON_VENV = sys.executable

load_dotenv(Path(__file__).resolve().parent / ".env")

# Nivel de log do PROPRIO Prefect -- por padrao vem em INFO e polui o terminal
# com mensagens de framework ("Created task run...", "Finished in state
# Completed()"). WARNING deixa so' o que a gente escreve (print() abaixo) e erro
# de verdade. Precisa ser setado ANTES do "import prefect".
os.environ.setdefault("PREFECT_LOGGING_LEVEL", "WARNING")

USAR_PREFECT = "--sem-prefect" not in sys.argv
if USAR_PREFECT:
    from prefect import flow, get_run_logger, task  # depois do setdefault acima, de proposito
else:
    # Mesma assinatura dos decoradores do Prefect, sem fazer nada: o pipeline
    # roda como Python puro.
    import logging

    def task(*_args, **_kwargs):
        return lambda funcao: funcao

    def flow(*_args, **_kwargs):
        return lambda funcao: funcao

    def get_run_logger():
        return logging.getLogger("orquestracao")

# Politica de retry de cada task (rodar_script, abaixo) -- vem do
# orquestracao/.env, ver .env.example.
ORQUESTRACAO_RETRIES = int(os.getenv("ORQUESTRACAO_RETRIES") or "1")
ORQUESTRACAO_RETRY_DELAY_SEGUNDOS = int(os.getenv("ORQUESTRACAO_RETRY_DELAY_SEGUNDOS") or "300")

# Historico simples de execucoes (parquet), por fora do proprio banco do
# Prefect -- pra dar pra auditar quando/quanto demorou/deu certo sem precisar
# abrir a interface do Prefect. Nao vai pro Git.
PASTA_RESUMO = Path(__file__).parent / "parquet"
ARQUIVO_RESUMO = PASTA_RESUMO / "execucoes.parquet"


def _estimativa_segundos(pipeline: str, n_amostras: int = 5) -> float | None:
    """Media da duracao das ultimas "n_amostras" execucoes BEM-SUCEDIDAS desse
    pipeline, lida do proprio historico -- None se nunca rodou com sucesso.

    So' uma ETA -- nao pode travar o pipeline inteiro (isso ja' aconteceu de
    verdade: arquivo de historico corrompido derrubava o flow antes mesmo do 1o
    pipeline rodar). Historico ilegivel conta como "sem historico ainda"."""
    if not ARQUIVO_RESUMO.exists():
        return None
    try:
        historico = pl.read_parquet(ARQUIVO_RESUMO)
    except Exception as exc:
        print(f"Aviso: historico de execucoes ilegivel ({exc}) -- seguindo sem estimativa")
        return None
    duracoes = (
        historico.filter((pl.col("pipeline") == pipeline) & (pl.col("status") == "sucesso"))
        .sort("fim", descending=True)
        .head(n_amostras)["duracao_s"]
    )
    if duracoes.len() == 0:
        return None
    return float(duracoes.mean())


def _formata_duracao(segundos: float) -> str:
    minutos, seg = divmod(round(segundos), 60)
    if minutos == 0:
        return f"{seg}s"
    return f"{minutos}min{seg:02d}s"


def _registrar_execucao(pipeline: str, inicio: datetime, fim: datetime, status: str, detalhe: str) -> None:
    """Acrescenta uma linha ao historico de execucoes.

    Escrita ATOMICA (arquivo temporario + os.replace) -- bug real: escrever direto
    em cima do arquivo e interromper o processo no meio do write_parquet()
    deixava o historico corrompido pra sempre, e toda execucao seguinte quebrava
    ate' alguem apagar o arquivo a mao."""
    PASTA_RESUMO.mkdir(parents=True, exist_ok=True)
    linha = pl.DataFrame({
        "pipeline": [pipeline],
        "inicio": [inicio],
        "fim": [fim],
        "duracao_s": [round((fim - inicio).total_seconds(), 1)],
        "status": [status],
        "detalhe": [detalhe[-2000:]],  # so' o fim da saida/erro -- evita parquet crescer sem limite
    })
    if ARQUIVO_RESUMO.exists():
        linha = pl.concat([pl.read_parquet(ARQUIVO_RESUMO), linha], how="diagonal_relaxed")
    arquivo_tmp = ARQUIVO_RESUMO.with_suffix(".tmp")
    linha.write_parquet(arquivo_tmp)
    os.replace(arquivo_tmp, ARQUIVO_RESUMO)


@task(retries=ORQUESTRACAO_RETRIES, retry_delay_seconds=ORQUESTRACAO_RETRY_DELAY_SEGUNDOS)
def rodar_script(app_pasta: str, nome_script: str, argumentos: list[str] | None = None) -> None:
    """Roda um scripts/atualiza_*.py via subprocess -- sem precisar reescrever o
    script em funcao nenhuma pra encaixar no Prefect. Levanta excecao se o
    processo terminar com erro: aciona a nova tentativa automatica antes de
    marcar a task como falha de verdade.

    A saida e' lida LINHA A LINHA enquanto o processo roda, nao so' no final --
    sem isso quem olha o log so' via uma tela parada ate' o script terminar. "-u"
    desliga o buffer de saida do processo filho (com pipe, o Python bufferiza)."""
    logger = get_run_logger()
    caminho_app = RAIZ_PROJETO / app_pasta
    caminho_script = caminho_app / "scripts" / nome_script
    pipeline = f"{app_pasta}/{nome_script}"

    estimativa = _estimativa_segundos(pipeline)
    if estimativa is not None:
        print(f"{pipeline} iniciando (estimativa ~{_formata_duracao(estimativa)}, media das ultimas execucoes)")
    else:
        print(f"{pipeline} iniciando (sem historico ainda pra estimar)")

    inicio = datetime.now(timezone.utc)
    processo = subprocess.Popen(
        [str(PYTHON_VENV), "-u", str(caminho_script), *(argumentos or [])],
        cwd=caminho_app,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,  # junta os dois fluxos, na ordem real -- separado, um atraso no stderr
        text=True,                 # (ex: traceback so' no fim) faria parecer que travou antes da hora
        encoding="utf-8",
        # Byte fora de UTF-8 vira "?" no log, em vez de derrubar o passo.
        errors="replace",
        env={**os.environ, "PYTHONIOENCODING": "utf-8"},
        bufsize=1,
        # DETACHED_PROCESS (so' Windows): sem isso, o subprocesso fica anexado ao
        # MESMO console -- se a sessao cair (RDP desconectando), o Windows encerra
        # todo processo anexado, nao so' o orquestrador. Bug real: a camada bruta
        # (horas de execucao) morreu sozinha com STATUS_CONTROL_C_EXIT enquanto o
        # flow seguiu rodando.
        creationflags=getattr(subprocess, "DETACHED_PROCESS", 0),
    )
    linhas_saida: list[str] = []
    for linha in processo.stdout:
        linha = linha.rstrip("\n")
        linhas_saida.append(linha)
        if linha:
            print(f"  [{pipeline}] {linha}")
    processo.wait()
    fim = datetime.now(timezone.utc)
    saida_completa = "\n".join(linhas_saida)

    if processo.returncode == 0:
        print(f"{pipeline} OK em {(fim - inicio).total_seconds():.1f}s")
        _registrar_execucao(pipeline, inicio, fim, "sucesso", saida_completa)
    else:
        logger.error(f"{pipeline} falhou (codigo {processo.returncode})")
        _registrar_execucao(pipeline, inicio, fim, "erro", saida_completa)
        raise RuntimeError(f"{pipeline} terminou com codigo {processo.returncode}: {saida_completa[-500:]}")


# Ordem + dependencias de TODOS os pipelines -- 1 de cada vez, nunca em paralelo
# entre scripts. Cada item e' (pasta, script, [pipelines dos quais depende][,
# argumentos]) -- lista vazia = sem dependencia, roda sempre.
ORDEM_PIPELINE_COMPLETO: list[tuple] = [
    # 1. Camada BRUTA -- origem (ERP) -> bd_shiny_bruto. Nenhum depende de outro
    #    deste pipeline.
    ("dados_brutos", "atualiza_dados_brutos.py", []),
    ("dados_brutos", "atualiza_comodatos_bruto.py", []),
    # 2. Comercial -- dependem so' da camada bruta principal.
    ("Acompanhamento Comercial", "atualiza_pedidos_bonificados.py", ["dados_brutos/atualiza_dados_brutos.py"]),
    ("Acompanhamento Comercial", "atualiza_pedidos_cancelados.py", ["dados_brutos/atualiza_dados_brutos.py"]),
    # 3. Camada INTERMEDIARIA de Comodatos -- depende dos DOIS scripts da bruta.
    (
        "Acompanhamento Logistico",
        "atualiza_comodato_intermediario.py",
        ["dados_brutos/atualiza_dados_brutos.py", "dados_brutos/atualiza_comodatos_bruto.py"],
    ),
    # 4. .parquet FINAL de Comodatos -- depende da intermediaria.
    (
        "Acompanhamento Logistico",
        "atualiza_comodato_parquet.py",
        ["Acompanhamento Logistico/atualiza_comodato_intermediario.py"],
    ),
]


@flow(name="Pipeline completo - madrugada")
def pipeline_completo_producao() -> None:
    """Encadeia TODOS os pipelines na ordem de dependencia real entre eles, 1 de
    cada vez. Falha isolada num pipeline nao trava os outros -- so' pula quem
    depende diretamente dele (com a nova tentativa automatica antes)."""
    logger = get_run_logger()

    todos_pipelines = [f"{pasta}/{nome}" for pasta, nome, *_resto in ORDEM_PIPELINE_COMPLETO]
    estimativas = [_estimativa_segundos(p) for p in todos_pipelines]
    total = sum(e for e in estimativas if e is not None)
    if any(e is None for e in estimativas):
        print(f"ETA total (parcial, falta historico de algum pipeline): ~{_formata_duracao(total)}")
    else:
        print(f"ETA total: ~{_formata_duracao(total)}")

    sucesso: dict[str, bool] = {}
    for entrada in ORDEM_PIPELINE_COMPLETO:
        pasta, nome, dependencias = entrada[:3]
        argumentos = list(entrada[3]) if len(entrada) > 3 else []
        pipeline = f"{pasta}/{nome}"
        faltando = [dep for dep in dependencias if not sucesso.get(dep, False)]
        if faltando:
            motivo = f"depende de {', '.join(faltando)}, que falhou ou nao rodou"
            logger.error(f"{pipeline} pulado -- {motivo}")
            sucesso[pipeline] = False
            # Pulado tambem entra no historico: sem esta linha, o pipeline que nao
            # rodou por causa de uma dependencia ficava indistinguivel de "ainda
            # vai rodar" -- e e' justamente o caso mais comum de a tela amanhecer
            # com dado velho (a camada bruta falha e leva junto quem depende dela).
            agora = datetime.now(timezone.utc)
            _registrar_execucao(pipeline, agora, agora, "pulado", motivo)
            continue
        try:
            rodar_script(pasta, nome, argumentos)
            sucesso[pipeline] = True
        except Exception as exc:
            logger.error(f"{pipeline} falhou, seguindo para o proximo pipeline: {exc}")
            sucesso[pipeline] = False

    falhas = [p for p, ok in sucesso.items() if not ok]
    print(f"\nResumo: {len(sucesso) - len(falhas)}/{len(sucesso)} pipelines OK"
          f"{' -- com problema: ' + ', '.join(falhas) if falhas else ''}")


@flow(name="Aviso de prazo de comodato")
def aviso_prazo_comodato() -> None:
    """Avisa por e-mail os comodatos cujo prazo de documento esta' apertando.

    Agenda PROPRIA, ao MEIO-DIA todos os dias -- fora do pipeline da madrugada de
    proposito: o aviso e' mensagem pra gente, nao geracao de dado.

    Nao depende de nenhum passo do pipeline: le o cache que a tela le. Cache do
    dia anterior atrasa o dado, nunca dispara aviso errado -- o prazo e'
    recalculado na leitura (ver comodatos/dados_comodato.py::_expr_prazo_envio).
    """
    rodar_script("Acompanhamento Logistico", "avisa_prazo_comodato.py", ["--executar"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Orquestracao do pipeline de dados")
    parser.add_argument("--uma-vez", action="store_true", help="roda uma vez e sai (padrao quando chamado direto)")
    parser.add_argument("--aviso", action="store_true", help="roda o aviso de prazo de comodato em vez do pipeline")
    parser.add_argument("--sem-prefect", action="store_true", help="roda sem Prefect (mesma ordem, sem retry)")
    args = parser.parse_args()
    # Chamado direto, este arquivo sempre roda UMA vez (e' o que o iniciar.bat
    # do Agendador do Windows faz de madrugada); o agendamento continuo, com os
    # dois horarios, e' o agendamento.py.
    if args.aviso:
        aviso_prazo_comodato()
    else:
        pipeline_completo_producao()
