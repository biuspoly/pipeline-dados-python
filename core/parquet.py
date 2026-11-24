"""Ponto UNICO de gravacao dos scripts de atualizacao -- `.parquet` final (o que
os apps Shiny leem) e "MySQL" (a camada que outros scripts leem depois).

No original, cada app tem uma copia deste arquivo em `core/parquet.py`, e
`sobrescreve_mysql` grava de verdade no MySQL. Aqui ele grava no banco emulado
(`core/banco.py`, uma pasta de Parquet por banco) -- a assinatura e as regras
(tabela vazia nao apaga nada; SOMENTE_PARQUET desliga) sao as mesmas.
"""

import json
import os
import threading
from datetime import datetime
from pathlib import Path

import duckdb
import polars as pl

from . import banco as bancos

NOME_ARQUIVO_METADADOS = "_atualizacoes.json"
_trava = threading.Lock()


def somente_parquet() -> bool:
    """SOMENTE_PARQUET=1 desliga toda gravacao no MySQL dos scripts de
    atualizacao, deixando so' o .parquet -- usado por gerar_parquet.bat (raiz
    do projeto)."""
    return os.getenv("SOMENTE_PARQUET", "").strip() in ("1", "true", "True")


def registra_atualizacao(pasta: Path, nome: str) -> None:
    """Card "Atualizado em" do cabecalho dos apps: o momento vem de quem GRAVOU
    o cache, no instante em que a tabela foi escrita -- deliberadamente NAO e' a
    data de modificacao do arquivo, que poderia mudar sem os dados terem sido
    atualizados de verdade (ex: uma copia/restauracao do arquivo fora do
    pipeline)."""
    caminho = pasta / NOME_ARQUIVO_METADADOS
    with _trava:
        dados = {}
        if caminho.exists():
            try:
                dados = json.loads(caminho.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                dados = {}
        dados[nome] = datetime.now().isoformat(timespec="seconds")
        caminho.write_text(json.dumps(dados, ensure_ascii=False, indent=2), encoding="utf-8")


def sobrescreve_parquet(nome: str, tabela: pl.DataFrame, pasta: Path) -> None:
    # Equivalente a parte de parquet do sobrescreve_tabela() do app em R -- usa
    # o DuckDB so como "gravador": registra o DataFrame como uma view temporaria
    # e copia ela pra um arquivo .parquet.
    pasta.mkdir(parents=True, exist_ok=True)
    caminho = pasta / f"{nome}.parquet"

    con = duckdb.connect(":memory:")
    con.register("tabela_temp", tabela.to_arrow())
    con.execute(f"COPY tabela_temp TO '{caminho.as_posix()}' (FORMAT PARQUET)")
    con.unregister("tabela_temp")
    con.close()

    # Timestamp companheiro do card "Atualizado em" -- gravado aqui, no unico
    # ponto por onde todo pipeline de atualizacao passa, pra nao precisar
    # instrumentar cada scripts/atualiza_*.py individualmente.
    registra_atualizacao(pasta, nome)

    print(f"  -> {nome}: {tabela.height} linhas gravadas em {caminho.relative_to(bancos.RAIZ).as_posix()}")


def sobrescreve_mysql(
    nome: str,
    tabela: pl.DataFrame,
    banco: str,
    indices: list[str] | None = None,
    tipos: dict[str, str] | None = None,
) -> None:
    # Equivalente a sobrescreve_tabela() do R: dropa a tabela se existir, recria
    # com o schema da tabela nova e insere os dados. Tabela vazia nao mexe na
    # tabela existente (mesma guarda do R: "if (nrow(df) == 0)
    # return(invisible(NULL))"), pra uma consulta sem resultado nao apagar dados
    # validos que ja estavam la.
    #
    # "indices"/"tipos": no MySQL, write_database cria toda coluna de texto como
    # TEXT, que so aceita indice por PREFIXO -- o original converte as colunas de
    # join pra VARCHAR e recria os indices a cada execucao (medido: consulta de
    # Comodatos de 56s pra 41s). Sem MySQL, sao aceitos e ignorados aqui; ficam
    # na chamada pra documentar o que o banco de verdade recebe.
    if tabela.height == 0:
        return

    if somente_parquet():
        print(f"  -> {nome}: MySQL ignorado (SOMENTE_PARQUET)")
        return

    bancos.grava_tabela(banco, nome, tabela)
    print(f"  -> {nome}: {tabela.height} linhas gravadas no banco {banco}")
