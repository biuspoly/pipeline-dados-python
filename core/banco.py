"""Os "bancos" da demonstracao -- ERP e MySQL trocados por Parquet.

No projeto original:

- a ORIGEM e' o ERP de cada revenda, lido por ODBC (pyodbc, um DSN por revenda);
- a camada bruta e a intermediaria moram no MySQL (bancos `bd_shiny_bruto`,
  `bd_comodato`, `bd_dlp`), gravadas/lidas com SQLAlchemy + pymysql.

Aqui, para o repositorio rodar sem servidor nenhum:

- a ORIGEM vira `dados/origem/erp/<revenda>/<consulta>.parquet`, um arquivo por
  consulta, com as MESMAS colunas que a consulta ao ERP devolve (gerados por
  `dados/gerar_dados_ficticios.py`);
- cada banco MySQL vira uma pasta `dados/bancos/<banco>/`, uma tabela por
  arquivo `.parquet`;
- as consultas SQL que os scripts faziam no MySQL continuam sendo SQL, so' que
  executadas pelo DuckDB sobre esses arquivos (`consulta()` abaixo registra cada
  arquivo da pasta como uma view com o nome da tabela). DuckDB nao diferencia
  caixa em nome de tabela/coluna, como o MySQL -- `TB_COMODATO` e `tb_comodato`
  sao a mesma coisa nos dois.
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb
import polars as pl

from . import revendas

RAIZ = Path(__file__).resolve().parent.parent
PASTA_ORIGEM = RAIZ / "dados" / "origem"
PASTA_BANCOS = RAIZ / "dados" / "bancos"

# Tabelas que NENHUM pipeline grava -- no original, quem escreve nelas e' a
# propria tela (tb_anexos_cev: anexos do comodato) ou sao cadastro fixo. Na
# demonstracao vem prontas de dados/origem/telas/.
PASTAS_EXTRAS = {"bd_comodato": [PASTA_ORIGEM / "telas"]}


def pasta_banco(banco: str) -> Path:
    return PASTA_BANCOS / banco


# ==========================================================================
# Origem (ERP)
# ==========================================================================


def consulta_erp(empresa: dict, nome_consulta: str) -> pl.DataFrame:
    """Faz o papel de `consulta_odbc(conexao, sql)`: devolve o resultado daquela
    consulta para aquela revenda. Revenda sem a pasta = ERP fora do ar (levanta,
    como o pyodbc.connect levantaria) -- e' o que deixa testar a regra "revenda
    que falhou nao some da tabela" (ver atualiza_tabela_por_cnpj)."""
    pasta = PASTA_ORIGEM / "erp" / empresa["slug"]
    if not pasta.exists():
        raise ConnectionError(f"fonte ERP de {empresa['nome']} indisponivel ({pasta.name}/ nao existe)")
    caminho = pasta / f"{nome_consulta}.parquet"
    if not caminho.exists():
        return pl.DataFrame()
    return pl.read_parquet(caminho)


def consulta_api(nome: str) -> pl.DataFrame:
    """Faz o papel de uma chamada HTTP a uma API externa (no original, uma Edge
    Function com chave de API) -- aqui, o registro "devolvido" esta em
    dados/origem/api/<nome>.parquet."""
    return pl.read_parquet(PASTA_ORIGEM / "api" / f"{nome}.parquet")


# ==========================================================================
# Bancos (MySQL -> pasta de Parquet)
# ==========================================================================


def existe_tabela(banco: str, nome: str) -> bool:
    return (pasta_banco(banco) / f"{nome.lower()}.parquet").exists()


def ler_tabela(banco: str, nome: str) -> pl.DataFrame:
    caminho = pasta_banco(banco) / f"{nome.lower()}.parquet"
    if not caminho.exists():
        return pl.DataFrame()
    return pl.read_parquet(caminho)


def grava_tabela(banco: str, nome: str, tabela: pl.DataFrame) -> None:
    """Regrava a tabela inteira -- escrita ATOMICA (arquivo temporario + troca de
    nome), mesmo cuidado do historico de execucoes em orquestracao/flows.py: uma
    interrupcao no meio deixa so' o .tmp, nunca a tabela pela metade."""
    pasta = pasta_banco(banco)
    pasta.mkdir(parents=True, exist_ok=True)
    caminho = pasta / f"{nome.lower()}.parquet"
    temporario = caminho.with_suffix(".tmp")
    tabela.write_parquet(temporario)
    os.replace(temporario, caminho)


def _tb_empresas() -> pl.DataFrame:
    """TB_EMPRESAS (CNPJ_EMP, EMPRESA) -- cadastro fixo em bd_dlp/bd_comodato no
    original; aqui sai da tabela unica de revendas."""
    return pl.DataFrame(
        {
            "CNPJ_EMP": [r.cnpj for r in revendas.REVENDAS],
            "EMPRESA": [revendas.nome_empresa(r) for r in revendas.REVENDAS],
        }
    )


def consulta(banco: str, sql: str) -> pl.DataFrame:
    """Equivalente a `pl.read_database(sql, engine_do_banco)`: roda o SQL no
    DuckDB com uma view por tabela do banco."""
    con = duckdb.connect(":memory:")
    try:
        pastas = [pasta_banco(banco), *PASTAS_EXTRAS.get(banco, [])]
        for pasta in pastas:
            if not pasta.exists():
                continue
            for arquivo in sorted(pasta.glob("*.parquet")):
                con.execute(
                    f'CREATE OR REPLACE VIEW "{arquivo.stem}" AS SELECT * FROM read_parquet(\'{arquivo.as_posix()}\')'
                )
        con.register("tb_empresas", _tb_empresas().to_arrow())
        return con.execute(sql).pl()
    finally:
        con.close()
