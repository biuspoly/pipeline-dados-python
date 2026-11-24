"""Camada bruta -- titulos em aberto, comodatos pendentes (CEV), estoque e o
livro fiscal de entrada/saida (devolucao/emprestimo de vasilhame) de cada
revenda, direto do ERP. Grava em "bd_shiny_bruto" -- mesmo banco de
atualiza_dados_brutos.py, mas sem depender dele (tabelas diferentes). E' a base
que atualiza_comodato_intermediario.py (Acompanhamento Logistico/scripts/) le
pra montar a camada intermediaria de Comodatos.

Mesmo padrao de paralelismo e protecao do atualiza_dados_brutos.py
(ThreadPoolExecutor, uma thread por revenda, prazo total de espera).

DEMONSTRACAO: o ERP e' `dados/origem/erp/<revenda>/<consulta>.parquet` (as
colunas sao as da consulta em consultas_erp.py) e o MySQL e' a pasta
`dados/bancos/bd_shiny_bruto/`.
"""

import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path

import polars as pl

RAIZ = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(RAIZ))

from core import banco as bancos  # noqa: E402
from core import revendas  # noqa: E402
from gravacao_bruta import (  # noqa: E402
    atualiza_tabela_janela_recente,
    atualiza_tabela_por_cnpj,
    meses_existentes_local,
    meses_recentes,
)

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

EMPRESAS = revendas.para_erp(campos=("cnpj", "nome", "slug", "licenca"))

DATA_INICIO_LIVRO = date(2025, 1, 1)
# Piso do CEV pendente (AAAAMM do NPED) -- vai no WHERE da consulta ao ERP.
MES_INICIO_CEV = 202001
TIMEOUT_TOTAL_SEGUNDOS = 20 * 60


def processar_empresa(empresa: dict) -> dict[str, pl.DataFrame] | None:
    nome = empresa["nome"]

    try:
        titulos_abertos = bancos.consulta_erp(empresa, "titulos_abertos")
        print(f"  Conectou em {nome}")

        cev = bancos.consulta_erp(empresa, "cev")
        if cev.height:
            cev = cev.filter(pl.col("NPED").str.slice(0, 6).cast(pl.Int64, strict=False) >= MES_INICIO_CEV)

        # Estoque -- no original pula domingo/feriado e, sem lancamento no dia
        # de referencia, tenta ate' 2 dias uteis antes (ver consultas_erp.py).
        estoque = bancos.consulta_erp(empresa, "estoque")

        # Livro fiscal de entrada/saida -- mes a mes, desde DATA_INICIO_LIVRO.
        # Filtro dinamico: pergunta a base LOCAL o que ja' existe pra essa
        # revenda antes de perguntar ao ERP -- pula qualquer mes ja' presente
        # (exceto mes atual/passado, sempre re-buscados).
        piso_livro = DATA_INICIO_LIVRO.strftime("%Y%m")
        meses_recentes_set = set(meses_recentes(1))
        livros = {}
        for chave, tabela_local in (("livro_entrada", "tb_livro_entrada_cev"), ("livro_saida", "tb_livro_saida_cev")):
            existentes = meses_existentes_local(tabela_local, empresa["cnpj"], piso_livro)
            livro = bancos.consulta_erp(empresa, chave)
            if livro.height:
                livro = livro.with_columns(pl.col("DATA").cast(pl.Utf8).str.slice(0, 6).alias("MES"))
                pulados = livro.filter(pl.col("MES").is_in(list(existentes - meses_recentes_set)))["MES"].n_unique()
                livro = livro.filter(
                    (pl.col("MES") >= piso_livro) & ~pl.col("MES").is_in(list(existentes - meses_recentes_set))
                )
                if pulados:
                    print(f"  {nome}: {pulados} mes(es) de {chave} ja existiam localmente, pulados")
            livros[chave] = livro

        tabelas = {
            "titulos_abertos": titulos_abertos,
            "cev": cev,
            "livro_entrada": livros["livro_entrada"],
            "livro_saida": livros["livro_saida"],
            "estoque": estoque,
        }
        for chave, df in tabelas.items():
            if df.height > 0:
                tabelas[chave] = df.with_columns(
                    pl.lit(empresa["cnpj"]).alias("CNPJ_REV"), pl.lit(empresa["licenca"]).alias("LICENCA")
                )

        print(f"  Concluiu {nome}: {tabelas['titulos_abertos'].height} titulos, {tabelas['cev'].height} cev")
        return tabelas

    except Exception as exc:
        print(f"  ERRO na empresa {nome}: {exc}")
        return None


def main() -> None:
    resultados: list[dict[str, pl.DataFrame]] = []
    cnpjs_ok: list[str] = []  # revendas cuja consulta terminou de verdade nesta execucao
    executor = ThreadPoolExecutor(max_workers=len(EMPRESAS))
    futuros = {executor.submit(processar_empresa, empresa): empresa for empresa in EMPRESAS}
    try:
        for futuro in as_completed(futuros, timeout=TIMEOUT_TOTAL_SEGUNDOS):
            empresa = futuros[futuro]
            resultado = futuro.result()
            if resultado is not None:
                resultados.append(resultado)
                cnpjs_ok.append(empresa["cnpj"])
    except TimeoutError:
        pendentes = [empresa["nome"] for f, empresa in futuros.items() if not f.done()]
        print(f"Aviso: {TIMEOUT_TOTAL_SEGUNDOS}s esgotados esperando resposta de: {', '.join(pendentes)} "
              "-- seguindo so com quem ja respondeu.")
    executor.shutdown(wait=False, cancel_futures=True)

    if not resultados:
        print("Nenhuma revenda conectou -- nada a gravar.")
        return

    def junta(chave: str) -> pl.DataFrame:
        partes = [r[chave] for r in resultados if r[chave].height > 0]
        if not partes:
            return pl.DataFrame()
        return pl.concat(partes, how="diagonal_relaxed")

    atualiza_tabela_por_cnpj("tb_titulos_abertos", cnpjs_ok, junta("titulos_abertos"))
    atualiza_tabela_por_cnpj("tb_cev", cnpjs_ok, junta("cev"))
    # Livro fiscal: mesma regra de "so' mes atual/passado + mes faltando" das
    # tabelas de alto volume -- corte FIXO em DATA_INICIO_LIVRO, nao rolante
    # (a consulta ao ERP sempre busca de 2025-01 pra frente).
    corte_livro = DATA_INICIO_LIVRO.strftime("%Y%m")
    atualiza_tabela_janela_recente(
        "tb_livro_entrada_cev", cnpjs_ok, junta("livro_entrada"), "MES", corte_fixo=corte_livro
    )
    atualiza_tabela_janela_recente(
        "tb_livro_saida_cev", cnpjs_ok, junta("livro_saida"), "MES", corte_fixo=corte_livro
    )
    atualiza_tabela_por_cnpj("tb_estoque_cev", cnpjs_ok, junta("estoque"))

    print("atualiza_comodatos_bruto.py finalizado.")


if __name__ == "__main__":
    main()
