"""Como a camada bruta grava -- as duas regras que nasceram de bug real no
projeto original, agora sobre o banco emulado (core/banco.py).

1. **Troca por revenda** (`atualiza_tabela_por_cnpj`): so' mexe nas linhas das
   revendas que responderam NESTA execucao. Antes, cada tabela era dropada e
   recriada so' com quem respondeu a tempo -- uma revenda lenta ou fora do ar
   simplesmente desaparecia da tabela, mesmo sem erro nenhum no dado que ela ja'
   tinha de uma execucao anterior.
2. **Janela recente** (`atualiza_tabela_janela_recente`): as tabelas de alto
   volume (pedidos, itens, livro fiscal) nao sao regravadas inteiras todo dia --
   so' o mes atual/os ultimos meses MAIS qualquer mes que a origem trouxe e a
   base local ainda nao tem (buraco no historico). A poda do que passou da
   retencao (`podar_tabela_antiga`) e' separada.

No original estas funcoes estao duplicadas em atualiza_dados_brutos.py e
atualiza_comodatos_bruto.py e fazem DELETE+INSERT numa transacao so' (com
tabela de passagem) no MySQL. Aqui, ler + filtrar + concatenar + regravar o
arquivo atomicamente (core/banco.py::grava_tabela) tem o mesmo efeito.
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import polars as pl
from dateutil.relativedelta import relativedelta

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from core import banco as bancos  # noqa: E402

BANCO = "bd_shiny_bruto"


def _alinha_colunas(nome_tabela: str, existente: pl.DataFrame, nova: pl.DataFrame) -> pl.DataFrame:
    """Schema evolutivo: coluna nova na consulta entra (nula nas linhas antigas);
    coluna que saiu da consulta sai da tabela, pra nao ficar orfa (sempre nula).
    Comparacao SEM diferenciar caixa -- bug real no original: "operacao" (do
    banco) x "OPERACAO" (do DataFrame) eram tratadas como colunas diferentes e o
    MySQL recusava o ADD COLUMN com "Duplicate column name"."""
    novas_lower = {c.lower(): c for c in nova.columns}
    renomear, orfas = {}, []
    for coluna in existente.columns:
        if coluna.lower() not in novas_lower:
            orfas.append(coluna)
        elif novas_lower[coluna.lower()] != coluna:
            renomear[coluna] = novas_lower[coluna.lower()]
    for coluna in orfas:
        print(f"  -> {nome_tabela}: coluna {coluna} removida (nao existe mais na consulta)")
    existentes_lower = {c.lower() for c in existente.columns}
    for coluna in nova.columns:
        if coluna.lower() not in existentes_lower:
            print(f"  -> {nome_tabela}: coluna {coluna} adicionada")
    return existente.drop(orfas).rename(renomear)


def _substitui_linhas(nome_tabela: str, tabela: pl.DataFrame, manter: pl.Expr) -> None:
    """Fica com as linhas existentes que casam com `manter` e acrescenta as de
    `tabela` -- a troca e' gravada de uma vez (arquivo temporario + rename), entao
    um erro no meio nunca deixa a revenda apagada e nao reinserida (bug real do
    original, em que o DELETE era confirmado antes do INSERT)."""
    existente = bancos.ler_tabela(BANCO, nome_tabela)
    if existente.height or existente.width:
        existente = _alinha_colunas(nome_tabela, existente, tabela).filter(manter)
        tabela = pl.concat([existente, tabela], how="diagonal_relaxed")
    bancos.grava_tabela(BANCO, nome_tabela, tabela)


def atualiza_tabela_por_cnpj(nome_tabela: str, cnpjs_ok: list[str], tabela: pl.DataFrame) -> None:
    """So' mexe nas linhas das revendas que conectaram NESTA execucao (cnpjs_ok)
    -- revenda que falhou mantem o dado da execucao anterior em vez de sumir."""
    if not cnpjs_ok:
        print(f"  -> {nome_tabela}: nenhuma revenda conectou nesta execucao, tabela mantida como estava")
        return
    if tabela.height == 0 and not bancos.existe_tabela(BANCO, nome_tabela):
        print(f"  -> {nome_tabela}: tabela ainda nao existe e nao ha dados para cria-la")
        return
    _substitui_linhas(nome_tabela, tabela, ~pl.col("CNPJ_REV").is_in(cnpjs_ok))
    print(f"  -> {nome_tabela}: {tabela.height} linhas gravadas ({len(cnpjs_ok)} revendas conectadas)")


def meses_recentes(meses_atras: int = 1) -> list[str]:
    """['AAAAMM' do mes atual, ... ate' `meses_atras` meses antes]."""
    inicio = date.today().replace(day=1)
    return [(inicio - relativedelta(months=n)).strftime("%Y%m") for n in range(meses_atras + 1)]


def meses_existentes_local(nome_tabela: str, cnpj: str, piso: str, coluna_mes: str = "MES") -> set[str]:
    """Antes de perguntar a' origem (ODBC, lento) por um mes, olha o que ja'
    existe na base LOCAL pra essa revenda. Tabela ainda nao existente ou erro --
    devolve vazio (sem otimizacao, busca tudo: nunca incorreto)."""
    try:
        local = bancos.ler_tabela(BANCO, nome_tabela)
        if local.height == 0:
            return set()
        return set(
            local.filter((pl.col("CNPJ_REV") == cnpj) & (pl.col(coluna_mes) >= piso))[coluna_mes]
            .drop_nulls().unique().to_list()
        )
    except Exception:
        return set()


def atualiza_tabela_janela_recente(
    nome_tabela: str,
    cnpjs_ok: list[str],
    tabela: pl.DataFrame,
    coluna_mes: str,
    *,
    meses_atras: int = 1,
    corte_fixo: str,
) -> None:
    """Substitui so' (revenda que respondeu) x (mes da janela recente OU mes que a
    origem trouxe e a base local nao tem). O resto do historico fica intacto.

    Preco conhecido e aceito: correcao retroativa feita no ERP num mes mais antigo
    que a janela nao chega mais sozinha a' base local.

    O "OR mes IS NULL" do DELETE original continua aqui: linha antiga sem mes
    preenchido daquela revenda e' trocada junto, em vez de ficar duplicada."""
    if not cnpjs_ok or tabela.height == 0:
        print(f"  -> {nome_tabela}: nenhuma revenda conectou nesta execucao, tabela mantida como estava")
        return
    if not bancos.existe_tabela(BANCO, nome_tabela):
        bancos.grava_tabela(BANCO, nome_tabela, tabela)
        print(f"  -> {nome_tabela}: {tabela.height} linhas gravadas (tabela nao existia, gravou tudo)")
        return

    fonte = tabela.filter(pl.col("CNPJ_REV").is_in(cnpjs_ok) & (pl.col(coluna_mes) >= corte_fixo))
    local = bancos.ler_tabela(BANCO, nome_tabela)
    combos_fonte = fonte.select("CNPJ_REV", coluna_mes).unique()
    combos_destino = local.filter(pl.col("CNPJ_REV").is_in(cnpjs_ok)).select("CNPJ_REV", coluna_mes).unique()
    # Tabela sem nenhum mes preenchido ainda -- tudo da fonte conta como faltando
    # (no original, o join quebrava com SchemaError: a consulta vazia vinha com
    # as colunas como Null).
    if combos_destino.height == 0:
        faltando = combos_fonte
    else:
        faltando = combos_fonte.join(combos_destino, on=["CNPJ_REV", coluna_mes], how="anti")
    meses_faltando = set(faltando[coluna_mes].unique().to_list()) if faltando.height > 0 else set()
    if meses_faltando:
        print(f"  -> {nome_tabela}: sem dado local pro(s) mes(es) {sorted(meses_faltando)} -- buscando da revenda tambem")

    meses_janela = set(meses_recentes(meses_atras)) | meses_faltando
    janela = fonte.filter(pl.col(coluna_mes).is_in(list(meses_janela)))
    if janela.height == 0:
        print(f"  -> {nome_tabela}: nada na janela recente (nem mes faltando) pra gravar")
        return

    trocar = pl.col("CNPJ_REV").is_in(cnpjs_ok) & (
        pl.col(coluna_mes).is_in(list(meses_janela)) | pl.col(coluna_mes).is_null()
    )
    _substitui_linhas(nome_tabela, janela, ~trocar)
    print(
        f"  -> {nome_tabela}: {janela.height} linhas gravadas "
        f"({len(meses_janela)} mes(es): {sorted(meses_janela)}, {len(cnpjs_ok)} revendas)"
    )


def podar_tabela_antiga(nome_tabela: str, coluna_mes: str, *, meses_retencao: int) -> None:
    """Retencao de verdade, separada da janela: apaga o que passou de
    `meses_retencao` meses -- independente de qual revenda respondeu hoje."""
    local = bancos.ler_tabela(BANCO, nome_tabela)
    if local.height == 0:
        return
    corte = (date.today().replace(day=1) - relativedelta(months=meses_retencao)).strftime("%Y%m")
    mantidas = local.filter(pl.col(coluna_mes).is_null() | (pl.col(coluna_mes) >= corte))
    if mantidas.height < local.height:
        bancos.grava_tabela(BANCO, nome_tabela, mantidas)
        print(f"  -> {nome_tabela}: {local.height - mantidas.height} linha(s) anteriores a {corte} podadas")
