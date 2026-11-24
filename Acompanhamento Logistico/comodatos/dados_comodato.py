"""Regras de PRAZO de documento do comodato -- o recorte de
`pages/Comodatos/Dados/dados_comodato.py` do app que o aviso do meio-dia usa.

No app, este modulo e' a camada de dados das telas de Comodatos (le os
.parquet que scripts/atualiza_comodato_parquet.py grava e aplica as regras de
negocio por cima). Aqui ficou so' o necessario para o aviso por e-mail: a regra
do prazo e as faixas de cor. A regra e' UNICA -- a cor da celula "Prazo" na
tela e o aviso por e-mail chamam as mesmas funcoes, pra nunca discordarem.

Fora do recorte: anexos lidos ao vivo do banco (aqui ja' vem no .parquet), as
listas de cliente de EXCECAO/estrangeiro (config/excecoes_comodato.json) e a
excecao de uma revenda cujo prazo nunca vira "Esgotado".
"""

import re
from datetime import date
from pathlib import Path

import polars as pl

from core import revendas

PASTA_PARQUET = Path(__file__).resolve().parents[1] / "data" / "bd_comodato"

COLUNA_PDV = "anexo1"
"""Documento do PDV -- anexado pela tela, so' no fluxo manual."""
COLUNA_CONTRATO = "anexo2"
"""Contrato/CEV assinado -- quem grava e' o webhook, nunca a tela."""
COLUNA_CANHOTO = "anexo3"
"""Canhoto de entrega -- anexado pela tela."""

DOCUMENTOS_EXIGIDOS_AUTOMATICO = (COLUNA_CONTRATO, COLUNA_CANHOTO)
"""Contrato automatico (1 ano): contrato assinado + canhoto."""
DOCUMENTOS_EXIGIDOS_MANUAL = (COLUNA_PDV, COLUNA_CONTRATO, COLUNA_CANHOTO)
"""Fluxo manual (CEV antigo e o de 30 dias): os tres."""

SEM_ANEXO = ("", "na", "nan", "null", "none")
"""Texto que NAO conta como documento -- o app em R gravou a palavra "NA" como
texto em dezenas de registros; contar isso como entregue daria baixa no prazo de
um CEV sem documento nenhum."""

DIAS_PRAZO_ENVIO = 10
"""Dias pra entregar a documentacao, contados da emissao do CEV."""
DTE_MINIMA_PRAZO = 20240507
DATA_BASE_PRAZO_ANTIGO = date(2025, 4, 7)
"""CEV emitido antes de DTE_MINIMA_PRAZO conta o prazo a partir desta data --
senao todo comodato velho nasceria vencido. Mesma regra do R."""

ROTULO_PRAZO_ESGOTADO = "Esgotado"
"""O que a coluna "Prazo" mostra quando o prazo venceu. Constante porque a cor
da celula, o bloqueio da revenda e o aviso por e-mail comparam contra esta
palavra -- escrita a mao em cada ponto, teria como divergir."""

# --- Faixas de cor da coluna "Prazo" -------------------------------------
FAIXA_PRAZO_FOLGA = "folga"
FAIXA_PRAZO_ATENCAO = "atencao"
FAIXA_PRAZO_CRITICO = "critico"

PRAZO_DIAS_FOLGA = 7
"""De 7 dias em diante o prazo esta' em dia -- verde."""
PRAZO_DIAS_ATENCAO = 3
"""De 3 a 6 dias o prazo aperta -- amarelo. Abaixo disso (2, 1, 0, negativo e
"Esgotado") e' critico -- vermelho."""

DIAS_CONTRATO_AUTOMATICO = 300
"""Acima deste intervalo o contrato vai por e-mail automatico e so' o canhoto e'
anexado; ate' aqui (o de 30 dias, na pratica) valem os tres documentos."""

# Data em que cada revenda entrou no fluxo novo (contrato automatico). No app
# vem de config/excecoes_comodato.json -- as MESMAS datas do bloqueio por prazo
# esgotado, pra nao haver uma segunda lista que divirja. Datas ficticias.
CORTE_CONTRATO_AUTOMATICO = {
    revendas.cnpj_da_revenda("Aurora"): "20260324",
    revendas.cnpj_da_revenda("Boreal Norte"): "20260401",
    revendas.cnpj_da_revenda("Boreal Sul"): "20260415",
    revendas.cnpj_da_revenda("Cerrado"): "20260427",
}
CORTE_PADRAO = "20260427"


def _carregar(nome: str, cnpj_rev: str | None = None) -> pl.DataFrame:
    """Com `cnpj_rev`, le so' o arquivo daquela revenda quando ele existir
    (particao gravada por atualiza_comodato_parquet.py); sem particao, cai no
    arquivo unico de sempre."""
    if cnpj_rev:
        parcial = PASTA_PARQUET / f"{nome}_{cnpj_rev}.parquet"
        if parcial.exists():
            return pl.read_parquet(parcial)
    caminho = PASTA_PARQUET / f"{nome}.parquet"
    if not caminho.exists():
        return pl.DataFrame()
    return pl.read_parquet(caminho)


def _data_aaaammdd(coluna: str) -> pl.Expr:
    return pl.col(coluna).cast(pl.Utf8).str.to_date("%Y%m%d", strict=False)


def _expr_corte_contrato_automatico(coluna_cnpj: str = "cnpj") -> pl.Expr:
    return (
        pl.col(coluna_cnpj).cast(pl.Utf8)
        .replace_strict(CORTE_CONTRATO_AUTOMATICO, default=CORTE_PADRAO, return_dtype=pl.Utf8)
        .str.to_date("%Y%m%d", strict=False)
    )


def _expr_antes_contrato_automatico(coluna_cnpj: str = "cnpj") -> pl.Expr:
    """CEV emitido ANTES de a revenda entrar no contrato automatico: segue o fluxo
    manual antigo, ou seja, pede os tres documentos em qualquer intervalo e nao
    entra na regra do prazo vencido."""
    emissao = pl.col("Data Emissão").cast(pl.Utf8).str.replace_all("-", "").str.to_date("%Y%m%d", strict=False)
    return emissao.is_not_null() & (emissao < _expr_corte_contrato_automatico(coluna_cnpj))


def _expr_tem_anexo(coluna: str) -> pl.Expr:
    texto = pl.col(coluna).cast(pl.Utf8).str.strip_chars()
    return texto.is_not_null() & ~texto.str.to_lowercase().is_in(list(SEM_ANEXO))


def _expr_contrato_manual(colunas, coluna_cnpj: str = "cnpj") -> pl.Expr | None:
    """CEV do fluxo manual -- o que pede os TRES documentos na tela: emitido antes
    de a revenda entrar no contrato automatico, OU intervalo ate'
    DIAS_CONTRATO_AUTOMATICO dias (o comodato de 30 dias).

    O `over("CEV")` reproduz o `any()`/`max()` sobre o CEV inteiro: o grupo e' o
    que a tela mostra, entao a decisao tem de ser a mesma para todas as linhas."""
    partes = []
    if "Data Emissão" in colunas and coluna_cnpj in colunas:
        partes.append(_expr_antes_contrato_automatico(coluna_cnpj).any().over("CEV"))
    if "Intervalo Vencimento" in colunas:
        intervalo = pl.col("Intervalo Vencimento").cast(pl.Int64, strict=False).max().over("CEV")
        partes.append(intervalo.is_null() | (intervalo <= DIAS_CONTRATO_AUTOMATICO))
    if not partes:
        return None
    expressao = partes[0]
    for parte in partes[1:]:
        expressao = expressao | parte
    return expressao


def _todos_entregues(documentos) -> pl.Expr:
    expressao = pl.lit(True)
    for coluna in documentos:
        expressao = expressao & _expr_tem_anexo(coluna)
    return expressao


def _expr_docs_completos(colunas, coluna_cnpj: str = "cnpj") -> pl.Expr:
    """Documentacao entregue por inteiro -- o conjunto AUTOMATICO ou o MANUAL,
    conforme o CEV."""
    if not all(coluna in colunas for coluna in DOCUMENTOS_EXIGIDOS_MANUAL):
        return pl.col("dataI").is_not_null()
    automatico = _todos_entregues(DOCUMENTOS_EXIGIDOS_AUTOMATICO)
    manual = _expr_contrato_manual(colunas, coluna_cnpj)
    if manual is None:
        return automatico
    return pl.when(manual).then(_todos_entregues(DOCUMENTOS_EXIGIDOS_MANUAL)).otherwise(automatico)


def _expr_prazo_envio(colunas) -> pl.Expr:
    """Dias que faltam para o prazo de envio vencer, contados de HOJE.

    A coluna "Prazo Envio" do cache e' calculada pelo pipeline e fica congelada no
    `.parquet` ate' a proxima execucao -- pipeline atrasado significaria prazo que
    nao anda. Recalcular aqui custa uma subtracao de datas e nao depende de nada
    rodar de madrugada. Sem a data de emissao na base, usa o valor do cache."""
    if "Data Emissão" not in colunas:
        return pl.col("Prazo Envio").cast(pl.Int64, strict=False)
    emissao_texto = pl.col("Data Emissão").cast(pl.Utf8).str.replace_all("-", "")
    emissao = emissao_texto.str.to_date("%Y%m%d", strict=False)
    base = (
        pl.when(emissao_texto.cast(pl.Int64, strict=False) < DTE_MINIMA_PRAZO)
        .then(pl.lit(DATA_BASE_PRAZO_ANTIGO))
        .otherwise(emissao)
    )
    calculado = (DIAS_PRAZO_ENVIO - (pl.lit(date.today()) - base).dt.total_days()).cast(pl.Int64)
    return calculado.fill_null(pl.col("Prazo Envio").cast(pl.Int64, strict=False))


def _prazo_input(colunas, coluna_cnpj: str = "cnpj") -> pl.Expr:
    # "Prazo Envio" (dias restantes) -> texto. Para de correr quando a
    # documentacao esta completa (ver _expr_docs_completos).
    prazo = _expr_prazo_envio(colunas)
    return (
        pl.when(_expr_docs_completos(colunas, coluna_cnpj)).then(None)
        .when(prazo < 0).then(pl.lit(ROTULO_PRAZO_ESGOTADO))
        .otherwise(prazo.cast(pl.Utf8) + pl.lit(" dias restantes"))
    )


def comod_geral(revenda: str | None) -> pl.DataFrame:
    """O cache "comod_geral_bruto" filtrado pela revenda, com a coluna "Prazo do
    Input" (o texto da coluna "Prazo" da tela). O filtro por EMPRESA e'
    case-insensitive pra replicar a collation padrao do MySQL, onde ele era um
    "WHERE E.EMPRESA LIKE '{revenda}%'"."""
    revenda_up = (revenda or "").upper()
    base = _carregar("comod_geral_bruto", revendas.cnpj_da_revenda(revenda))
    if base.height == 0:
        return base
    base = base.filter(pl.col("EMPRESA").str.to_uppercase().str.starts_with(revenda_up)).drop("EMPRESA")
    if base.height == 0:
        return base
    return base.with_columns(
        _prazo_input(base.columns).alias("Prazo do Input"),
        _data_aaaammdd("Data Emissão").alias("Data Emissão"),
        _data_aaaammdd("Data Vencimento").alias("Data Vencimento"),
    )


def expr_prazo_esgotado() -> pl.Expr:
    """Prazo de envio do documento vencido -- por linha. Vale so' de a data de
    corte da revenda em diante: CEV do fluxo manual antigo continua recebendo
    documento."""
    dias = pl.col("Prazo do Input").str.extract(r"(-?\d+)").cast(pl.Int64, strict=False)
    esgotado = (pl.col("Prazo do Input") == ROTULO_PRAZO_ESGOTADO) | (dias < 0)
    return esgotado.fill_null(False) & ~_expr_antes_contrato_automatico()


def mapa_prazo_esgotado(base: pl.DataFrame) -> dict[str, bool]:
    """CEV -> prazo de envio vencido. Uma linha vencida marca o CEV inteiro."""
    if base.height == 0 or "Prazo do Input" not in base.columns:
        return {}
    agrupado = (
        base.with_columns(expr_prazo_esgotado().alias("_esgotado"))
        .group_by("CEV").agg(pl.col("_esgotado").any())
    )
    return {str(cev): bool(valor) for cev, valor in zip(agrupado["CEV"], agrupado["_esgotado"])}


_RE_DIAS_PRAZO = re.compile(r"-?\d+")


def dias_do_prazo(texto) -> int | None:
    """Dias restantes escritos na coluna "Prazo" ("9 dias restantes" -> 9).

    Le o TEXTO que a tela mostra em vez de refazer a conta de datas: assim a cor
    da celula, o filtro "Prazo" e o aviso por e-mail falam todos do mesmo numero.
    Devolve o MENOR quando o texto tem mais de um numero. "Esgotado", celula
    vazia e nulo devolvem None."""
    if texto is None:
        return None
    valores = _RE_DIAS_PRAZO.findall(str(texto))
    return min(int(valor) for valor in valores) if valores else None


def faixa_prazo(texto, esgotado: bool = False) -> str | None:
    """Faixa de cor do prazo de um CEV. None = sem cor: e' a celula vazia de quem
    entregou a documentacao (o prazo parou de correr) ou um texto sem numero."""
    if esgotado:
        return FAIXA_PRAZO_CRITICO
    conteudo = "" if texto is None else str(texto).strip()
    if not conteudo or conteudo.lower() == "nan":
        return None
    if ROTULO_PRAZO_ESGOTADO in conteudo:
        return FAIXA_PRAZO_CRITICO
    dias = dias_do_prazo(conteudo)
    if dias is None:
        return None
    if dias >= PRAZO_DIAS_FOLGA:
        return FAIXA_PRAZO_FOLGA
    if dias >= PRAZO_DIAS_ATENCAO:
        return FAIXA_PRAZO_ATENCAO
    return FAIXA_PRAZO_CRITICO


def mapa_apos_implantacao(base: pl.DataFrame, coluna_cnpj: str = "cnpj") -> dict[str, bool]:
    """CEV -> emitido a partir do dia em que a REVENDA DELE entrou no fluxo novo.
    Comodato de antes da implantacao segue o fluxo manual antigo e nao entra em
    cobranca automatica."""
    if base.height == 0 or "CEV" not in base.columns or "Data Emissão" not in base.columns:
        return {}
    agrupado = (
        base.with_columns((~_expr_antes_contrato_automatico(coluna_cnpj)).alias("_apos"))
        .group_by("CEV").agg(pl.col("_apos").any())
    )
    return {str(cev): bool(valor) for cev, valor in zip(agrupado["CEV"], agrupado["_apos"])}
