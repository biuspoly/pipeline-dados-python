"""Camada intermediaria de Comodatos: le a camada bruta gravada por
"dados_brutos/scripts/atualiza_dados_brutos.py" (clientes, produtos, precos,
hierarquia de vendedor) e "dados_brutos/scripts/atualiza_comodatos_bruto.py"
(titulos em aberto, CEV pendente, estoque, livro fiscal) -- ambas em
bd_shiny_bruto --, calcula inadimplencia/status/saldo de vasilhame, e grava o
resultado em "bd_comodato" (TB_COMODATO, tb_livro_fiscal, TB_CLIENTES_INAD,
TB_PRODUTOS_CEV, tb_refrig_cev). E' a base que
scripts/atualiza_comodato_parquet.py (deste mesmo app) le pra gerar o cache
.parquet usado pelas paginas de Comodatos.

Porte de um script em R (a versao que rodava no servidor antes). Diferente dos
scripts de camada bruta (que moram em "dados_brutos/", fora de qualquer app):
este e' especifico de Comodatos, entao mora aqui, junto do resto do pipeline
desta funcionalidade.

Nome trocado de "atualiza_comodato.py" pra "..._intermediario.py" no original:
o nome quase identico do outro passo (so' com "s" a mais) nao deixava claro que
existiam DOIS passos separados -- rodar so' um deixava a tela vazia.

Observacao dos dados: o R original usava um banco MySQL pessoal/externo so' pra
pegar a coluna "subordinacao" (o Gerente de Area, "GA" na tela) de cada
vendedor. Achado que tb_funcionarios (gravada pela camada bruta a partir da API
de hierarquia) ja' tem uma coluna literalmente chamada GA, com os mesmos
codigos de vendedor -- trocado, eliminando a dependencia externa.

DEMONSTRACAO: "bd_shiny_bruto" e "bd_comodato" sao pastas de Parquet lidas pelo
DuckDB (core/banco.py) -- as consultas abaixo sao as mesmas do MySQL, com o
minimo de ajuste de dialeto (anotado onde houve).
"""

import re
import sys
from datetime import date
from pathlib import Path

import polars as pl

RAIZ = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(RAIZ))

from core import revendas  # noqa: E402
from core.banco import consulta as consulta_banco  # noqa: E402
from core.parquet import sobrescreve_mysql  # noqa: E402

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

hoje = date.today()


def consulta(banco: str, sql: str) -> pl.DataFrame:
    return consulta_banco(banco, sql)


def chave(df: pl.DataFrame, *colunas: str) -> pl.Expr:
    """Equivalente a paste0(col1, col2, ...) do R, mas robusto a um bug ja'
    documentado no projeto (coluna numerica vinda do MySQL como DOUBLE gerando
    "470.0" em vez de "470")."""
    partes = []
    for coluna in colunas:
        col = pl.col(coluna)
        if df.schema[coluna] in (pl.Float32, pl.Float64):
            col = col.cast(pl.Int64)
        partes.append(col.cast(pl.Utf8))
    return pl.concat_str(partes, separator="")


_PADRAO_UNIDADE = re.compile(r"\d{1,3}(?:X\d{1,2})?(?=\s*U|UN)", re.IGNORECASE)
_PADRAO_CHAVE_ZERO = re.compile(r"0000(?=.{2}$)")


def extrai_unidade(produto: str | None) -> float | None:
    """Porte de "str_extract(PRODUTO, '(?i)\\d{1,3}(?:X\\d{1,2})?(?=\\s*U|UN)')"
    + "prod(as.numeric(unlist(strsplit(x, 'X'))))" do R -- o regex tem
    lookahead, que o motor Rust do polars nao suporta, entao roda linha a linha
    em Python puro (re, que suporta lookahead) via map_elements."""
    if produto is None:
        return None
    m = _PADRAO_UNIDADE.search(produto)
    if not m:
        return None
    resultado = 1
    for parte in re.split("[Xx]", m.group(0)):
        resultado *= int(parte)
    return float(resultado)


def compacta_chave(valor: str | None) -> str | None:
    """Porte de "gsub('0000(?=.{2}$)', '', x, perl=TRUE)" do R -- mesmo motivo
    do extrai_unidade acima (lookahead, sem suporte no motor do polars)."""
    if valor is None:
        return None
    return _PADRAO_CHAVE_ZERO.sub("", valor)


MESES_PT = {
    1: "jan", 2: "fev", 3: "mar", 4: "abr", 5: "mai", 6: "jun",
    7: "jul", 8: "ago", 9: "set", 10: "out", 11: "nov", 12: "dez",
}

# ==========================================================================
# Leitura -- copia 1-pra-1 das consultas do R
# ==========================================================================

# So' os 2 ultimos anos-calendario de CEV entram nesta camada (NPED e' AAAAMM +
# 6 digitos). O ano sai de hoje pra a janela andar sozinha; a camada bruta
# continua guardando desde 202001, quem corta e' so' esta leitura. CAST: NPED e'
# varchar e tem valor de 1 e de 12 caracteres, entao comparar como texto so'
# acertaria por sorte. (MySQL: CAST(... AS UNSIGNED); DuckDB: BIGINT.)
NPED_MINIMO = ((hoje.year - 2) * 100 + 1) * 1_000_000

cev_bruto_completo = consulta(
    "bd_shiny_bruto", f"SELECT * FROM tb_cev WHERE CAST(NPED AS BIGINT) >= {NPED_MINIMO}"
)
if "TIPO" in cev_bruto_completo.columns:
    # TIPO de tb_cev (tipo do comodato) colide com o TIPO de tb_produtos (tipo
    # de produto, Impulse/Take Home) juntado mais abaixo -- sem este rename, o
    # join geraria "TIPO_right" e sobrescreveria o significado de "TIPO" sem
    # avisar.
    cev_bruto_completo = cev_bruto_completo.rename({"TIPO": "TIPO_COMODATO"})
titulos_em_aberto = consulta("bd_shiny_bruto", "SELECT * FROM tb_titulos_abertos")
clientes = consulta("bd_shiny_bruto", "SELECT * FROM tb_clientes WHERE VEN <> '999'")
produtos = consulta("bd_shiny_bruto", "SELECT * FROM tb_produtos")
produtos_cev = consulta(
    "bd_shiny_bruto",
    """
    SELECT
        pr.*,
        CASE WHEN ec.ESTOQUE IS NULL THEN 'NAO' ELSE ec.ESTOQUE END AS ESTOQUE,
        ROUND(pp.PRECO_UN, 2) AS PRECO,
        pp.TAB_CEV
    FROM (SELECT * FROM tb_produtos) pr
    LEFT JOIN tb_precos_prod pp ON pp.CODCOMP = pr.CODCOMP AND pp.CNPJ_REV = pr.CNPJ_REV
    LEFT JOIN tb_estoque_cev ec ON ec.CODCOMP = pr.CODCOMP AND ec.CNPJ_REV = pr.CNPJ_REV
    """,
)
livro_fiscal = consulta(
    "bd_shiny_bruto", "SELECT * FROM tb_livro_entrada_cev UNION ALL SELECT * FROM tb_livro_saida_cev"
)
# Colunas normalizadas pra maiuscula -- o livro fiscal tem a coluna "DATA"
# maiuscula mas "operacao" minuscula (a camada bruta grava exatamente essa
# mistura, alias por alias). Uniformizar aqui evita ter que decorar caixa por
# coluna toda vez que este script for editado.
livro_fiscal.columns = [c.upper() for c in livro_fiscal.columns]
# PRODUTO pode chegar como Int64 se a tabela da camada bruta for de antes do
# CAST na consulta ao ERP -- cast defensivo garante Utf8 sempre.
livro_fiscal = livro_fiscal.with_columns(pl.col("PRODUTO").cast(pl.Utf8))

funcionarios = consulta("bd_shiny_bruto", "SELECT VEN, REVENDA, GA FROM tb_funcionarios")

# cev e cev_vencidos_a_vencer (mesma tabela, so' acrescenta VENC) vem da MESMA
# consulta no R -- uma unica leitura aqui.
cev = cev_bruto_completo
cev_vencidos_a_vencer = cev_bruto_completo.with_columns(
    pl.when(pl.col("DTV").cast(pl.Int64, strict=False) >= int(hoje.strftime("%Y%m%d")))
    .then(pl.lit("A VENCER"))
    .otherwise(pl.lit("VENCIDO"))
    .alias("VENC")
)

# ==========================================================================
# REVENDA (nome) -> CNPJ_REV -- tb_funcionarios so' tem o nome da revenda.
# Vem do lugar unico (core/revendas.py); antes era um dicionario escrito a mao,
# igual ao de outros 11 arquivos.
# ==========================================================================

REVENDA_PARA_CNPJ = revendas.cnpj_por_nome_oficial()
funcionarios = funcionarios.with_columns(
    pl.col("REVENDA").replace_strict(REVENDA_PARA_CNPJ, default=None).alias("CNPJ_REV"),
    pl.col("GA").alias("subordinacao"),
)

# ==========================================================================
# UNIDADE de cada produto (extraida do texto, com 2 excecoes fixas) -- usado
# depois no join com livro_fiscal.
# ==========================================================================

produtos = produtos.with_columns(
    pl.col("PRODUTO").map_elements(extrai_unidade, return_dtype=pl.Float64).alias("UNIDADE")
)
produtos = produtos.with_columns(
    pl.when(pl.col("CODCOMP").is_in(["700101", "700102"]))
    .then(pl.lit(12.0))
    .when(pl.col("CODCOMP").is_in(["700305", "700306"]))
    .then(pl.lit(24.0))
    .otherwise(pl.col("UNIDADE"))
    .alias("UNIDADE")
)

# ==========================================================================
# Titulos em aberto -- inadimplencia (>=1 dia corrido, ou 1 dia se nao cair em
# fim de semana)
# ==========================================================================

titulos_em_aberto = titulos_em_aberto.with_columns(
    pl.col("TITULO_MAIS_ANTIGO").cast(pl.Utf8).str.to_date("%Y%m%d", strict=False).alias("_data_titulo")
)
titulos_em_aberto = titulos_em_aberto.with_columns(
    (pl.lit(hoje) - pl.col("_data_titulo")).dt.total_days().alias("_dias_aberto"),
    pl.col("_data_titulo").dt.weekday().alias("Dia_Semana"),  # 1=segunda ... 7=domingo, igual a format(...,'%u')
)
# Mesma sequencia (aparentemente redundante) de 2 ifelse do R -- portada tal
# como esta, sem "corrigir": a 2a linha nao amplia nada que a 1a (>=1 dia) ja'
# nao tenha coberto, mas e' o que o script em producao faz.
titulos_em_aberto = titulos_em_aberto.with_columns(
    pl.when(pl.col("_dias_aberto") >= 1).then(pl.lit("Sim")).otherwise(pl.lit("Não")).alias("Inadimplente")
)
titulos_em_aberto = titulos_em_aberto.with_columns(
    pl.when((pl.col("_dias_aberto") == 1) & (~pl.col("Dia_Semana").is_in([6, 7])))
    .then(pl.lit("Sim"))
    .otherwise(pl.col("Inadimplente"))
    .alias("Inadimplente")
)

funcionarios = funcionarios.with_columns(chave(funcionarios, "VEN", "CNPJ_REV").alias("CHAVE_VEN"))
titulos_em_aberto = titulos_em_aberto.with_columns(chave(titulos_em_aberto, "CODCLI", "CNPJ_REV").alias("CHAVE_CLI"))
cev = cev.with_columns(
    chave(cev, "CODCLI", "CNPJ_REV").alias("CHAVE_CLI"),
    chave(cev, "CODCOMP", "CNPJ_REV").alias("CHAVE_PROD"),
    chave(cev, "CNPJ_REV", "CEV").alias("CHAVE_REVCEV"),
)
cev_vencidos_a_vencer = cev_vencidos_a_vencer.with_columns(
    chave(cev_vencidos_a_vencer, "CODCLI", "CNPJ_REV").alias("CHAVE_CLI"),
    chave(cev_vencidos_a_vencer, "CODCOMP", "CNPJ_REV").alias("CHAVE_PROD"),
    chave(cev_vencidos_a_vencer, "CNPJ_REV", "CEV").alias("CHAVE_REVCEV"),
)
clientes = clientes.with_columns(
    chave(clientes, "CODCLI", "CNPJ_REV").alias("CHAVE_CLI"),
    chave(clientes, "VEN", "CNPJ_REV").alias("CHAVE_VEN"),
)
produtos = produtos.with_columns(chave(produtos, "CODCOMP", "CNPJ_REV").alias("CHAVE_PROD"))

clientes = clientes.with_columns(pl.col("FANTASIA").str.to_titlecase(), pl.col("NOME").str.to_titlecase())
produtos = produtos.with_columns(pl.col("PRODUTO_GENERICO").str.to_titlecase())

titulos_agrupados = (
    titulos_em_aberto.filter((pl.col("Inadimplente") == "Sim") & (~pl.col("Dia_Semana").is_in([6, 7])))
    .group_by("CHAVE_CLI", "Inadimplente")
    .agg(pl.col("TITULO_MAIS_ANTIGO").max().alias("TITULO_MAIS_ANTIGO"))
)

cev = (
    cev.join(titulos_agrupados.select("CHAVE_CLI", "Inadimplente"), on="CHAVE_CLI", how="left")
    .join(clientes.select("CHAVE_CLI", "FANTASIA", "NOME"), on="CHAVE_CLI", how="left")
    .join(produtos.select("CHAVE_PROD", "PRODUTO_GENERICO", "TIPO"), on="CHAVE_PROD", how="left")
)
cev_vencidos_a_vencer = (
    cev_vencidos_a_vencer.join(titulos_agrupados.select("CHAVE_CLI", "Inadimplente"), on="CHAVE_CLI", how="left")
    .join(clientes.select("CHAVE_CLI", "FANTASIA", "NOME", "CANAL", "PORTE", "VEN"), on="CHAVE_CLI", how="left")
    .join(produtos.select("CHAVE_PROD", "PRODUTO_GENERICO", "TIPO"), on="CHAVE_PROD", how="left")
)

# ==========================================================================
# Intervalo, "Contrato vencido" e resumo por cliente (CEV_PEN)
# ==========================================================================

cev = cev.with_columns(
    pl.col("DTE").cast(pl.Utf8).str.to_date("%Y%m%d", strict=False).alias("_data_emissao"),
    pl.col("DTV").cast(pl.Utf8).str.to_date("%Y%m%d", strict=False).alias("_data_vencimento"),
)
cev = cev.with_columns((pl.col("_data_vencimento") - pl.col("_data_emissao")).dt.total_days().alias("Intervalo"))
cev = cev.with_columns(
    pl.when((pl.col("Intervalo") > 15) & (pl.col("Intervalo") < 185))
    .then(pl.lit("Incorreto"))
    .otherwise(None)
    .alias("Validacao Intervalo")
)
# Nota: o R compara Sys.Date() (objeto Date) direto com DTV (inteiro AAAAMMDD),
# sem converter -- na pratica essa condicao do R e' sempre verdadeira (bug
# latente). Portado aqui com a comparacao de data de verdade (mesmo significado
# pretendido pelo nome do campo), nao a comparacao literal quebrada.
cev = cev.with_columns(
    pl.when(pl.lit(hoje) < pl.col("_data_vencimento")).then(pl.lit("Sim")).otherwise(None).alias("Contrato vencido")
)

cev_cliente = cev.group_by("CHAVE_CLI").agg(pl.col("CHAVE_REVCEV").n_unique().alias("CEV_PEN"))

cev = cev.drop(["CHAVE_CLI", "CHAVE_REVCEV", "CHAVE_PROD"])
cev = cev.with_columns(
    pl.when(pl.col("DTE").cast(pl.Int64, strict=False) < 20240507)
    .then(pl.lit("20250407"))
    .otherwise(pl.col("DTE").cast(pl.Utf8))
    .str.to_date("%Y%m%d", strict=False)
    .alias("_data_base_prazo")
)
cev = cev.with_columns(
    (10 - (pl.lit(hoje) - pl.col("_data_base_prazo")).dt.total_days()).cast(pl.Int64).alias("Prazo Envio")
)
cev = cev.with_columns(pl.col("Inadimplente").fill_null("Não"))
cev = cev.drop(["_data_emissao", "_data_vencimento", "_data_base_prazo"])

# ==========================================================================
# Livro fiscal -- GRUPO_REV (mapeamento base por CNPJ, depois override por
# faixa de VEN -- os dois em sequencia, igual ao R: o override depende do
# GRUPO_REV base ja' calculado, por isso e' um 2o with_columns, nao o mesmo)
# ==========================================================================

CNPJ_PARA_GRUPO_REV = {r.cnpj: r.nome for r in revendas.REVENDAS}
CNPJ_BOREAL_NORTE = revendas.cnpj_da_revenda("Boreal Norte")
CNPJ_CERRADO = revendas.cnpj_da_revenda("Cerrado")

livro_fiscal = livro_fiscal.with_columns(pl.col("VEN").cast(pl.Int64, strict=False).cast(pl.Utf8).alias("_ven_txt"))
livro_fiscal = livro_fiscal.with_columns(
    pl.col("CNPJ_REV").replace_strict(CNPJ_PARA_GRUPO_REV, default=None).alias("GRUPO_REV")
)
ven1 = pl.col("_ven_txt").str.slice(0, 1)
ven3 = pl.col("_ven_txt").str.slice(0, 3)
# No original sao ~20 regras (sub-regiao de cada revenda pela faixa do codigo do
# vendedor); aqui ficam duas, no mesmo formato.
livro_fiscal = livro_fiscal.with_columns(
    pl.when((ven3 == "211") & (pl.col("CNPJ_REV") == CNPJ_BOREAL_NORTE)).then(pl.lit("Boreal Norte Serra"))
    .when(ven3.is_in(["401", "402"]) & (pl.col("CNPJ_REV") == CNPJ_CERRADO)).then(pl.lit("Cerrado Vale"))
    .otherwise(pl.col("GRUPO_REV"))
    .alias("GRUPO_REV")
).drop("_ven_txt")

livro_fiscal = livro_fiscal.with_columns(pl.col("DATA").cast(pl.Utf8).str.to_date("%Y%m%d", strict=False).alias("_data_livro"))
livro_fiscal = livro_fiscal.with_columns(
    ((pl.lit(hoje.year) - pl.col("_data_livro").dt.year()) * 12 + (pl.lit(hoje.month) - pl.col("_data_livro").dt.month())).alias(
        "_meses_atras"
    )
)
livro_fiscal = livro_fiscal.with_columns(
    pl.when(pl.col("_meses_atras") == 0)
    .then(pl.lit("Mês Atual"))
    .otherwise(pl.col("_data_livro").dt.month().replace_strict(MESES_PT, default=None) + pl.lit("/") + pl.col("_data_livro").dt.year().cast(pl.Utf8))
    .alias("MES_ATUAL")
).drop(["_data_livro", "_meses_atras"])

livro_fiscal = livro_fiscal.with_columns(pl.col("PRODUTO_GENERICO").str.to_titlecase())
livro_fiscal = livro_fiscal.with_columns(chave(livro_fiscal, "PRODUTO", "CNPJ_REV").alias("CHAVE_PROD"))
livro_fiscal = livro_fiscal.join(produtos.select("CHAVE_PROD", "UNIDADE"), on="CHAVE_PROD", how="left")
livro_fiscal = livro_fiscal.with_columns((pl.col("QTD") / pl.col("UNIDADE")).floor().alias("QTD_REAL"))
livro_fiscal = livro_fiscal.unique()

# ==========================================================================
# Produtos categorizados (Material/Vasilhame/Outro) -- mesmo regex do R, sem
# lookahead, funciona direto no motor do polars.
# ==========================================================================

produtos_cev = produtos_cev.with_columns(
    pl.when(pl.col("PRODUTO").str.contains("(?i)CADEIRA|MESA|REFR|REF |CHOP|BARRIL|CILINDRO|BARRACA|POLTRONA|BOX|CARRINHO"))
    .then(pl.lit("Material"))
    .when(pl.col("PRODUTO").str.contains("(?i)CAIXA|GARRAFA"))
    .then(pl.lit("Vasilhame"))
    .otherwise(pl.lit(None))
    .alias("TIPO")
)
produtos_cev = produtos_cev.with_columns(
    pl.when(pl.col("PRODUTO").str.contains("(?i)CAIXA T[ÉE]RMICA|CX T[ÉE]RMICA")).then(pl.lit("Material")).otherwise(pl.col("TIPO")).alias("TIPO")
)
produtos_cev = produtos_cev.with_columns(
    pl.when(pl.col("TIPO").is_in(["Vasilhame", "Material"])).then(pl.col("TIPO")).otherwise(pl.lit("Outro")).alias("TIPO")
)

# ==========================================================================
# Status DLP (bloqueado/disponivel) por saldo de vasilhame -- copia 1-pra-1
# das listas e formulas do R.
# ==========================================================================

CAIXA_1L = ["700101", "700102"]
CAIXA_300ML = ["700201", "700202", "700203", "700204", "700205"]
CAIXA_600ML_PREMIUM = ["700401", "700402"]
EMPRESTIMO_BLOQ = ["700301", "700302", "700303", "700304", "700305", "700306"]

livro_para_saldo = livro_fiscal.filter(pl.col("PRODUTO").is_in(CAIXA_1L + CAIXA_300ML + EMPRESTIMO_BLOQ + CAIXA_600ML_PREMIUM))


def soma_qtd(operacao: str, produtos_lista: list[str]) -> pl.Expr:
    return (
        pl.col("QTD")
        .filter((pl.col("OPERACAO") == operacao) & pl.col("PRODUTO").is_in(produtos_lista))
        .sum()
        .fill_null(0)
    )


saldo_consolidado = livro_para_saldo.group_by("CNPJ_REV").agg(
    soma_qtd("Dev Emprestimo", CAIXA_1L).alias("Recolha Ano 1L"),
    soma_qtd("Emprestei", CAIXA_1L).alias("Empréstimo Ano 1L"),
    soma_qtd("Dev Emprestimo", CAIXA_300ML).alias("Recolha Ano 300ml"),
    soma_qtd("Emprestei", CAIXA_300ML).alias("Empréstimo Ano 300ml"),
    soma_qtd("Dev Emprestimo", EMPRESTIMO_BLOQ).alias("Recolha Ano 600ml"),
    soma_qtd("Emprestei", EMPRESTIMO_BLOQ).alias("Empréstimo Ano 600ml"),
    soma_qtd("Dev Emprestimo", CAIXA_600ML_PREMIUM).alias("Recolha Ano 600ml Premium"),
    soma_qtd("Emprestei", CAIXA_600ML_PREMIUM).alias("Empréstimo Ano 600ml Premium"),
)
saldo_consolidado = saldo_consolidado.with_columns(
    (pl.col("Recolha Ano 1L") - pl.col("Empréstimo Ano 1L")).alias("Caixa 1L"),
    (pl.col("Recolha Ano 300ml") - pl.col("Empréstimo Ano 300ml")).alias("Caixa 300"),
    (pl.col("Recolha Ano 600ml Premium") - pl.col("Empréstimo Ano 600ml Premium")).alias("Caixa Premium 600"),
    (pl.col("Recolha Ano 600ml") - pl.col("Empréstimo Ano 600ml")).alias("Caixa 600ml"),
)
saldo_consolidado = saldo_consolidado.with_columns(
    (pl.col("Caixa Premium 600") + ((pl.col("Caixa 600ml")) / 5).floor()).alias("Total Premium 600"),
    (pl.col("Caixa 600ml") / 5).floor().alias("Bônus Premium 600"),
)
saldo_consolidado = saldo_consolidado.select(
    "CNPJ_REV", "Caixa 1L", "Caixa 300", "Total Premium 600", "Caixa Premium 600", "Bônus Premium 600", "Caixa 600ml"
)

mapa_produtos = pl.DataFrame(
    {
        "PRODUTO": CAIXA_1L + CAIXA_300ML + EMPRESTIMO_BLOQ + CAIXA_600ML_PREMIUM,
        "TIPO_SALDO": (
            ["Caixa 1L"] * len(CAIXA_1L)
            + ["Caixa 300"] * len(CAIXA_300ML)
            + ["Caixa 600ml"] * len(EMPRESTIMO_BLOQ)
            + ["Caixa Premium 600"] * len(CAIXA_600ML_PREMIUM)
        ),
    }
)

resultado_final = mapa_produtos.join(saldo_consolidado.select("CNPJ_REV"), how="cross").join(
    saldo_consolidado, on="CNPJ_REV", how="left"
)
resultado_final = resultado_final.with_columns(
    pl.when(pl.col("TIPO_SALDO") == "Caixa 1L").then(pl.col("Caixa 1L"))
    .when(pl.col("TIPO_SALDO") == "Caixa 300").then(pl.col("Caixa 300"))
    .when(pl.col("TIPO_SALDO") == "Caixa 600ml").then(pl.col("Caixa 600ml"))
    .when(pl.col("TIPO_SALDO") == "Caixa Premium 600").then(pl.col("Caixa Premium 600"))
    .alias("saldo")
).select("CNPJ_REV", "PRODUTO", "saldo")

resultado_final = resultado_final.with_columns(
    pl.when(pl.col("saldo") <= 0).then(pl.lit("Bloqueado")).otherwise(pl.lit("Disponível")).alias("STATUS_DLP")
)
resultado_final = resultado_final.with_columns(
    pl.when(pl.col("PRODUTO").is_in(EMPRESTIMO_BLOQ)).then(pl.lit("Bloqueado")).otherwise(pl.col("STATUS_DLP")).alias("STATUS_DLP")
)

produtos_cev = produtos_cev.join(
    resultado_final.select("CNPJ_REV", "PRODUTO", "STATUS_DLP"),
    left_on=["CNPJ_REV", "CODCOMP"],
    right_on=["CNPJ_REV", "PRODUTO"],
    how="left",
)

# ==========================================================================
# Clientes inadimplentes -- copia 1-pra-1 do bloco final do R
# ==========================================================================

qtd_titulos = titulos_em_aberto.group_by("CHAVE_CLI").agg(
    (pl.col("Inadimplente") == "Sim").sum().alias("Titulos")
)

clientes_inad = clientes.select(
    "CGC_CPF", "PORTE", "CHAVE", "CHAVE_CLI", "CHAVE_VEN", "FANTASIA", "NOME", "CANAL", "VEN", "CODCLI", "CNPJ_REV",
    "ENDERECO", "CONTATO", "PTO_REF", "CIDADE", "DIA_VISIT", "SEQ", "CEP", "FONE", "PROPRIETARIO",
    "CPF_PROP", "EMAIL_PROP", "NASC_PROP",
).join(titulos_agrupados.select("CHAVE_CLI", "TITULO_MAIS_ANTIGO", "Inadimplente"), on="CHAVE_CLI", how="left")
clientes_inad = clientes_inad.join(qtd_titulos, on="CHAVE_CLI", how="left")
clientes_inad = clientes_inad.with_columns(
    pl.col("CHAVE").cast(pl.Int64, strict=False).cast(pl.Utf8).str.pad_start(14, "0").alias("CHAVE")
)
clientes_inad = clientes_inad.join(cev_cliente, on="CHAVE_CLI", how="left")
clientes_inad = clientes_inad.join(
    funcionarios.select("CHAVE_VEN", "subordinacao"), on="CHAVE_VEN", how="left"
)

colunas_texto = [nome for nome, tipo in clientes_inad.schema.items() if tipo == pl.Utf8]
clientes_inad = clientes_inad.with_columns([pl.col(c).str.to_titlecase() for c in colunas_texto])

clientes_inad = clientes_inad.with_columns(pl.col("CHAVE").map_elements(compacta_chave, return_dtype=pl.Utf8).alias("CHAVE"))
clientes_inad = clientes_inad.with_columns(
    pl.when(pl.col("CGC_CPF") == "0000").then(pl.lit("Cpf")).otherwise(pl.lit("Cnpj")).alias("CGC_CPF")
)

clientes_inad = clientes_inad.with_columns(
    pl.col("CNPJ_REV").replace_strict(CNPJ_PARA_GRUPO_REV, default=None).alias("REVENDA")
)
clientes_inad = clientes_inad.with_columns(pl.col("Inadimplente").fill_null("Não"))
clientes_inad = clientes_inad.with_columns(
    pl.when(pl.col("CEV_PEN").is_null()).then(pl.lit("Não")).otherwise(pl.lit("Sim")).alias("CEV_PEN")
)
clientes_inad = clientes_inad.with_columns(
    pl.when(pl.col("EMAIL_PROP").str.strip_chars() == "").then(pl.lit("Não")).otherwise(pl.lit("Sim")).alias("TEM_CAD")
)

# ==========================================================================
# Refrigeradores -- clientes com "Ref" no nome generico do produto
# ==========================================================================

refrigerador = (
    cev_vencidos_a_vencer.filter(pl.col("PRODUTO_GENERICO").str.contains("Ref") & pl.col("NOME").is_not_null())
    .group_by("CNPJ_REV", "CODCLI", "NOME", "FANTASIA", "VEN", "CANAL", "PORTE")
    .agg(pl.col("QTD").sum().fill_null(0).alias("Total"))
)

# ==========================================================================
# Grava em bd_comodato -- sobrescreve_mysql (core/parquet.py) dropa e recria
# cada tabela.
# ==========================================================================

# "tipos"/"indices": no MySQL, sem eles a coluna de join nasce TEXT e o banco
# varre as tabelas inteiras no lugar de usar indice. Como sobrescreve_mysql
# recria a tabela, os dois precisam ser declarados aqui -- feitos a mao, se
# perderiam na proxima execucao. (Ignorados no banco emulado.)
CHAVES_JOIN = {"CNPJ_REV": "VARCHAR(20)", "CODCLI": "VARCHAR(20)"}

# CODCOMP fica FORA de "tipos"/"indices" de tb_comodato de proposito. Ele chega
# como numero (tb_cev.CODCOMP e' int, o zero a esquerda ja' se perdeu la') e o
# catalogo guarda o codigo de 6 digitos em texto ("001001"). Convertido pra
# VARCHAR ele viraria texto x texto e "1001" <> "001001" -- produtos do catalogo
# ficariam orfaos EM SILENCIO.
sobrescreve_mysql(
    "tb_comodato", cev, "bd_comodato",
    tipos=CHAVES_JOIN,
    indices=[
        "CREATE INDEX idx_comod_rev ON tb_comodato (CNPJ_REV)",
        "CREATE INDEX idx_comod_rev_cli ON tb_comodato (CNPJ_REV, CODCLI)",
    ],
)
sobrescreve_mysql("tb_livro_fiscal", livro_fiscal, "bd_comodato")
sobrescreve_mysql(
    "tb_clientes_inad", clientes_inad, "bd_comodato",
    tipos=CHAVES_JOIN,
    indices=["CREATE INDEX idx_inad_rev_cli ON tb_clientes_inad (CNPJ_REV, CODCLI)"],
)
# tb_produtos_cev fica SEM indice de proposito: o join com tb_comodato compara
# CODCOMP texto x numero, entao indice nenhum serve pra casar as linhas -- e a
# existencia dele fazia o otimizador do MySQL trocar o hash join por laco
# aninhado (medido no original: sem indice 1,48s; com indice, 45,9s a 282s).
sobrescreve_mysql(
    "tb_produtos_cev", produtos_cev, "bd_comodato",
    tipos={"CNPJ_REV": "VARCHAR(20)", "CODCOMP": "VARCHAR(20)"},
)
sobrescreve_mysql("tb_refrig_cev", refrigerador, "bd_comodato")

print("atualiza_comodato_intermediario.py finalizado.")
