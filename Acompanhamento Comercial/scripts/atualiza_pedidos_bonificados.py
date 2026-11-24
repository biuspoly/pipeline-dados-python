"""Pipeline offline de Pedidos Bonificados: bruto (bd_shiny_bruto) -> bd_dlp + Parquet.

DEMONSTRACAO: os bancos sao pastas de Parquet consultadas pelo DuckDB
(core/banco.py). Ficou de fora o modo "--cnpj" do original (atualizacao sob
demanda de UMA revenda, disparada pelo botao "Atualizar" da tela via fila
Celery/Redis), que regrava so' a particao daquela revenda.
"""

import sys
from datetime import date
from pathlib import Path

import polars as pl
from dateutil.relativedelta import relativedelta

# Path(__file__) (nao cwd) resolve a pasta do app independente de como o
# script roda. Precisa vir antes do "from core..." abaixo: rodando
# "python scripts/atualiza_pedidos_bonificados.py", o Python so' coloca
# "scripts/" no caminho de busca de modulos.
PASTA_APP = Path(__file__).resolve().parent.parent
PASTA_PARQUET = PASTA_APP / "data" / "bd_dlp"
sys.path.insert(0, str(PASTA_APP.parent))

from core.banco import consulta  # noqa: E402
from core.parquet import sobrescreve_mysql, sobrescreve_parquet  # noqa: E402

# Forca a saida do terminal (print, logs) a usar UTF-8, independente do
# idioma/codepage configurado no Windows daquela maquina especifica.
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

inicio = int((date.today() - relativedelta(months=3)).strftime("%Y%m") + "00")

# Uma revenda bonifica tambem com uma ocorrencia propria (602) -- no original
# e' uma revenda especifica; aqui, a ficticia "Cerrado".
REVENDA_OCO_PROPRIA = "Cerrado"

pedidos = consulta(
    "bd_shiny_bruto",
    f"""SELECT CODCLI, CNPJ_REV, COD_VENDEDOR, OCO, PED_ORIGEM, PEDIDO, DT, STATUS, REVENDA
        FROM TB_PEDIDOS
        WHERE DT >= 20260100 AND DT >= {inicio}
          AND ((REVENDA = '{REVENDA_OCO_PROPRIA}' AND OCO IN (2, 602))
               OR (REVENDA <> '{REVENDA_OCO_PROPRIA}' AND OCO IN (2, 4)))""",
).with_columns(
    pl.col("CODCLI", "CNPJ_REV").cast(pl.Utf8),
    pl.col("PEDIDO").cast(pl.Int64, strict=False).cast(pl.Utf8),
    pl.col("OCO").cast(pl.Utf8),
)
clientes = consulta(
    "bd_shiny_bruto", "SELECT CODCLI, CNPJ_REV, FANTASIA, CANAL FROM TB_CLIENTES"
).with_columns(pl.col("CODCLI", "CNPJ_REV").cast(pl.Utf8))
itens = consulta(
    "bd_shiny_bruto",
    f"""SELECT COD_PED, CNPJ_REV, QTD, QTDS, "VALOR VENDA", VALOR_BONIF, QTD_BONIF, QTDS_BONIF
        FROM TB_VEND_DEV_ITENS WHERE COD_PED >= {inicio}0000""",
).with_columns(
    pl.col("COD_PED").cast(pl.Int64, strict=False).cast(pl.Utf8),
    pl.col("CNPJ_REV").cast(pl.Utf8),
)
empresas = consulta("bd_dlp", "SELECT CNPJ_EMP, EMPRESA FROM TB_EMPRESAS").with_columns(
    pl.col("CNPJ_EMP").cast(pl.Utf8)
)

clientes = clientes.with_columns(pl.concat_str(["CODCLI", "CNPJ_REV"]).alias("CHAVE_CLI"))
pedidos = pedidos.with_columns(pl.concat_str(["CODCLI", "CNPJ_REV"]).alias("CHAVE_CLI")).join(
    clientes.select("CHAVE_CLI", "FANTASIA", "CANAL"), on="CHAVE_CLI", how="left"
)
resumo_itens = itens.group_by("COD_PED", "CNPJ_REV").agg(
    pl.col("QTD").sum().alias("QTD_TOTAL_VENDA"),
    pl.col("QTDS").sum().alias("QTDS_TOTAL_VENDA"),
    pl.col("VALOR VENDA").sum().alias("VALOR_VENDA"),
    pl.col("VALOR_BONIF").sum(),
    pl.col("QTD_BONIF").sum().alias("QTD_TOTAL_BONIF"),
    pl.col("QTDS_BONIF").sum().alias("QTDS_TOTAL_BONIF"),
)
historico = pedidos.join(
    resumo_itens, left_on=["PEDIDO", "CNPJ_REV"], right_on=["COD_PED", "CNPJ_REV"], how="inner"
).with_columns(
    pl.when(pl.col("STATUS").is_in([2, 7])).then(pl.lit("Bloqueado"))
    .when(pl.col("STATUS") == 9).then(pl.lit("Cancelado"))
    .otherwise(pl.col("STATUS").cast(pl.Utf8)).alias("STATUS")
).filter(
    ~pl.col("STATUS").is_in(["Bloqueado", "Cancelado"]) & pl.col("FANTASIA").is_not_null()
).select(
    "CNPJ_REV", "CODCLI", "FANTASIA", "CANAL", "COD_VENDEDOR", "OCO",
    "PED_ORIGEM", "PEDIDO", "DT", "VALOR_VENDA", "QTD_TOTAL_VENDA",
    "VALOR_BONIF", "QTD_TOTAL_BONIF", "QTDS_TOTAL_VENDA", "QTDS_TOTAL_BONIF",
).unique()

cache = historico.join(empresas, left_on="CNPJ_REV", right_on="CNPJ_EMP", how="left").with_columns(
    pl.concat_str([pl.col("CODCLI"), pl.lit(" - "), pl.col("FANTASIA")]).alias("CLIENTE")
)
sobrescreve_mysql("tb_historico_bonif", historico, "bd_dlp")
sobrescreve_parquet("tb_historico_bonif", cache, PASTA_PARQUET)
