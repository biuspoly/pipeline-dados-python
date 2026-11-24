"""Pipeline offline de Pedidos Cancelados (Limite CPF/SKU/Fora de Rota/Combo): bruto -> bd_dlp + Parquet.

Quatro regras que explicam POR QUE um pedido foi bloqueado/cancelado, cada uma
numa tabela propria:

- tb_historico_dlp           -- cliente pessoa fisica que passou do limite de
                                faturamento (dia + acumulado do mes);
- tb_historico_dlp_sku       -- pedido com menos SKUs que o minimo do canal;
- tb_historico_dlp_combo     -- bonificou o SKU A sem vender o SKU B (no
                                original, uma regra de marca especifica);
- tb_historico_dlp_fora_rota -- pedido fora do dia de visita do cliente.

DEMONSTRACAO: os bancos sao pastas de Parquet consultadas pelo DuckDB
(core/banco.py). Ficou de fora o modo "--cnpj" do original (atualizacao sob
demanda de UMA revenda pelo botao "Atualizar" da tela).
"""

import sys
from datetime import date
from pathlib import Path

import polars as pl
from dateutil.relativedelta import relativedelta

PASTA_APP = Path(__file__).resolve().parent.parent
PASTA_PARQUET = PASTA_APP / "data" / "bd_dlp"
sys.path.insert(0, str(PASTA_APP.parent))

from core.banco import consulta  # noqa: E402
from core.parquet import sobrescreve_mysql, sobrescreve_parquet  # noqa: E402
from core.revendas import cnpj_da_revenda  # noqa: E402

# Forca a saida do terminal (print, logs) a usar UTF-8, independente do
# idioma/codepage configurado no Windows daquela maquina especifica.
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")


def _com_empresa(tabela: pl.DataFrame, empresas: pl.DataFrame) -> pl.DataFrame:
    return tabela.join(empresas, left_on="CNPJ_REV", right_on="CNPJ_EMP", how="left").with_columns(
        pl.concat_str([pl.col("CODCLI"), pl.lit(" - "), pl.col("FANTASIA")]).alias("CLIENTE")
    )


inicio = int((date.today() - relativedelta(months=3)).strftime("%Y%m") + "00")

# Limite de faturamento de pessoa fisica: uma revenda tem limite proprio.
CNPJ_LIMITE_MENOR = cnpj_da_revenda("Aurora")
# Regra de combo: o pedido que bonifica SKU_COMBO_BONIF precisa vender SKU_COMBO_VENDA.
SKU_COMBO_BONIF = 800001
SKU_COMBO_VENDA = 800002

pedidos = consulta(
    "bd_shiny_bruto",
    f"""SELECT CODCLI, CNPJ_REV, PED_ORIGEM, PEDIDO, DT, VALOR, STATUS,
               OCO, FORA_ROTA, REPROGRAMADO
        FROM TB_PEDIDOS WHERE DT >= 20260100 AND DT >= {inicio}""",
).with_columns(
    pl.col("CODCLI").cast(pl.Utf8),
    pl.col("CNPJ_REV").cast(pl.Utf8),
    pl.col("PEDIDO").cast(pl.Int64, strict=False).cast(pl.Utf8),
    pl.col("DT").cast(pl.Int64),
)
clientes = consulta(
    "bd_shiny_bruto",
    "SELECT CODCLI, CNPJ_REV, FANTASIA, CGC_CPF, COD_CATEGORIA, CANAL, QUINZENAL FROM TB_CLIENTES",
).with_columns(pl.col("CODCLI", "CNPJ_REV").cast(pl.Utf8))
itens = consulta(
    "bd_shiny_bruto",
    f"SELECT COD_PED, CNPJ_REV, CODR, CFOP FROM TB_VEND_DEV_ITENS WHERE COD_PED >= {inicio}0000",
).with_columns(
    pl.col("COD_PED").cast(pl.Int64, strict=False).cast(pl.Utf8),
    pl.col("CNPJ_REV").cast(pl.Utf8),
    pl.col("CODR", "CFOP").cast(pl.Int64, strict=False),
)
empresas = consulta("bd_dlp", "SELECT CNPJ_EMP, EMPRESA FROM TB_EMPRESAS").with_columns(
    pl.col("CNPJ_EMP").cast(pl.Utf8)
)

clientes = clientes.with_columns(pl.concat_str(["CODCLI", "CNPJ_REV"]).alias("CHAVE_CLI"))
pedidos = pedidos.with_columns(pl.concat_str(["CODCLI", "CNPJ_REV"]).alias("CHAVE_CLI")).join(
    clientes.select("CHAVE_CLI", "FANTASIA", "CGC_CPF", "COD_CATEGORIA", "CANAL", "QUINZENAL"),
    on="CHAVE_CLI", how="left",
)
itens_pedido = itens.group_by("COD_PED", "CNPJ_REV").agg(
    pl.col("CODR").n_unique().alias("QTD_SKUS"),
    ((pl.col("CODR") == SKU_COMBO_BONIF) & pl.col("CFOP").is_in([5910, 1910])).sum().alias("TEM_COMBO_BONIF"),
    ((pl.col("CODR") == SKU_COMBO_VENDA) & ~pl.col("CFOP").is_in([5910, 1910])).sum().alias("TEM_COMBO_VEND"),
)
pedidos = pedidos.join(
    itens_pedido, left_on=["PEDIDO", "CNPJ_REV"], right_on=["COD_PED", "CNPJ_REV"], how="left"
).with_columns(
    pl.when(pl.col("STATUS").is_in([2, 7])).then(pl.lit("Bloqueado"))
    .when(pl.col("STATUS") == 9).then(pl.lit("Cancelado"))
    .otherwise(pl.col("STATUS").cast(pl.Utf8)).alias("STATUS"),
    pl.col("DT").cast(pl.Utf8).str.slice(0, 6).alias("MES"),
).unique()

valor_diario = pedidos.filter(
    pl.col("FANTASIA").is_not_null() & (pl.col("CGC_CPF") == "0000") & pl.col("OCO").is_in([1, 60])
).group_by("CODCLI", "FANTASIA", "DT", "CHAVE_CLI", "MES").agg(
    pl.col("VALOR").sum().alias("FAT_DIARIO")
).sort("CHAVE_CLI", "MES", "DT").with_columns(
    (pl.col("FAT_DIARIO").cum_sum().over("CHAVE_CLI", "MES") - pl.col("FAT_DIARIO")).alias("FAT_MENSAL"),
    pl.concat_str(["CHAVE_CLI", pl.col("DT").cast(pl.Utf8)]).alias("CHAVE_FAT_DIARIO"),
)

cancelados = pedidos.filter(
    pl.col("STATUS").is_in(["Bloqueado", "Cancelado"]) &
    pl.col("FANTASIA").is_not_null() & (pl.col("CGC_CPF") == "0000")
).with_columns(pl.concat_str(["CHAVE_CLI", pl.col("DT").cast(pl.Utf8)]).alias("CHAVE_FAT_DIARIO"))
historico_cpf = cancelados.join(
    valor_diario.select("CHAVE_FAT_DIARIO", "FAT_DIARIO", "FAT_MENSAL"),
    on="CHAVE_FAT_DIARIO", how="left",
).select(
    "CNPJ_REV", "CODCLI", "FANTASIA", "PED_ORIGEM", "PEDIDO", "DT", "VALOR", "FAT_DIARIO", "FAT_MENSAL"
).unique().with_columns(
    pl.when(pl.col("CNPJ_REV") == CNPJ_LIMITE_MENOR).then(12000.0).otherwise(30000.0).alias("LIMITE_CPF")
).with_columns(
    pl.when(pl.col("FAT_DIARIO") + pl.col("FAT_MENSAL") > pl.col("LIMITE_CPF"))
    .then(pl.lit("SIM")).otherwise(pl.lit("NAO")).alias("ATINGE_LIMITE")
).filter(pl.col("ATINGE_LIMITE") == "SIM")

grupo_2 = ["B3", "B4", "D8", "E7", "E8", "E9", "J1", "J2", "J3"]
grupo_3 = ["K1", "L1", "L2", "L3", "L4", "L5", "L6", "L7"]
cancelados_sku = pedidos.with_columns(pl.col("CANAL").str.strip_chars()).filter(
    pl.col("STATUS").is_in(["Bloqueado", "Cancelado"]) & pl.col("OCO").is_in([1, 7]) &
    (
        (pl.col("COD_CATEGORIA").is_in(grupo_2) & (pl.col("QTD_SKUS") < 2)) |
        (pl.col("COD_CATEGORIA").is_in(grupo_3) & (pl.col("QTD_SKUS") < 3)) |
        (~pl.col("COD_CATEGORIA").is_in(grupo_2 + grupo_3) & (pl.col("QTD_SKUS") <= 1))
    )
).select("CNPJ_REV", "CODCLI", "FANTASIA", "PED_ORIGEM", "CANAL", "PEDIDO", "DT", "VALOR", "QTD_SKUS").unique()

cancelados_combo = pedidos.filter(
    pl.col("STATUS").is_in(["Bloqueado", "Cancelado"]) &
    (pl.col("TEM_COMBO_BONIF") >= 1) & (pl.col("TEM_COMBO_VEND") == 0)
).select("CNPJ_REV", "CODCLI", "FANTASIA", "PED_ORIGEM", "PEDIDO", "DT", "VALOR").unique()

cancelados_fora_rota = pedidos.filter(
    pl.col("STATUS").is_in(["Bloqueado", "Cancelado"]) & (pl.col("FORA_ROTA") == "F")
).with_columns(
    pl.col("DT").cast(pl.Utf8).str.to_date("%Y%m%d", strict=False).dt.weekday().alias("DIA_PEDIDO"),
    pl.col("QUINZENAL").cast(pl.Utf8).str.slice(-1).cast(pl.Int64, strict=False).alias("QUINZENA_INV"),
).filter(
    (pl.col("DIA_PEDIDO") != pl.col("QUINZENA_INV")) & (pl.col("REPROGRAMADO") != "ReP")
).select("CNPJ_REV", "CODCLI", "FANTASIA", "PED_ORIGEM", "CANAL", "PEDIDO", "DT", "VALOR").unique()

tabelas = {
    "tb_historico_dlp": historico_cpf,
    "tb_historico_dlp_sku": cancelados_sku,
    "tb_historico_dlp_combo": cancelados_combo,
    "tb_historico_dlp_fora_rota": cancelados_fora_rota,
}
for nome, tabela in tabelas.items():
    sobrescreve_mysql(nome, tabela, "bd_dlp")
    sobrescreve_parquet(nome, _com_empresa(tabela, empresas), PASTA_PARQUET)
