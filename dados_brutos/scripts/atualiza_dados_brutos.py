"""Camada bruta (1a etapa do pipeline, sem dependencia de nenhum outro
script): le o ERP de cada revenda e grava cliente, produto, preco, pedido e
itens de venda em "bd_shiny_bruto". Tambem busca a hierarquia de vendedores
numa API externa (tb_funcionarios), que nao depende do ERP.

Todo o resto do pipeline depende das tabelas que este script grava:
atualiza_comodatos_bruto.py aqui do lado, a camada intermediaria de Comodatos
(Acompanhamento Logistico) e os scripts do Acompanhamento Comercial.

Cada revenda roda numa thread propria (ThreadPoolExecutor) -- no original, a
consulta ODBC e' dominada por espera de rede, que libera o GIL. Cada revenda
roda dentro do proprio try/except: uma revenda com o ERP fora do ar nao derruba
as outras, e as linhas dela na base local ficam como estavam (ver
gravacao_bruta.py).

DEMONSTRACAO: o ERP e' `dados/origem/erp/<revenda>/<consulta>.parquet` (as
colunas de cada arquivo sao as da consulta em consultas_erp.py) e o MySQL e' a
pasta `dados/bancos/bd_shiny_bruto/`. No original, ficaram de fora daqui:
vendas_e_dev, motoristas, vendedores, romaneios, condicao de pagamento e metas
(mesmo padrao das tabelas abaixo), e a correcao de uma revenda que divide o ERP
com outra (reetiqueta linhas por faixa de vendedor).
"""

import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path

import polars as pl
from dateutil.relativedelta import relativedelta

RAIZ = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(RAIZ))

from core import banco as bancos  # noqa: E402
from core import revendas  # noqa: E402
from core.parquet import sobrescreve_mysql  # noqa: E402
from gravacao_bruta import (  # noqa: E402
    BANCO,
    atualiza_tabela_janela_recente,
    atualiza_tabela_por_cnpj,
    meses_existentes_local,
    meses_recentes,
    podar_tabela_antiga,
)

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

EMPRESAS = revendas.para_erp(campos=("cnpj", "nome", "slug", "licenca"))

hoje = date.today()
filtro_data_2 = (hoje - relativedelta(months=6)).strftime("%Y%m")

# Janela que as tabelas com recorte de tempo pedem a' origem e regravam a cada
# execucao, CONTANDO o mes atual -- 6 = mes atual + os 5 anteriores. As funcoes
# contam "meses_atras" ALEM do mes atual -- dai o -1.
MESES_JANELA = 6
MESES_ATRAS_JANELA = MESES_JANELA - 1

# Prazo maximo esperando as revendas responderem antes de desistir das que
# faltarem e seguir so' com quem ja' respondeu. No original, pyodbc.connect()
# chegou a travar uma thread pra sempre num DSN sem resposta, mesmo com timeout=.
TIMEOUT_TOTAL_SEGUNDOS = 4 * 60 * 60


def _so_meses(df: pl.DataFrame, meses: set[str], coluna_mes: str = "MES") -> pl.DataFrame:
    """Faz o papel da clausula de mes no WHERE da consulta ao ERP (ver
    consultas_erp.py): pede so' os meses que de fato faltam."""
    if df.height == 0:
        return df
    return df.filter(pl.col(coluna_mes).is_in(list(meses)))


def processar_empresa(empresa: dict) -> dict[str, pl.DataFrame] | None:
    nome = empresa["nome"]
    try:
        clientes = bancos.consulta_erp(empresa, "clientes")
        print(f"  Conectou na empresa {nome}")
        produtos = bancos.consulta_erp(empresa, "produtos")
        preco_prod = bancos.consulta_erp(empresa, "preco_prod")

        # Filtro dinamico -- pergunta a base LOCAL o que ja' tem antes de pedir
        # a' origem: so' a janela recente + mes que falta de verdade. Um buraco
        # isolado no meio do historico pede SO' aquele mes, nao "tudo desde o mes
        # mais antigo que falta" (1a versao, corrigida no original).
        recentes = set(meses_recentes(MESES_ATRAS_JANELA))

        def meses_para_buscar(nome_tabela: str) -> set[str]:
            existentes = meses_existentes_local(nome_tabela, empresa["cnpj"], filtro_data_2)
            todos = {m for m in _meses_desde(filtro_data_2)}
            return (todos - existentes) | recentes

        pedidos = bancos.consulta_erp(empresa, "pedidos")
        if pedidos.height:
            pedidos = pedidos.with_columns(pl.col("DT").cast(pl.Utf8).str.slice(0, 6).alias("MES"))
            pedidos = _so_meses(pedidos, meses_para_buscar("tb_pedidos"))
        vend_dev_itens = bancos.consulta_erp(empresa, "vend_dev_itens")
        if vend_dev_itens.height:
            # vend_dev_itens NAO tem coluna de data propria -- so' COD_PED, que e'
            # o NIT do pedido (6 primeiros digitos = ano+mes).
            vend_dev_itens = vend_dev_itens.with_columns(pl.col("COD_PED").cast(pl.Utf8).str.slice(0, 6).alias("MES"))
            vend_dev_itens = _so_meses(vend_dev_itens, meses_para_buscar("tb_vend_dev_itens"))

        tabelas = {
            "clientes": clientes,
            "produtos": produtos,
            "preco_prod": preco_prod,
            "pedidos": pedidos,
            "vend_dev_itens": vend_dev_itens,
        }
        for chave, df in tabelas.items():
            if df.height > 0:
                tabelas[chave] = df.with_columns(
                    pl.lit(empresa["cnpj"]).alias("CNPJ_REV"),
                    pl.lit(nome).alias("REVENDA"),
                    pl.lit(empresa["licenca"]).alias("LICENCA"),
                )

        print(f"  Concluiu {nome}: {tabelas['clientes'].height} clientes, {tabelas['pedidos'].height} pedidos")
        return tabelas

    except Exception as exc:
        print(f"  ERRO na empresa {nome}: {exc}")
        return None


def _meses_entre(inicio: str, fim: date) -> list[str]:
    atual = date(int(inicio[:4]), int(inicio[4:]), 1)
    meses = []
    while atual <= fim:
        meses.append(atual.strftime("%Y%m"))
        atual += relativedelta(months=1)
    return meses


def _meses_desde(inicio: str) -> list[str]:
    return _meses_entre(inicio, hoje)


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
    # Nao espera thread travada (no original: conexao ODBC presa num DSN sem
    # resposta, e as threads do ThreadPoolExecutor nao sao daemon).
    executor.shutdown(wait=False, cancel_futures=True)

    if not resultados:
        print("Nenhuma revenda conectou -- nada a gravar.")
        return

    def junta(chave: str) -> pl.DataFrame:
        partes = [r[chave] for r in resultados if r[chave].height > 0]
        if not partes:
            return pl.DataFrame()
        return pl.concat(partes, how="diagonal_relaxed")

    clientes_completo = junta("clientes")
    produtos_completo = junta("produtos")
    preco_completo = junta("preco_prod")
    vend_dev_itens_completo = junta("vend_dev_itens")
    pedidos_completo = junta("pedidos")
    pedidos_completo = pedidos_completo.unique() if pedidos_completo.height > 0 else pedidos_completo

    # Grava em bd_shiny_bruto -- so' as linhas das revendas que responderam.
    atualiza_tabela_por_cnpj("tb_clientes", cnpjs_ok, clientes_completo)
    atualiza_tabela_por_cnpj("tb_produtos", cnpjs_ok, produtos_completo)
    atualiza_tabela_por_cnpj("tb_precos_prod", cnpjs_ok, preco_completo)

    # Alto volume: so' a janela recente + mes que falta; a poda do que passou
    # da retencao roda a parte.
    atualiza_tabela_janela_recente(
        "tb_vend_dev_itens", cnpjs_ok, vend_dev_itens_completo, "MES",
        meses_atras=MESES_ATRAS_JANELA, corte_fixo=filtro_data_2,
    )
    atualiza_tabela_janela_recente(
        "tb_pedidos", cnpjs_ok, pedidos_completo, "MES",
        meses_atras=MESES_ATRAS_JANELA, corte_fixo=filtro_data_2,
    )
    podar_tabela_antiga("tb_vend_dev_itens", "MES", meses_retencao=6)
    podar_tabela_antiga("tb_pedidos", "MES", meses_retencao=6)

    # Hierarquia de vendedores -- API externa, sem relacao com o ERP. Falha
    # aqui nao desfaz nada do que ja' foi gravado acima.
    buscar_hierarquia_vendedores()

    print("atualiza_dados_brutos.py finalizado.")


def buscar_hierarquia_vendedores() -> None:
    """No original: requests.get(URL, headers={"x-api-key": ...}) numa Edge
    Function (URL e chave no .env). Aqui o "registro devolvido" vem de
    dados/origem/api/hierarquia_vendedores.parquet."""
    try:
        funcionarios = bancos.consulta_api("hierarquia_vendedores")
        funcionarios.columns = [c.upper() for c in funcionarios.columns]
        if "UPDATED_AT" in funcionarios.columns:
            funcionarios = funcionarios.drop("UPDATED_AT")
        # dict.rename() do polars quebra se a chave nao existir na tabela
        # (diferente do "names(df)[names(df)==X] <- Y" do R, que so' ignora em
        # silencio) -- so' renomeia o que existir de verdade.
        mapa_renomeio = {k: v for k, v in {"EMPRESA": "REVENDA"}.items() if k in funcionarios.columns}
        funcionarios = funcionarios.rename(mapa_renomeio)
        sobrescreve_mysql("tb_funcionarios", funcionarios, BANCO)
    except Exception as exc:
        print(f"Erro ao buscar hierarquia de vendedores: {exc}")


if __name__ == "__main__":
    main()
