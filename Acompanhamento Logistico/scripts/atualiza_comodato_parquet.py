"""Le a camada intermediaria (bd_comodato, gravada por
atualiza_comodato_intermediario.py, mesma pasta) e materializa os .parquet que
as paginas de Comodatos de fato leem (nunca consultam bd_comodato ao vivo).

Nome trocado de "atualiza_comodatos.py" pra "..._parquet.py" no original -- o
"s" a mais no nome antigo era a unica diferenca visivel em relacao ao passo
intermediario, levando a rodar so' um dos dois achando que bastava.

DEMONSTRACAO: bd_comodato e' uma pasta de Parquet consultada pelo DuckDB
(core/banco.py). Ajustes de dialeto MySQL -> DuckDB anotados em cada ponto
(crase vira aspas duplas; CAST explicito onde o MySQL convertia sozinho). Ficou
de fora a manutencao de schema que o original faz aqui (indice em tb_anexos_cev
e coluna gerada em tb_comod_email), que so' existe no MySQL.
"""

import sys
from pathlib import Path

import polars as pl

RAIZ = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(RAIZ))

from core.banco import consulta as consulta_banco  # noqa: E402
from core.parquet import sobrescreve_parquet  # noqa: E402

# Idioma/codepage do Windows varia por maquina -- sem isso, acentos podem sair
# como caractere errado ou quebrar com erro.
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

PASTA_APP = Path(__file__).resolve().parent.parent
PASTA_PARQUET = PASTA_APP / "data" / "bd_comodato"


# Modo por revenda: "python atualiza_comodato_parquet.py <CNPJ>" refaz o cache
# so' daquela revenda. Sem argumento, refaz tudo (a madrugada). Existe porque o
# botao "Atualizar" da tela e' por revenda e terminava chamando este script SEM
# filtro, regravando todas -- com tb_comodato em 100 mil linhas isso passou de
# meia hora por clique.
CNPJ_REVENDA = sys.argv[1].strip() if len(sys.argv) > 1 else None
if CNPJ_REVENDA and not CNPJ_REVENDA.isdigit():
    raise SystemExit(f"CNPJ invalido: {CNPJ_REVENDA!r} (esperado so digitos)")


def filtro_revenda(alias: str = "C", ja_tem_where: bool = False) -> str:
    """Clausula de revenda pra encaixar em cada consulta. Vazia no modo
    completo, pra a consulta sair exatamente como sempre foi."""
    if not CNPJ_REVENDA:
        return ""
    ligacao = "AND" if ja_tem_where else "WHERE"
    coluna = f"{alias}.CNPJ_REV" if alias else "CNPJ_REV"
    return f" {ligacao} {coluna} = '{CNPJ_REVENDA}'"


def consulta(sql: str) -> pl.DataFrame:
    return consulta_banco("bd_comodato", sql)


# Produtos fixos (livro fiscal / Vasilhames) -- as mesmas listas da camada
# intermediaria.
CAIXA_1L = ["700101", "700102"]
CAIXA_300ML = ["700201", "700202", "700203", "700204", "700205"]
CAIXA_600ML_PREMIUM = ["700401", "700402"]
EMPRESTIMO_BLOQ = ["700301", "700302", "700303", "700304", "700305", "700306"]
PRODUTOS_LIVRO = CAIXA_1L + CAIXA_300ML + CAIXA_600ML_PREMIUM + EMPRESTIMO_BLOQ

# Um registro de anexo por (CNPJ_REV, ncev) -- o mais recente. tb_anexos_cev nao
# tem chave unica e tem CEV com 2 registros: o LEFT JOIN direto duplicava a
# linha do comodato e a soma por CEV saia dobrada na tela.
ANEXOS_UNICOS = """(
    SELECT a.* FROM tb_anexos_cev a
    JOIN (
        SELECT CNPJ_REV, ncev, MAX(id) AS id
        FROM tb_anexos_cev WHERE situacao <> 'X'
        GROUP BY CNPJ_REV, ncev
    ) ultimo ON ultimo.id = a.id
)"""

# ncev e' varchar e TB_COMODATO.CEV e' numero: sem o CAST o MySQL converte os
# DOIS lados pra double, nenhum indice serve, e a derivada acima e' varrida
# inteira PARA CADA linha de comodato (medido no original: mais de 7 minutos na
# maior revenda; com o CAST do lado de FORA, 1,2s). No MySQL vai ainda um
# "COLLATE utf8mb4_unicode_ci" (CAST(... AS CHAR) sai na collation da conexao);
# no DuckDB o CAST basta.
JUNCAO_ANEXO = "ON A.CNPJ_REV = C.CNPJ_REV AND A.ncev = CAST(C.CEV AS VARCHAR)"

# ==========================================================================
# comod_geral (aba "Geral") -- traz todas as revendas de uma vez, com a coluna
# EMPRESA crua a mais pra o app filtrar por revenda depois de ler o .parquet
# (coluna interna, existe so' pra filtro).
# ==========================================================================

comod_geral_bruto = consulta("""
    SELECT
         E.EMPRESA AS EMPRESA,
         CONCAT(E.EMPRESA, ' - ', C.CEV) AS CEV,
         c.CODCOMP,
         C.NF,
         c.CODCLI AS Cliente,
         c.FANTASIA AS Fantasia,
         c.NOME AS "Razão Social",
         c.DTE AS "Data Emissão",
         c.DTV AS "Data Vencimento",
         c.Intervalo AS "Intervalo Vencimento",
         c.PRODUTO_GENERICO AS Produto,
         c.QTD AS Qtd,
         c.Inadimplente AS "Inadimplencia 60 dias +",
         c."Prazo Envio",
         a.usuario AS Usuario,
         a.dataI,
         a.anexo1,
         a.anexo2,
         a.anexo3,
         a.situacao,
         c.CNPJ_REV AS cnpj,
         -- Vendedor do cliente: TB_COMODATO nao tem essa coluna, e ela e'
         -- necessaria pra reconhecer o cliente de EXCECAO por vendedor. O join
         -- com TB_CLIENTES_INAD ja' existia; e' so' trazer mais um campo dele.
         i.VEN AS VEN,
         i.CHAVE AS CNPJ_CLI,
         i.ENDERECO,
         i.CEP,
         CASE WHEN c.PRE_PED IS NOT NULL THEN 'Dash' ELSE 'ERP' END AS Origem
    FROM TB_COMODATO C
    LEFT JOIN TB_EMPRESAS E ON E.CNPJ_EMP = C.CNPJ_REV
    LEFT JOIN """ + ANEXOS_UNICOS + """ A """ + JUNCAO_ANEXO + """
    LEFT JOIN TB_CLIENTES_INAD I ON I.CNPJ_REV = C.CNPJ_REV AND C.CODCLI = I.CODCLI
    WHERE C.QTD > 0
""" + filtro_revenda("C", ja_tem_where=True))

# ==========================================================================
# saldo_geral (aba "Vasilhames")
# ==========================================================================

saldo_comodatos_geral_bruto = consulta("""
    SELECT
         E.EMPRESA AS Revenda,
         c.CODCOMP,
         c.Inadimplente,
         c."Prazo Envio",
         -- Emissao e intervalo: e' por eles que se sabe se o CEV pede dois ou
         -- tres documentos pra contar como entregue.
         c.DTE AS "Data Emissão",
         c.Intervalo AS "Intervalo Vencimento",
         a.dataI,
         a.situacao,
         c.CNPJ_REV AS cnpj_rev,
         c.CEV AS CEV
    FROM TB_COMODATO C
    LEFT JOIN TB_EMPRESAS E ON E.CNPJ_EMP = C.CNPJ_REV
    LEFT JOIN """ + ANEXOS_UNICOS + """ A """ + JUNCAO_ANEXO + """
""" + filtro_revenda("C"))

produtos_sql = ", ".join(f"'{p}'" for p in PRODUTOS_LIVRO)
# Sem filtro de revenda de proposito, nos DOIS modos: este arquivo nao e'
# particionado (a tela le ele inteiro), entao gravar so' uma revenda apagaria
# as outras. (DATA e' texto AAAAMMDD: o MySQL compara com o numero sozinho, o
# DuckDB pede o CAST.)
saldo_livro_bruto = consulta(
    f"SELECT * FROM tb_livro_fiscal WHERE CAST(data AS BIGINT) >= 20250101 AND produto IN ({produtos_sql})"
)

# ==========================================================================
# clientes_inadimplentes (aba "Solicitação")
# ==========================================================================

# P.CODCOMP e' texto ("700501") e C.CODCOMP e' numero (ver o comentario de
# CODCOMP na camada intermediaria): o MySQL converte sozinho, o DuckDB pede o
# TRY_CAST.
clientes_inadimplentes_base_bruto = consulta("""
    SELECT
         E.EMPRESA AS Revenda,
         c.CODCOMP,
         c.CEV,
         c.CODCLI AS Cliente,
         c.FANTASIA AS Fantasia,
         c.NOME AS "Razão Social",
         c.DTE AS "Data Emissão",
         c.DTV AS "Data Vencimento",
         c.Intervalo AS "Intervalo Vencimento",
         c.PRODUTO_GENERICO AS Produto,
         c.QTD AS Qtd,
         c.Inadimplente AS "Inadimplencia 60 dias +",
         c."Prazo Envio",
         a.usuario AS Usuario,
         a.dataI,
         -- Alias explicito: o MySQL devolve o nome como foi escrito na consulta
         -- ("cnpj_rev"); o DuckDB devolve o da tabela ("CNPJ_REV"), e a
         -- particao por revenda procura "cnpj_rev".
         c.CNPJ_REV AS cnpj_rev,
         a.anexo1, a.anexo2, a.anexo3,
         a.situacao,
         CONCAT(c.codcli, c.cnpj_rev) AS CHAVE_CLI,
         p.TIPO
    FROM TB_COMODATO C
    LEFT JOIN TB_PRODUTOS_CEV P ON TRY_CAST(P.CODCOMP AS BIGINT) = C.CODCOMP AND C.CNPJ_REV = P.CNPJ_REV
    LEFT JOIN TB_EMPRESAS E ON E.CNPJ_EMP = C.CNPJ_REV
    LEFT JOIN """ + ANEXOS_UNICOS + """ A """ + JUNCAO_ANEXO + """
    WHERE C.QTD > 0
""" + filtro_revenda("C", ja_tem_where=True))

clientes_inadimplentes_titulos_bruto = consulta("SELECT * FROM tb_clientes_inad" + filtro_revenda(""))

# ==========================================================================
# produtos_cev (catalogo de item que pode ser pedido no modal "Solicitar
# comodato") -- cacheia todas as revendas; o filtro por revenda acontece em
# Python na hora de ler.
# ==========================================================================

produtos_cev_bruto = consulta("SELECT * FROM tb_produtos_cev WHERE estoque = 'SIM'")

# ==========================================================================
# Gravacao em .parquet
# ==========================================================================

# Tabela grande sai tambem em um arquivo por revenda: a tela le so' o da
# propria. Com o periodo estendido ate' 2020 estas passaram de ~10 mil pra mais
# de 100 mil linhas, e abrir tudo pra usar uma revenda ficou caro. O arquivo
# unico continua sendo gravado (e' a reserva de quem nao tem particao). As
# tabelas pequenas -- produtos, livro -- ficam so' no arquivo unico.
PARTICIONADAS = {
    "comod_geral_bruto": "cnpj",
    "saldo_comodatos_geral_bruto": "cnpj_rev",
    "clientes_inadimplentes_base_bruto": "cnpj_rev",
    "clientes_inadimplentes_titulos_bruto": "CNPJ_REV",
}


def grava(nome: str, tabela) -> None:
    coluna = PARTICIONADAS.get(nome)
    # Tabela sem particao: a consulta dela ignora o filtro de revenda, entao o
    # arquivo unico ja' vem completo e pode ser regravado nos dois modos.
    if coluna is None:
        sobrescreve_parquet(nome, tabela, PASTA_PARQUET)
        return
    # No modo por revenda a consulta trouxe SO' uma revenda: reescrever o
    # arquivo unico apagaria as outras. Grava so' a particao dela.
    if not CNPJ_REVENDA:
        sobrescreve_parquet(nome, tabela, PASTA_PARQUET)
    if tabela.height == 0 or coluna not in tabela.columns:
        return
    for chave, parte in tabela.partition_by(coluna, as_dict=True).items():
        cnpj = chave[0] if isinstance(chave, tuple) else chave
        if cnpj:
            sobrescreve_parquet(f"{nome}_{cnpj}", parte, PASTA_PARQUET)


grava("comod_geral_bruto", comod_geral_bruto)
grava("saldo_comodatos_geral_bruto", saldo_comodatos_geral_bruto)
grava("saldo_livro_bruto", saldo_livro_bruto)
grava("clientes_inadimplentes_base_bruto", clientes_inadimplentes_base_bruto)
grava("clientes_inadimplentes_titulos_bruto", clientes_inadimplentes_titulos_bruto)
grava("produtos_cev_bruto", produtos_cev_bruto)
