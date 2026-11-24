"""Gera os Parquet "de origem" FICTICIOS que substituem o ERP e as APIs.

Cada arquivo tem as MESMAS colunas que a consulta original devolvia (os nomes
das colunas sao os aliases do SELECT em dados_brutos/scripts/*.py). Nenhum dado
aqui e' real: revendas, clientes, CNPJs, CPFs, telefones, e-mails e enderecos
sao inventados -- CNPJ/CPF propositalmente com cara de falso (zeros, noves).

As datas sao RELATIVAS A HOJE (pedidos dos ultimos meses, comodatos emitidos
nos ultimos dias), entao rode de novo antes de uma demonstracao se os arquivos
tiverem sido gerados ha' muito tempo:

    python dados/gerar_dados_ficticios.py

Saida:
    dados/origem/erp/<revenda>/<consulta>.parquet  -- o "ERP" de cada revenda
    dados/origem/api/hierarquia_vendedores.parquet -- a "API" de hierarquia
    dados/origem/telas/tb_anexos_cev.parquet       -- o que a tela de Comodatos
                                                      grava (anexos do CEV)
"""

import random
import shutil
import sys
from datetime import date, timedelta
from pathlib import Path

import polars as pl

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ))

from core import revendas  # noqa: E402

PASTA_ORIGEM = RAIZ / "dados" / "origem"
hoje = date.today()
rnd = random.Random(2026)

# Produtos de comodato (materiais e vasilhames). Os codigos de vasilhame sao os
# que as regras de saldo usam (CAIXA_1L, CAIXA_300ML, ...) -- ver
# Acompanhamento Logistico/scripts/atualiza_comodato_intermediario.py.
PRODUTOS = [
    # CODCOMP, CODRED, PRODUTO, PRODUTO_GENERICO, TIPO
    ("700101", 700101, "CAIXA PLASTICA 1L 12UN", "CAIXA PLASTICA 1L", "TAKE HOME"),
    ("700102", 700102, "GARRAFA VIDRO 1L 12UN", "GARRAFA 1L", "TAKE HOME"),
    ("700203", 700203, "CAIXA PLASTICA 300ML 24UN", "CAIXA PLASTICA 300ML", "IMPULSE"),
    ("700201", 700201, "GARRAFA 300ML 24UN", "GARRAFA 300ML", "IMPULSE"),
    ("700401", 700401, "CAIXA PREMIUM 600ML 24UN", "CAIXA PREMIUM 600ML", "IMPULSE"),
    ("700305", 700305, "CAIXA EMPRESTIMO 600ML 24UN", "CAIXA EMPRESTIMO 600ML", "IMPULSE"),
    ("700301", 700301, "CAIXA EMPRESTIMO 600ML 12UN", "CAIXA EMPRESTIMO 600ML B", "IMPULSE"),
    ("700501", 700501, "REFRIGERADOR VERTICAL 1 PORTA", "REFRIGERADOR 1 PORTA", "IMPULSE"),
    ("700502", 700502, "REFRIGERADOR VERTICAL 2 PORTAS", "REFRIGERADOR 2 PORTAS", "IMPULSE"),
    ("700503", 700503, "JOGO MESA E 4 CADEIRAS", "MESA E CADEIRAS", "IMPULSE"),
    ("700504", 700504, "CHOPEIRA ELETRICA 1 TORNEIRA", "CHOPEIRA", "IMPULSE"),
    ("700505", 700505, "CAIXA TERMICA 50L", "CAIXA TERMICA", "IMPULSE"),
]
# Produtos de venda (os que entram nos pedidos). 800001/800002 sao os dois SKUs
# da regra de COMBO de Pedidos Cancelados.
PRODUTOS_VENDA = [
    ("101001", 800001, "CERVEJA PILSEN LATA 350ML 12UN", "PILSEN LATA 350ML", "TAKE HOME"),
    ("101002", 800002, "CERVEJA PURO MALTE LONG NECK 330ML 24UN", "PURO MALTE LN 330ML", "IMPULSE"),
    ("101003", 900101, "CERVEJA PILSEN GARRAFA 600ML 24UN", "PILSEN 600ML", "IMPULSE"),
    ("101004", 900102, "CERVEJA PILSEN GARRAFA 1L 12UN", "PILSEN 1L", "TAKE HOME"),
    ("102001", 900201, "REFRIGERANTE COLA PET 2L 6UN", "REFRI COLA 2L", "TAKE HOME"),
    ("103001", 900301, "AGUA MINERAL 500ML 12UN", "AGUA 500ML", "IMPULSE"),
]

CANAIS = [("BAR", "B3"), ("RESTAURANTE", "D8"), ("MERCEARIA", "K1"), ("SUPERMERCADO", "L2"), ("CONVENIENCIA", "A1")]
PORTES = [("D", "Diamante", 2.5), ("O", "Ouro", 2.0), ("P", "Prata", 1.7), ("B", "Bronze", 1.4)]
CIDADES = ["CIDADE ALFA", "CIDADE BETA", "CIDADE GAMA", "CIDADE DELTA"]
NOMES_FANTASIA = ["BAR DO PONTO", "MERCADINHO CENTRAL", "RESTAURANTE SABOR", "CONVENIENCIA 24H",
                  "BAR ESQUINA", "SUPERMERCADO BOM PRECO", "LANCHONETE PRACA", "EMPORIO VILA"]


def aaaammdd(dia: date) -> int:
    return int(dia.strftime("%Y%m%d"))


def grava(df: pl.DataFrame, *partes: str) -> None:
    caminho = PASTA_ORIGEM.joinpath(*partes)
    caminho.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(caminho)


def gera_revenda(indice: int, revenda: revendas.Revenda) -> dict:
    vendedores = [100 * (indice + 1) + v for v in range(1, 5)] + [200 + indice * 10 + 1]
    # --- clientes ---------------------------------------------------------
    clientes = []
    for n in range(60):
        codcli = f"{indice + 1}{n + 1:04d}"
        pessoa_fisica = n % 2 == 0
        chave = f"{n:09d}{indice:02d}" if pessoa_fisica else f"99{indice:02d}{n:06d}0001"
        canal, cod_categoria = CANAIS[n % len(CANAIS)]
        porte_cod, porte, acrescimo = PORTES[n % len(PORTES)]
        clientes.append({
            # Mesma derivacao da consulta: 4 digitos antes dos 2 ultimos do
            # CNPJ/CPF -- "0000" so' sai de CPF (aqui, de proposito, sempre).
            "CGC_CPF": "0000" if pessoa_fisica else chave[-6:-2],
            "FANTASIA": f"{NOMES_FANTASIA[n % len(NOMES_FANTASIA)]} {n + 1}",
            "CHAVE": chave,
            "CANAL": canal,
            "COD_CATEGORIA": cod_categoria,
            "CIDADE": CIDADES[n % len(CIDADES)],
            "CODCLI": codcli,
            "PORTE": porte,
            "ACRESCIMO": acrescimo,
            "VEN": vendedores[n % len(vendedores)],
            "NOME": f"CLIENTE FICTICIO {indice + 1}-{n + 1:03d} LTDA",
            "TPCOBRANCA": 4 if n % 3 else 1,
            "CPG": 7 + (n % 3) * 7,
            "BANCO": 0,
            "LIMITE_CREDITO": float(1000 * (1 + n % 10)),
            "ENDERECO": f"RUA EXEMPLO, {100 + n} - CENTRO - {CIDADES[n % len(CIDADES)]} - {revenda.uf}",
            "CONTATO": f"CONTATO {n + 1}",
            "PTO_REF": "PROXIMO A PRACA",
            "DIA_VISIT": str(n % 5 + 1),
            "SEQ": str(n + 1),
            "CEP": "00000-000",
            "FONE": f"(00) 90000-{n:04d}",
            "PROPRIETARIO": f"PROPRIETARIO FICTICIO {n + 1}",
            "CPF_PROP": f"000000{n:03d}{indice:02d}" if n % 4 else "",
            "NASC_PROP": 19800101,
            # Sem e-mail em parte dos clientes: e' o que vira TEM_CAD = "Não".
            "EMAIL_PROP": f"cliente{indice + 1}{n + 1:03d}@example.com" if n % 5 else " ",
            "QUINZENAL": 10 + (n % 5) + 1,
            "LATITUDE": -5.0 - indice - n / 1000,
            "LONGITUDE": -38.0 - indice - n / 1000,
            "COD_IBGE": 0,
        })
    clientes_df = pl.DataFrame(clientes)

    # --- produtos / preco / estoque ----------------------------------------
    produtos_df = pl.DataFrame([
        {"CODCOMP": c, "CODRED": r, "PRODUTO": p, "PRODUTO_GENERICO": g, "UNIDADE": 1, "LITRAGEM": "",
         "TIPO": t, "TIPO_2": "", "GRUPO": 1, "NCM": "00000000", "CEV": "S", "COD_PROD_FORN": "", "MARCA": "MARCA FICTICIA"}
        for c, r, p, g, t in PRODUTOS + PRODUTOS_VENDA
    ])
    preco_df = pl.DataFrame([
        {"CODCOMP": c, "PRECO_UN": round(rnd.uniform(5, 900), 2), "PRECO": 0.0, "TAB_CEV": f"{hoje:%Y%m}99"}
        for c, *_ in PRODUTOS + PRODUTOS_VENDA
    ])
    estoque_df = pl.DataFrame([{"CODCOMP": c, "ESTOQUE": "SIM"} for c, *_ in PRODUTOS if rnd.random() > 0.2])

    # --- pedidos + itens -----------------------------------------------------
    pedidos, itens = [], []
    seq = 0
    for _ in range(260):
        cli = rnd.choice(clientes)
        # Uns poucos clientes CPF concentram pedido grande -- e' o que faz a
        # regra do "limite CPF" de Pedidos Cancelados ter o que mostrar.
        grande = cli["CGC_CPF"] == "0000" and int(cli["CODCLI"][-3:]) % 5 == 1
        # ...e compram sempre nos mesmos poucos dias do mes, que e' onde o limite
        # do dia + acumulado do mes estoura.
        dia = (hoje.replace(day=1) + timedelta(days=rnd.choice([1, 4, 8, 12]))) if grande else hoje - timedelta(days=rnd.randint(0, 100))
        if dia.weekday() == 6:
            dia -= timedelta(days=1)
        seq += 1
        pedido = int(f"{dia:%Y%m%d}{seq:04d}")
        oco = rnd.choices([1, 60, 2, 4, 7], weights=[60, 5, 12, 5, 8])[0]
        status = rnd.choices([8, 0, 2, 7, 9], weights=[70, 10, 6, 4, 10])[0]
        valor = round(rnd.uniform(6000, 14000) if grande else rnd.uniform(150, 2500), 2)
        pedidos.append({
            "PEDIDO": pedido,
            "DT": aaaammdd(dia),
            "ROMANEIO": int(f"{dia:%Y%m}{rnd.randint(1, 300):05d}"),
            "CODCLI": cli["CODCLI"],
            "COD_VENDEDOR": cli["VEN"],
            "TIPO_COBRANCA": cli["TPCOBRANCA"],
            "OCO": oco,
            "PED_ORIGEM": rnd.choice(["ERP", "SFA", "Portal"]),
            "NOTA_FISCAL": 100000 + seq,
            "VALOR": valor,
            "CHAVE_NFE": "",
            "VENC": aaaammdd(dia + timedelta(days=14)),
            "STATUS": status,
            "FORA_ROTA": "F" if rnd.random() < 0.2 else "D",
            "REPROGRAMADO": "ReP" if rnd.random() < 0.1 else "usuario01",
            "PAG": "",
        })
        produtos_pedido = rnd.sample(PRODUTOS_VENDA, k=rnd.randint(1, 4))
        for _c, codr, _p, generico, _t in produtos_pedido:
            bonif = oco in (2, 4)
            qtd = rnd.randint(1, 20)
            preco_item = round(valor / len(produtos_pedido), 2)
            itens.append({
                "COD_PED": pedido,
                "CODR": codr,
                "PRODUTO_GENERICO": generico,
                "CFOP": 5910 if bonif or (codr == 800001 and rnd.random() < 0.5) else 5102,
                "QTD": qtd,
                "DEVOLUCAO_CX": 0,
                "QTDS": qtd * 12,
                "VALOR VENDA": 0.0 if bonif else preco_item,
                "VALOR DEVOLUCAO": 0.0,
                "VALOR_BONIF": preco_item if bonif else 0.0,
                "VALOR_BONIF_DEV": 0.0,
                "QTD_BONIF": qtd if bonif else 0,
                "QTDS_BONIF": qtd * 12 if bonif else 0,
            })
    pedidos_df = pl.DataFrame(pedidos)
    itens_df = pl.DataFrame(itens)

    # --- titulos em aberto ----------------------------------------------------
    titulos = []
    for cli in rnd.sample(clientes, k=15):
        venc = hoje - timedelta(days=rnd.randint(-10, 90))
        titulos.append({
            "CODCLI": int(cli["CODCLI"]),
            "VEN": cli["VEN"],
            "TITULO_MAIS_ANTIGO": aaaammdd(venc),
            "VALOR": round(rnd.uniform(100, 3000), 2),
        })
    titulos_df = pl.DataFrame(titulos)

    # --- CEV (comodatos pendentes) + anexos ---------------------------------
    cevs, anexos = [], []
    materiais = [p for p in PRODUTOS if p[0].startswith("7005")]
    for n in range(28):
        cli = rnd.choice(clientes)
        # Metade emitida nos ultimos 15 dias (prazo de documento correndo: e' o
        # que o aviso do meio-dia cobra) e o resto mais antigo.
        dte = hoje - timedelta(days=rnd.randint(0, 15) if n % 2 == 0 else rnd.randint(16, 300))
        intervalo = 30 if n % 3 == 0 else 365
        numero_cev = 14000 + indice * 1000 + n
        nped = f"{dte:%Y%m}{n + 1:06d}"
        for item in rnd.sample(materiais, k=rnd.randint(1, 2)):
            cevs.append({
                "CODCLI": cli["CODCLI"],
                "CODCOMP": int(item[0]),
                "CEV": numero_cev,
                "MOT": rnd.randint(1, 9),
                "DTE": aaaammdd(dte),
                "DTV": aaaammdd(dte + timedelta(days=intervalo)),
                "QTD": rnd.randint(1, 3),
                "NF": 200000 + numero_cev,
                "SERIE": "1",
                "NPED": nped,
                "PRE_PED": f"PP{numero_cev}" if n % 4 == 0 else None,
                "ROMA": int(f"{dte:%Y%m}{n + 1:05d}"),
                "TIPO": "COMODATO",
                "STATUS": "PENDENTE",
            })
        # Documento: parte sem nada, parte so' com o contrato, parte completa.
        situacao_doc = n % 4
        if situacao_doc == 0:
            continue
        anexos.append({
            "id": indice * 1000 + n,
            "CNPJ_REV": revenda.cnpj,
            "ncev": str(numero_cev),
            "usuario": f"usuario{n % 3 + 1:02d}",
            "dataI": (dte + timedelta(days=1)).isoformat(),
            "anexo1": f"uploads/comodatos/{numero_cev}_pdv.pdf" if situacao_doc == 3 else None,
            "anexo2": f"uploads/comodatos/{numero_cev}_contrato.pdf",
            "anexo3": f"uploads/comodatos/{numero_cev}_canhoto.pdf" if situacao_doc >= 2 else None,
            "situacao": "A" if situacao_doc >= 2 else "B",
        })
    cev_df = pl.DataFrame(cevs, schema_overrides={"PRE_PED": pl.Utf8})

    # --- livro fiscal de vasilhame (entrada = devolucao, saida = emprestimo) --
    vasilhames = [p for p in PRODUTOS if not p[0].startswith("7005")]
    entrada, saida = [], []
    for mes_atras in range(0, 12):
        dia = (hoje.replace(day=1) - timedelta(days=30 * mes_atras)).replace(day=10)
        for item in vasilhames:
            for lista, operacoes in ((entrada, ("Dev Emprestimo", "Dev Venda")), (saida, ("Emprestei", "Vendi"))):
                cli = rnd.choice(clientes)
                qtd = rnd.randint(1, 40) * 12
                lista.append({
                    "NF": 300000 + len(lista),
                    "operacao": rnd.choice(operacoes),
                    "DATA": f"{dia:%Y%m%d}",
                    "CLI": cli["CODCLI"],
                    "PRODUTO": item[0],
                    "PRODUTO_GENERICO": item[3],
                    "QTD": qtd,
                    "QTDS": 0,
                    "VEN": cli["VEN"],
                })

    return {
        "clientes": clientes_df,
        "produtos": produtos_df,
        "preco_prod": preco_df,
        "pedidos": pedidos_df,
        "vend_dev_itens": itens_df,
        "titulos_abertos": titulos_df,
        "cev": cev_df,
        "estoque": estoque_df,
        "livro_entrada": pl.DataFrame(entrada),
        "livro_saida": pl.DataFrame(saida),
        "_anexos": pl.DataFrame(anexos),
        "_vendedores": vendedores,
    }


def main() -> None:
    if PASTA_ORIGEM.exists():
        shutil.rmtree(PASTA_ORIGEM)

    hierarquia, anexos = [], []
    for indice, revenda in enumerate(revendas.REVENDAS):
        tabelas = gera_revenda(indice, revenda)
        for nome, df in tabelas.items():
            if not nome.startswith("_"):
                grava(df, "erp", revenda.slug, f"{nome}.parquet")
        anexos.append(tabelas["_anexos"])
        for posicao, ven in enumerate(tabelas["_vendedores"]):
            hierarquia.append({
                "EMPRESA": revenda.nome,
                "VEN": ven,
                "NOME": f"VENDEDOR FICTICIO {ven}",
                "GA": f"GERENTE AREA {indice + 1}{'A' if posicao < 3 else 'B'}",
                "UPDATED_AT": hoje.isoformat(),
            })
        print(f"  {revenda.nome}: " + ", ".join(
            f"{nome}={df.height}" for nome, df in tabelas.items() if not nome.startswith("_")
        ))

    grava(pl.DataFrame(hierarquia), "api", "hierarquia_vendedores.parquet")
    grava(pl.concat(anexos, how="diagonal_relaxed"), "telas", "tb_anexos_cev.parquet")
    print(f"Dados ficticios gravados em {PASTA_ORIGEM.relative_to(RAIZ).as_posix()}/ (referencia: {hoje:%d/%m/%Y})")


if __name__ == "__main__":
    main()
