"""Aviso por e-mail quando o prazo de um comodato esta' apertando.

Pedido do usuario, junto da cor da coluna "Prazo": "queria ver um jeito onde
quando troque, envie o email pro responsavel informando, alguma automacao so'
pra isso, pra avisar que o prazo ta esgotando".

TRES decisoes que moram aqui:

1. **A faixa e' a MESMA da cor da celula** -- `dados_comodato.faixa_prazo`, a
   funcao que a tabela usa pra pintar. Nao ha uma segunda regra: se a tela mostra
   amarelo, o aviso fala de amarelo, sempre.
2. **Um e-mail por revenda**, com a lista dos CEVs que mudaram de faixa -- e nao
   um e-mail por CEV, que num dia movimentado viraria dezenas de mensagens.
3. **Cada CEV e' avisado UMA vez por faixa** (tabela `tb_comod_aviso_prazo`). Ele
   piorou de amarelo pra vermelho? Sai aviso novo. Continua no mesmo amarelo de
   ontem? Nao repete.

DEMONSTRACAO: nada e' enviado. No original, `--executar` manda por SMTP
(smtplib, com STARTTLS e autenticacao NTLM pro Exchange, e um plano B de
remetente). Aqui, `--executar` monta exatamente a mesma mensagem e a GRAVA em
`saida/emails/<revenda>.eml` (abre em qualquer cliente de e-mail), imprimindo o
corpo em texto. Os destinatarios sao ficticios (@example.com).
"""

import json
from datetime import datetime
from email.message import EmailMessage
from email.utils import formataddr
from html import escape

import polars as pl

from core import banco as bancos
from core import revendas
from core.credenciais import obter_credencial
from . import dados_comodato as dc

RAIZ = bancos.RAIZ
ARQUIVO_DESTINATARIOS = RAIZ / "config" / "avisos_prazo_comodato.json"
"""Quem recebe, por revenda (muda sem reiniciar nada)."""
PASTA_EMAILS = RAIZ / "saida" / "emails"

DESLIGADO = "-"
"""Valor que desliga o aviso de uma revenda de propósito (ver o JSON)."""

FAIXAS_QUE_AVISAM = (dc.FAIXA_PRAZO_ATENCAO, dc.FAIXA_PRAZO_CRITICO)
"""Só as duas faixas de aperto. Verde não gera aviso -- e-mail que chega quando
não há nada a fazer é e-mail que ninguém lê depois."""

AVISA_SO_APOS_IMPLANTACAO = True
"""Só comodato emitido A PARTIR da data em que a revenda entrou no fluxo novo --
o corte é a DATA, não a origem do registro. Medido no original: sem esse corte,
4.617 comodatos entravam em faixa de aperto (quase todos do histórico antigo,
vencidos há muito tempo); com ele, sobra o que a operação de hoje consegue
resolver."""

ROTULO_FAIXA = {
    dc.FAIXA_PRAZO_ATENCAO: "Atenção",
    dc.FAIXA_PRAZO_CRITICO: "Crítico",
}

LIMITE_LINHAS_EMAIL = 40
"""Quantos comodatos a tabela do e-mail lista (o total sempre aparece no texto).
Medido na 1ª execução do original: uma revenda tinha 792 comodatos em faixa de
aperto -- e-mail com 792 linhas ninguém lê, e alguns servidores cortam mensagem
grande. Os mais apertados vêm primeiro; o restante entra como "e mais N"."""

TABELA_AVISOS = "tb_comod_aviso_prazo"
"""Tabela PRÓPRIA (no MySQL, chave primária REVENDA+CEV+FAIXA com INSERT IGNORE),
não uma coluna nova em tabela existente -- a chave (revenda, CEV, faixa) É a
regra de "não repetir"."""

# Cores iguais às da célula "Prazo" na tela, pra quem abre o e-mail reconhecer o
# que vai ver no painel. (No app vêm de core/cores.py.)
SIDEBAR_BG = "#254D71"
COMODATO_PRAZO_ATENCAO_FUNDO, COMODATO_PRAZO_ATENCAO_TEXTO = "#FFF3CD", "#7A5B00"
COMODATO_PRAZO_CRITICO_FUNDO, COMODATO_PRAZO_CRITICO_TEXTO = "#F8D7DA", "#842029"


# ==========================================================================
# Quem recebe
# ==========================================================================


def destinatarios(revenda: str) -> list[str]:
    """E-mails que recebem o aviso daquela revenda.

    Revenda sem lista própria (ou com a lista vazia) cai na lista "padrao". `["-"]`
    desliga.
    """
    try:
        config = json.loads(ARQUIVO_DESTINATARIOS.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        config = {}
    por_revenda = config.get("por_revenda") or {}
    oficial = revendas.nome_oficial(revenda) or revenda
    # Procura pelo nome oficial e, se não achar, por qualquer chave equivalente
    # (a grafia do JSON pode vir sem acento, ou em outra caixa).
    lista = por_revenda.get(oficial)
    if lista is None:
        alvo = revendas.chave_nome(oficial)
        lista = next((v for k, v in por_revenda.items() if revendas.chave_nome(k) == alvo), None)
    if lista is None or (isinstance(lista, list) and not lista):
        lista = config.get("padrao") or []
    if isinstance(lista, str):
        lista = [lista]
    limpos = [str(e).strip() for e in lista if str(e).strip()]
    if DESLIGADO in limpos:
        return []
    return [e for e in limpos if "@" in e]


# ==========================================================================
# Quais CEVs avisar
# ==========================================================================

_COLUNAS_AVISO = ("CEV", "Cliente", "Razão Social", "TIPO", "Prazo do Input",
                  "Data Emissão", "Data Vencimento", "Produto")

_ESQUEMA_AVISO = {
    "CEV": pl.Utf8, "NUMERO": pl.Utf8, "CLIENTE": pl.Utf8, "CODIGO_CLIENTE": pl.Utf8,
    "TIPO": pl.Utf8, "FAIXA": pl.Utf8, "DIAS": pl.Int64, "PRAZO": pl.Utf8,
    "EMISSAO": pl.Utf8, "VENCIMENTO": pl.Utf8, "ITENS": pl.Int64,
}
"""Esquema declarado na PRÓPRIA construção do quadro, nunca depois.

**Bug real, achado na 1ª execução do original**: `pl.DataFrame(linhas)` adivinha
o tipo pelas 100 primeiras linhas, e "DIAS" é nulo justamente nas de cima (as
"Esgotado", que não têm número) -- a coluna nascia Null e a 101ª linha, com um
inteiro, derrubava a revenda inteira ("could not append value: 0 of type: i64")."""


def cevs_apertando(revenda: str) -> pl.DataFrame:
    """Um registro por CEV daquela revenda que está em faixa de aperto.

    Lê o mesmo cache que a tela lê (`comod_geral`), então o número do aviso é o
    número que a pessoa vai ver quando abrir o painel.
    """
    base = dc.comod_geral(revenda)
    if base.height == 0 or "Prazo do Input" not in base.columns:
        return pl.DataFrame()

    esgotado = dc.mapa_prazo_esgotado(base)
    # Sem a data de emissão na base, o mapa vem vazio e aí ninguém é avisado --
    # de propósito: melhor não cobrar do que cobrar comodato antigo.
    apos = dc.mapa_apos_implantacao(base) if AVISA_SO_APOS_IMPLANTACAO else {}
    presentes = [c for c in _COLUNAS_AVISO if c in base.columns]
    # Um registro por CEV: o prazo é do CEV inteiro, e o que muda entre as linhas
    # é o produto (que entra como contagem).
    por_cev = base.select(presentes).group_by("CEV", maintain_order=True).agg(
        [
            pl.col(coluna).drop_nulls().first().alias(coluna)
            for coluna in presentes
            if coluna not in ("CEV", "Produto")
        ]
        + ([pl.len().alias("ITENS")] if "Produto" in presentes else [])
    )

    linhas = []
    for registro in por_cev.iter_rows(named=True):
        cev = str(registro["CEV"])
        if AVISA_SO_APOS_IMPLANTACAO and not apos.get(cev, False):
            continue
        faixa = dc.faixa_prazo(registro.get("Prazo do Input"), esgotado.get(cev, False))
        if faixa not in FAIXAS_QUE_AVISAM:
            continue
        linhas.append({
            "CEV": cev,
            "NUMERO": cev.rsplit(" - ", 1)[-1].strip() or cev,
            "CLIENTE": str(registro.get("Razão Social") or registro.get("Cliente") or "").strip(),
            "CODIGO_CLIENTE": str(registro.get("Cliente") or "").strip(),
            "TIPO": str(registro.get("TIPO") or "").strip(),
            "FAIXA": faixa,
            "DIAS": dc.dias_do_prazo(registro.get("Prazo do Input")),
            "PRAZO": str(registro.get("Prazo do Input") or "").strip(),
            "EMISSAO": _data_curta(registro.get("Data Emissão")),
            "VENCIMENTO": _data_curta(registro.get("Data Vencimento")),
            "ITENS": registro.get("ITENS"),
        })
    if not linhas:
        return pl.DataFrame()
    tabela = pl.DataFrame(linhas, schema=_ESQUEMA_AVISO)
    # Mais apertado primeiro: crítico antes de atenção, e dentro da faixa o menor
    # número de dias na frente. "Esgotado" não tem número -- vai no topo do crítico.
    return tabela.sort(
        [pl.col("FAIXA") != dc.FAIXA_PRAZO_CRITICO, pl.col("DIAS")],
        nulls_last=False,
    )


# ==========================================================================
# O que já foi avisado (para não repetir)
# ==========================================================================


def ja_avisados(revenda: str) -> set[tuple[str, str]]:
    """(CEV, faixa) que já foram avisados nesta revenda.

    Falha de leitura LEVANTA de propósito -- aqui a falha segura é NÃO enviar: sem
    saber o que já foi avisado, a execução seguinte mandaria o mesmo aviso de
    novo, todo dia.
    """
    avisos = bancos.ler_tabela("bd_comodato", TABELA_AVISOS)
    if avisos.height == 0:
        return set()
    avisos = avisos.filter(pl.col("REVENDA") == revenda)
    return {(str(cev), str(faixa)) for cev, faixa in zip(avisos["CEV"], avisos["FAIXA"])}


def registrar_avisos(revenda: str, linhas: list[dict]) -> int:
    """Marca os CEVs como avisados naquela faixa. Como o INSERT IGNORE do
    original: se a chave (revenda, CEV, faixa) já existe, não duplica."""
    if not linhas:
        return 0
    novas = pl.DataFrame(
        [{"REVENDA": revenda, "CEV": linha["CEV"], "FAIXA": linha["FAIXA"], "DIAS": linha["DIAS"],
          "AVISADO_EM": datetime.now()} for linha in linhas],
        schema={"REVENDA": pl.Utf8, "CEV": pl.Utf8, "FAIXA": pl.Utf8, "DIAS": pl.Int64, "AVISADO_EM": pl.Datetime},
    )
    existentes = bancos.ler_tabela("bd_comodato", TABELA_AVISOS)
    if existentes.height:
        novas = novas.join(existentes.select("REVENDA", "CEV", "FAIXA"), on=["REVENDA", "CEV", "FAIXA"], how="anti")
        tudo = pl.concat([existentes, novas], how="diagonal_relaxed")
    else:
        tudo = novas
    bancos.grava_tabela("bd_comodato", TABELA_AVISOS, tudo)
    return novas.height


def esquecer_avisos(revenda: str | None = None) -> int:
    """Apaga o histórico de avisos (de uma revenda, ou de todas) -- serve pro
    teste, e pro caso de alguém querer que a lista inteira seja avisada de novo."""
    avisos = bancos.ler_tabela("bd_comodato", TABELA_AVISOS)
    if avisos.height == 0:
        return 0
    restantes = avisos.filter(pl.col("REVENDA") != revenda) if revenda else avisos.clear()
    bancos.grava_tabela("bd_comodato", TABELA_AVISOS, restantes)
    return avisos.height - restantes.height


# ==========================================================================
# O e-mail
# ==========================================================================


def _data_curta(valor) -> str:
    if valor is None:
        return ""
    if hasattr(valor, "strftime"):
        return valor.strftime("%d/%m/%Y")
    texto = str(valor).removesuffix(".0").strip()
    if len(texto) == 8 and texto.isdigit():
        return f"{texto[6:]}/{texto[4:6]}/{texto[:4]}"
    return texto


def _assunto(revenda: str, linhas: list[dict]) -> str:
    criticos = sum(1 for linha in linhas if linha["FAIXA"] == dc.FAIXA_PRAZO_CRITICO)
    atencao = len(linhas) - criticos
    partes = []
    if criticos:
        partes.append(f"{criticos} no vermelho")
    if atencao:
        partes.append(f"{atencao} no amarelo")
    return f"Comodato: prazo de documento apertando -- {revenda} ({', '.join(partes)})"


def _corpo_texto(revenda: str, linhas: list[dict]) -> str:
    linhas_texto = [
        f"Comodatos da {revenda} com o prazo de envio de documento apertando"
        f" -- {datetime.now().strftime('%d/%m/%Y às %H:%M')}.",
        "",
        f"{'CEV':>10}  {'Situação':9}  {'Prazo':20}  Cliente",
    ]
    for linha in linhas[:LIMITE_LINHAS_EMAIL]:
        linhas_texto.append(
            f"{linha['NUMERO']:>10}  {ROTULO_FAIXA.get(linha['FAIXA'], ''):9}"
            f"  {linha['PRAZO']:20}  {linha['CLIENTE'][:45]}"
        )
    if len(linhas) > LIMITE_LINHAS_EMAIL:
        linhas_texto.append(f"... e mais {len(linhas) - LIMITE_LINHAS_EMAIL} comodato(s)"
                            " -- a lista completa está no painel.")
    linhas_texto += [
        "",
        "O prazo só para de correr quando o contrato assinado E o canhoto estão"
        " gravados; quando ele vence, o comodato deixa de aceitar documento e passa"
        " a bloquear solicitação nova de material da revenda.",
        "",
        "Abra o painel em Comodatos para anexar o que falta ou cobrar a assinatura.",
        "",
        "-- ",
        "Aviso automático do Acompanhamento Logístico. Cada comodato é avisado uma vez"
        " por faixa (amarelo e vermelho).",
    ]
    return "\n".join(linhas_texto)


_TH = f"padding:6px 10px;color:#ffffff;background:{SIDEBAR_BG};font-weight:600;"
"""Estilo de CADA célula do cabeçalho, com a cor da fonte repetida ali: cliente de
e-mail (Outlook em primeiro lugar) não herda cor do `<tr>` para os `<th>`."""


def _corpo_html(revenda: str, linhas: list[dict]) -> str:
    cores = {
        dc.FAIXA_PRAZO_ATENCAO: (COMODATO_PRAZO_ATENCAO_FUNDO, COMODATO_PRAZO_ATENCAO_TEXTO),
        dc.FAIXA_PRAZO_CRITICO: (COMODATO_PRAZO_CRITICO_FUNDO, COMODATO_PRAZO_CRITICO_TEXTO),
    }
    corpo = []
    for linha in linhas[:LIMITE_LINHAS_EMAIL]:
        fundo, texto = cores.get(linha["FAIXA"], ("#ffffff", "#000000"))
        corpo.append(
            "<tr>"
            f'<td style="padding:6px 10px;border-bottom:1px solid #eee;font-weight:600">{escape(linha["NUMERO"])}</td>'
            f'<td style="padding:6px 10px;border-bottom:1px solid #eee;background:{fundo};color:{texto};'
            f'font-weight:700;text-align:center">{escape(linha["PRAZO"])}</td>'
            f'<td style="padding:6px 10px;border-bottom:1px solid #eee">{escape(linha["CLIENTE"] or "-")}</td>'
            f'<td style="padding:6px 10px;border-bottom:1px solid #eee;text-align:center">{escape(linha["EMISSAO"] or "")}</td>'
            "</tr>"
        )
    if len(linhas) > LIMITE_LINHAS_EMAIL:
        corpo.append(
            '<tr><td colspan="4" style="padding:8px 10px;font-style:italic;color:#555">'
            f"... e mais {len(linhas) - LIMITE_LINHAS_EMAIL} comodato(s)"
            " -- a lista completa está no painel.</td></tr>"
        )
    return f"""\
<div style="font-family:system-ui,Segoe UI,Arial,sans-serif;color:#1a1a1a">
  <h2 style="color:{SIDEBAR_BG};font-size:1.05rem;margin:0 0 .3rem">
    Comodato: prazo de documento apertando -- {escape(revenda)}</h2>
  <p style="margin:.2rem 0 1rem;font-size:.9rem;color:#555">
    {len(linhas)} comodato(s) mudaram de faixa em {datetime.now().strftime('%d/%m/%Y às %H:%M')}.</p>
  <table style="border-collapse:collapse;font-size:.88rem">
    <thead><tr style="background:{SIDEBAR_BG}">
      <th style="{_TH}text-align:left">CEV</th>
      <th style="{_TH}">Prazo</th>
      <th style="{_TH}text-align:left">Cliente</th>
      <th style="{_TH}">Emissão</th>
    </tr></thead>
    <tbody>{''.join(corpo)}</tbody>
  </table>
  <p style="margin:1rem 0 .3rem;font-size:.88rem">
    O prazo só para de correr quando o <b>contrato assinado</b> e o <b>canhoto</b> estão
    gravados. Quando vence, o comodato deixa de aceitar documento e passa a bloquear
    solicitação nova de material da revenda.</p>
  <p style="margin-top:1.2rem;font-size:.78rem;color:#777">
    Aviso automático do Acompanhamento Logístico -- cada comodato é avisado uma vez por
    faixa (amarelo e vermelho).</p>
</div>"""


def enviar_aviso(revenda: str, linhas: list[dict], executar: bool = False) -> tuple[bool, str]:
    """Monta UM e-mail com a lista daquela revenda.

    `executar=False` (padrão) monta a mensagem e não grava nada -- é o ensaio que
    o projeto usa em toda escrita/envio. `executar=True`, na demonstração, grava o
    `.eml` em saida/emails/ no lugar do envio por SMTP.
    """
    destinos = destinatarios(revenda)
    if not destinos:
        return False, "sem destinatário configurado (ver config/avisos_prazo_comodato.json)"
    if not linhas:
        return False, "nada a avisar"

    # Remetente: configuracao, nao segredo -- mas passa pelo mesmo ponto unico.
    remetente = obter_credencial("EMAIL_REMETENTE_PADRAO") or "avisos-logistica@example.com"

    email = EmailMessage()
    email["Subject"] = _assunto(revenda, linhas)
    email["From"] = formataddr(("Acompanhamento Logístico", remetente))
    email["To"] = ", ".join(destinos)
    email.set_content(_corpo_texto(revenda, linhas))
    email.add_alternative(_corpo_html(revenda, linhas), subtype="html")

    if not executar:
        return True, f"[modo teste] {len(linhas)} comodato(s) para {', '.join(destinos)}"

    # --- DEMONSTRACAO: em vez de smtplib.SMTP(...).send_message(email) --------
    PASTA_EMAILS.mkdir(parents=True, exist_ok=True)
    arquivo = PASTA_EMAILS / f"{revendas.chave_nome(revenda).replace(' ', '_')}.eml"
    arquivo.write_bytes(bytes(email))
    print(f"\n---- e-mail que sairia ({arquivo.relative_to(RAIZ).as_posix()}) ----")
    print(f"Para: {email['To']}\nAssunto: {email['Subject']}\n")
    print(_corpo_texto(revenda, linhas))
    print("---- fim do e-mail ----\n")
    return True, f"[demonstracao, nao enviado] gravado em {arquivo.relative_to(RAIZ).as_posix()}"


# ==========================================================================
# O trabalho de uma revenda, de ponta a ponta
# ==========================================================================


def avisar_revenda(revenda: str, executar: bool = False, reenviar: bool = False) -> dict:
    """Confere a revenda, "envia" (se houver o que avisar) e registra o que saiu.

    ORDEM que importa: registra DEPOIS de o envio dar certo. Ao contrário, uma
    falha de SMTP marcaria o CEV como avisado e ninguém receberia nada -- nunca
    mais, porque a chave (revenda, CEV, faixa) já estaria gravada.
    """
    resumo = {"revenda": revenda, "apertando": 0, "novos": 0, "enviado": False, "detalhe": ""}
    apertando = cevs_apertando(revenda)
    resumo["apertando"] = apertando.height
    if apertando.height == 0:
        resumo["detalhe"] = "nenhum comodato em faixa de aperto"
        return resumo

    linhas = apertando.to_dicts()
    if not reenviar:
        vistos = ja_avisados(revenda)
        linhas = [linha for linha in linhas if (linha["CEV"], linha["FAIXA"]) not in vistos]
    resumo["novos"] = len(linhas)
    if not linhas:
        resumo["detalhe"] = "todos já avisados nessa faixa"
        return resumo

    ok, detalhe = enviar_aviso(revenda, linhas, executar=executar)
    resumo["enviado"] = ok
    resumo["detalhe"] = detalhe
    if ok and executar:
        resumo["registrados"] = registrar_avisos(revenda, linhas)
    return resumo
