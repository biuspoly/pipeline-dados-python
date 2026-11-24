"""Avisa por e-mail quando o prazo de documento de um comodato esta' apertando.

Um e-mail por revenda, com a lista dos CEVs que ENTRARAM na faixa amarela (6 a 3
dias) ou vermelha (2 a 0, negativo, "Esgotado") -- as mesmas faixas que pintam a
celula "Prazo" na tela (a regra e' unica, ver comodatos/dados_comodato.py::faixa_prazo).

Cada comodato e' avisado UMA vez por faixa: o controle fica em
`bd_comodato.tb_comod_aviso_prazo`, criada sozinha na 1a execucao. Quem recebe
esta' em `config/avisos_prazo_comodato.json` (enderecos ficticios @example.com).

Uso:
    python scripts/avisa_prazo_comodato.py                 # ensaia, nao grava nada
    python scripts/avisa_prazo_comodato.py --executar      # "envia" (na demonstracao: grava o .eml)
    python scripts/avisa_prazo_comodato.py --revenda Aurora --executar
    python scripts/avisa_prazo_comodato.py --reenviar --executar   # ignora o historico
    python scripts/avisa_prazo_comodato.py --esquecer      # limpa o historico de avisos

Roda sozinho ao MEIO-DIA, com agenda propria (orquestracao/flows.py::
aviso_prazo_comodato) -- fora da madrugada de proposito: o aviso e' mensagem pra
gente, nao geracao de dado, e chegar no meio do dia coloca o e-mail na frente de
quem esta' trabalhando.
"""

import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

PASTA_APP = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PASTA_APP.parent))
sys.path.insert(0, str(PASTA_APP))

from core import revendas  # noqa: E402
from comodatos import aviso_prazo as ap  # noqa: E402


def _revendas_alvo(pedida: str | None) -> list[str]:
    if pedida:
        # Aceita qualquer grafia ("aurora", "AURORA", "Aurora") -- resolve pro nome
        # oficial, que e' o que a leitura e o JSON de destinatarios usam.
        return [revendas.nome_oficial(pedida) or pedida]
    return [r.nome for r in revendas.REVENDAS]


def main(argumentos: list[str]) -> int:
    executar = "--executar" in argumentos
    reenviar = "--reenviar" in argumentos
    esquecer = "--esquecer" in argumentos
    pedida = None
    if "--revenda" in argumentos:
        posicao = argumentos.index("--revenda")
        if posicao + 1 < len(argumentos):
            pedida = argumentos[posicao + 1]

    if esquecer:
        apagados = ap.esquecer_avisos(revenda=(revendas.nome_oficial(pedida) or pedida) if pedida else None)
        print(f"Historico de avisos apagado: {apagados} registro(s)")
        return 0

    alvo = _revendas_alvo(pedida)
    print(f"Aviso de prazo de comodato -- {len(alvo)} revenda(s)"
          f"{' [MODO TESTE, nada e gravado]' if not executar else ' [DEMONSTRACAO: grava o .eml, nao envia]'}"
          f"{' [reenviando, ignora o historico]' if reenviar else ''}")

    total_apertando = total_novos = enviados = falhas = 0
    for revenda in alvo:
        try:
            resumo = ap.avisar_revenda(revenda, executar=executar, reenviar=reenviar)
        except Exception as exc:
            # Uma revenda com problema (cache faltando, por exemplo) nao pode
            # impedir o aviso das outras -- mesma regra do resto do pipeline.
            falhas += 1
            print(f"  {revenda:16} ERRO: {type(exc).__name__}: {exc}")
            continue
        total_apertando += resumo["apertando"]
        total_novos += resumo["novos"]
        if resumo["enviado"]:
            enviados += 1
        marca = "enviado" if resumo["enviado"] else "-"
        print(f"  {revenda:16} apertando={resumo['apertando']:4} novos={resumo['novos']:4}"
              f"  {marca:8} {resumo['detalhe']}")

    print(f"\nResumo: {total_apertando} comodato(s) em faixa de aperto,"
          f" {total_novos} sem aviso ainda, {enviados} e-mail(s)"
          f"{' gerados' if executar else ' que sairiam'}"
          f"{f', {falhas} revenda(s) com erro' if falhas else ''}")
    # Falha de revenda nao derruba o passo do agendamento: o aviso e' informativo,
    # e derrubar aqui esconderia o que deu certo. O erro fica impresso e no log.
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
