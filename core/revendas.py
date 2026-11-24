"""Nome da revenda -> CNPJ / licenca / nome oficial.

Tabela unica das revendas (no projeto original, a mesma lista chegou a estar
escrita a mao em 12 arquivos, em quatro formatos -- ver o historico no
README). Aqui sao 4 revendas FICTICIAS: nomes, CNPJs e licencas inventados.

O nome chega de fontes que escrevem diferente ("BOREAL NORTE", "Boreal Norte",
"boreal norte") -- por isso a busca ignora caixa, acento e espaco em volta. E'
de proposito que ela NAO faz correspondencia parcial: "Boreal" casaria com duas
revendas, e devolver a primeira seria pior que nao achar nada.
"""

from __future__ import annotations

import unicodedata
from typing import NamedTuple


class Revenda(NamedTuple):
    nome: str     # nome oficial, do jeito que os mapas do pipeline escrevem
    cnpj: str     # CNPJ da filial (identificador de verdade) -- FICTICIO
    licenca: str  # licenca no ERP -- FICTICIA
    slug: str     # pasta da "fonte ERP" em dados/origem/erp/ (no original: o DSN ODBC)
    uf: str


# As revendas da demonstracao. No original sao 11, cada uma com o proprio ERP
# (um DSN ODBC por revenda, so' configurado no servidor de producao).
REVENDAS: tuple[Revenda, ...] = (
    Revenda("Aurora", "11111111000101", "9001", "aurora", "CE"),
    Revenda("Boreal Norte", "22222222000102", "9002", "boreal_norte", "PE"),
    Revenda("Boreal Sul", "22222222000203", "9003", "boreal_sul", "PE"),
    Revenda("Cerrado", "33333333000104", "9004", "cerrado", "PI"),
)


def _chave(texto: str | None) -> str:
    """Minusculo, sem acento e sem espaco em volta -- o mesmo tratamento nos dois
    lados da comparacao."""
    sem_acento = unicodedata.normalize("NFKD", str(texto or ""))
    return "".join(c for c in sem_acento if not unicodedata.combining(c)).strip().lower()


chave_nome = _chave

_POR_NOME: dict[str, Revenda] = {_chave(r.nome): r for r in REVENDAS}
_POR_CNPJ: dict[str, Revenda] = {r.cnpj: r for r in REVENDAS}


def por_nome(revenda: str | None) -> Revenda | None:
    """A revenda com este nome, ou None. Nunca levanta excecao: quem chama
    decide o que fazer com o None (falhar fechado)."""
    return _POR_NOME.get(_chave(revenda))


def por_cnpj(cnpj: str | None) -> Revenda | None:
    return _POR_CNPJ.get("".join(c for c in str(cnpj or "") if c.isdigit()))


def cnpj_da_revenda(revenda: str | None) -> str | None:
    achada = por_nome(revenda)
    return achada.cnpj if achada else None


def nome_oficial(revenda: str | None) -> str | None:
    achada = por_nome(revenda)
    return achada.nome if achada else None


def para_erp(campos: tuple[str, ...] | None = None) -> list[dict]:
    """As revendas no formato de lista de dicionarios que os scripts da camada
    bruta usam (a antiga constante `EMPRESAS`)."""
    lista = []
    for revenda in REVENDAS:
        completo = revenda._asdict()
        lista.append(completo if campos is None else {c: completo[c] for c in campos})
    return lista


def cnpj_por_nome_oficial() -> dict[str, str]:
    """{"Aurora": "11111111000101", ...} -- formato usado pelos mapas da camada
    intermediaria de Comodatos."""
    return {r.nome: r.cnpj for r in REVENDAS}


def nome_empresa(revenda: Revenda) -> str:
    """Valor da coluna EMPRESA de TB_EMPRESAS ("Aurora 01") -- e' o prefixo que
    a tela de Comodatos usa pra filtrar a revenda e pra montar "Aurora 01 - 14362"."""
    return f"{revenda.nome} 01"
