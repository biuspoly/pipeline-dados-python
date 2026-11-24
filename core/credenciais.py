"""Ponto UNICO de leitura de credencial/configuracao sensivel.

No projeto original cada script montava a propria URL de conexao com
os.getenv('DB_USER')/os.getenv('DB_PASSWORD') lidos do .env da pasta dele.
Nesta versao de demonstracao nenhum banco nem servidor de e-mail e' usado,
mas o ponto continua existindo: quem precisar de um segredo chama
obter_credencial("NOME") NO PONTO DE USO -- nada de constante com valor no
codigo, nada de segredo lido no import.

Para ligar num cofre (Azure Key Vault, AWS Secrets Manager, HashiCorp Vault,
keyring do Windows...), troque SO' o corpo desta funcao; quem chama nao muda.
"""

import os


def obter_credencial(nome: str) -> str:
    """Valor da credencial `nome`, ou "" se nao estiver definida.

    Padrao: variavel de ambiente (o .env, quando existir, e' carregado por quem
    roda o processo -- ver orquestracao/flows.py). Os NOMES esperados estao em
    .env.example, sem valor nenhum.
    """
    return os.environ.get(nome, "")
