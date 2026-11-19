"""Cliente TCP síncrono para publicar leituras na ordem da fila."""

import logging
from collections import deque
from dataclasses import dataclass
from uuid import UUID, uuid4

from shared.protocolo import (
    ErroProtocolo,
    Operacao,
    Resultado,
    codificar,
)


from .conexao import TEMPO_ACK, ConexaoTCP
from .conexao import IdentificacaoRecusada as IdentificacaoRecusada


CAPACIDADE_FILA = 1000
LOG = logging.getLogger(__name__)


class FilaCheia(Exception):
    """A nova leitura não entrou na fila."""


@dataclass(frozen=True)
class Leitura:
    uuid: UUID
    topico: str
    temperatura: float

    def codificar_publicacao(self, id_solicitacao):
        return codificar(
            Operacao.PUBLICAR, id_solicitacao,
            uuid=self.uuid, topico=self.topico, temperatura=self.temperatura,
        )


class Publicador(ConexaoTCP):
    """Usado sequencialmente, com uma publicação em trânsito por vez."""

    def __init__(self, host="127.0.0.1", porta=5000, *, cliente_id=None):
        super().__init__(host, porta, cliente_id=cliente_id)
        self._fila = deque()

    @property
    def pendentes(self):
        return len(self._fila)

    def enfileirar(self, topico, temperatura):
        """Valida e insere a leitura, retornando seu UUID."""
        if self.pendentes >= CAPACIDADE_FILA:
            raise FilaCheia(f"A fila já contém {CAPACIDADE_FILA} leituras")
        leitura = Leitura(uuid4(), topico, temperatura)
        # Valida os campos com o protocolo antes de ocupar um lugar na fila.
        leitura.codificar_publicacao(1)
        self._fila.append(leitura)
        return leitura.uuid

    def fechar(self):
        """Fecha o transporte e mantém as leituras em RAM."""
        self._fechar_conexao()

    def publicar_proxima(self):
        """Publica a primeira leitura. Retorna o resultado do ACK ou None se vazia.

        Repete em caso de timeout do ACK. Erros de conexão são propagados e
        uma nova chamada pode reconectar e tentar a mesma leitura.
        """
        if not self._fila:
            return None
        leitura = self._fila[0]
        try:
            self.conectar()
            while True:
                identificador = self._proximo_id()
                self._enviar(leitura.codificar_publicacao(identificador))
                LOG.info("%s aguardando ACK de publicação", leitura.uuid)
                try:
                    ack = self._aguardar(identificador, Operacao.ACK_PUBLICACAO)
                except TimeoutError:
                    LOG.info("%s sem ACK em %s s, retransmitindo", leitura.uuid, TEMPO_ACK)
                    continue
                if ack.campos["uuid"] != leitura.uuid:
                    raise ErroProtocolo("UUID do ACK difere do UUID publicado")
                break
        except (OSError, ErroProtocolo):
            self.fechar()
            raise

        resultado = ack.campos["resultado"]
        if resultado in (Resultado.OK, Resultado.DESCARTADA_SEM_INSCRITOS):
            self._fila.popleft()
        else:
            LOG.warning("%s recusada (%s), mantida na fila", leitura.uuid, resultado.name)
        return resultado
