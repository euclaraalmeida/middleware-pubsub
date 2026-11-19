"""Cliente TCP síncrono para publicar leituras na ordem da fila."""

import logging
import socket
from collections import deque
from dataclasses import dataclass
from time import monotonic
from uuid import UUID, uuid4

from shared.protocolo import (
    MAX_ID_SOLICITACAO,
    AcumuladorQuadros,
    ErroProtocolo,
    Operacao,
    Resultado,
    codificar,
)


CAPACIDADE_FILA = 1000
TEMPO_ACK = 5
LOG = logging.getLogger(__name__)


class FilaCheia(Exception):
    """A nova leitura não entrou na fila."""


class IdentificacaoRecusada(Exception):
    def __init__(self, resultado):
        self.resultado = resultado
        super().__init__(f"Identificação recusada: {resultado.name}")


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


class Publicador:
    """Usado sequencialmente, com uma publicação em trânsito por vez."""

    def __init__(self, host="127.0.0.1", porta=5000, *, cliente_id=None):
        self.host = host
        self.porta = porta
        self.cliente_id = cliente_id
        self._fila = deque()
        self._conexao = None
        self._acumulador = None
        self._ultimo_id = 0

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

    def conectar(self):
        """Abre a conexão e, se configurado, solicita identificação."""
        if self._conexao is not None:
            return
        self._conexao = socket.create_connection((self.host, self.porta), timeout=TEMPO_ACK)
        self._acumulador = AcumuladorQuadros()
        try:
            if self.cliente_id is not None:
                identificador = self._proximo_id()
                quadro = codificar(Operacao.IDENTIFICAR, identificador, cliente_id=self.cliente_id)
                self._enviar(quadro)
                resposta = self._aguardar(identificador, Operacao.RESPOSTA)
                resultado = resposta.campos["resultado"]
                if resultado != Resultado.OK:
                    raise IdentificacaoRecusada(resultado)
        except (OSError, ErroProtocolo, IdentificacaoRecusada):
            self.fechar()
            raise

    def fechar(self):
        """Fecha o transporte e mantém as leituras em RAM."""
        conexao = self._conexao
        self._conexao = None
        self._acumulador = None
        if conexao is not None:
            conexao.close()

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

    def _proximo_id(self):
        self._ultimo_id = self._ultimo_id % MAX_ID_SOLICITACAO + 1
        return self._ultimo_id

    def _enviar(self, quadro):
        self._conexao.settimeout(TEMPO_ACK)
        self._conexao.sendall(quadro)

    def _aguardar(self, identificador, operacao):
        prazo = monotonic() + TEMPO_ACK
        while True:
            restante = prazo - monotonic()
            if restante <= 0:
                raise TimeoutError("Prazo de resposta expirado")
            self._conexao.settimeout(restante)
            dados = self._conexao.recv(4096)
            if not dados:
                self._acumulador.finalizar()
                raise ConnectionError("O broker encerrou a conexão")
            resposta = None
            for mensagem in self._acumulador.alimentar(dados):
                # Uma resposta atrasada pode pertencer à tentativa anterior.
                if mensagem.id_solicitacao != identificador:
                    continue
                if mensagem.operacao != operacao:
                    raise ErroProtocolo(f"Resposta inesperada: {mensagem.operacao.name}")
                resposta = mensagem
            if resposta is not None:
                return resposta
