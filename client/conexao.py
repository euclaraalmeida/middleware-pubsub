"""Transporte TCP compartilhado pelos clientes."""

import socket
from time import monotonic

from shared.protocolo import (
    MAX_ID_SOLICITACAO, AcumuladorQuadros, ErroProtocolo, Operacao, Resultado, codificar,
)


TEMPO_ACK = 5


class IdentificacaoRecusada(Exception):
    def __init__(self, resultado):
        self.resultado = resultado
        super().__init__(f"Identificação recusada: {resultado.name}")


class ConexaoTCP:
    """Mantém o socket, o acumulador e a correlação das respostas."""

    def __init__(self, host, porta, *, cliente_id):
        self.host = host
        self.porta = porta
        self.cliente_id = cliente_id
        self._conexao = None
        self._acumulador = None
        self._ultimo_id = 0

    def conectar(self):
        """Abre a conexão e solicita identificação quando configurada."""
        if self._conexao is not None:
            return
        self._conexao = socket.create_connection((self.host, self.porta), timeout=TEMPO_ACK)
        self._acumulador = AcumuladorQuadros()
        try:
            if self.cliente_id is not None:
                identificador = self._proximo_id()
                self._enviar(codificar(
                    Operacao.IDENTIFICAR, identificador, cliente_id=self.cliente_id,
                ))
                resposta = self._aguardar(identificador, Operacao.RESPOSTA)
                resultado = resposta.campos["resultado"]
                if resultado != Resultado.OK:
                    raise IdentificacaoRecusada(resultado)
        except (OSError, ErroProtocolo, IdentificacaoRecusada):
            self._fechar_conexao()
            raise

    def _fechar_conexao(self):
        conexao = self._conexao
        self._conexao = None
        self._acumulador = None
        if conexao is not None:
            conexao.close()

    def _proximo_id(self):
        self._ultimo_id = self._ultimo_id % MAX_ID_SOLICITACAO + 1
        return self._ultimo_id

    def _enviar(self, quadro):
        self._conexao.settimeout(TEMPO_ACK)
        self._conexao.sendall(quadro)

    def _ler(self, timeout):
        self._conexao.settimeout(timeout)
        dados = self._conexao.recv(4096)
        if not dados:
            self._acumulador.finalizar()
            raise ConnectionError("O broker encerrou a conexão")
        return self._acumulador.alimentar(dados)

    def _tratar_espontanea(self, mensagem):
        """Permite ao inscrito tratar entregas durante uma solicitação."""

    def _aguardar(self, identificador, operacao):
        prazo = monotonic() + TEMPO_ACK
        while True:
            restante = prazo - monotonic()
            if restante <= 0:
                raise TimeoutError("Prazo de resposta expirado")
            resposta = None
            for mensagem in self._ler(restante):
                if mensagem.id_solicitacao != identificador:
                    self._tratar_espontanea(mensagem)
                    continue
                if mensagem.operacao != operacao:
                    raise ErroProtocolo(f"Resposta inesperada: {mensagem.operacao.name}")
                resposta = mensagem
            if resposta is not None:
                return resposta
