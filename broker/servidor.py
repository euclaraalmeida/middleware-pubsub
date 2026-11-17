"""Servidor TCP do broker."""

import logging
import socket
import socketserver
import sqlite3
import threading

from shared.protocolo import (
    CABECALHO,
    MAX_CONTEUDO,
    TAMANHO_CONTEUDO,
    ErroProtocolo,
    Operacao,
    Resultado,
    codificar,
    decodificar,
)

from .armazenamento import Armazenamento


LOG = logging.getLogger(__name__)


def _receber_exatamente(conexao, quantidade, encerrando, permitir_fim=False):
    dados = bytearray()
    while len(dados) < quantidade:
        if encerrando.is_set():
            raise ConnectionAbortedError("Broker encerrando")
        try:
            parte = conexao.recv(quantidade - len(dados))
        except socket.timeout:
            continue
        if not parte:
            if permitir_fim and not dados:
                return None
            raise ErroProtocolo("Conexao encerrada com quadro incompleto")
        dados.extend(parte)
    return bytes(dados)


def _receber_mensagem(conexao, encerrando):
    prefixo = _receber_exatamente(
        conexao, TAMANHO_CONTEUDO.size, encerrando, permitir_fim=True,
    )
    if prefixo is None:
        return None
    (tamanho,) = TAMANHO_CONTEUDO.unpack(prefixo)
    if not CABECALHO.size <= tamanho <= MAX_CONTEUDO:
        raise ErroProtocolo("Tamanho de conteudo fora dos limites")
    return decodificar(prefixo + _receber_exatamente(conexao, tamanho, encerrando))


class _ServidorTCP(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True

    def __init__(self, endereco, armazenamento):
        self.armazenamento = armazenamento
        self.clientes = {}
        self.conexoes = set()
        self.trava = threading.Lock()
        self.encerrando = threading.Event()
        super().__init__(endereco, _Conexao)


class _Conexao(socketserver.BaseRequestHandler):
    def handle(self):
        self.cliente_id = None
        # Acorda periodicamente para encerrar, sem desconectar clientes ociosos.
        self.request.settimeout(0.2)
        with self.server.trava:
            if self.server.encerrando.is_set():
                return
            self.server.conexoes.add(self.request)
        try:
            while True:
                mensagem = _receber_mensagem(self.request, self.server.encerrando)
                if mensagem is None:
                    break
                self._tratar(mensagem)
        except ErroProtocolo as erro:
            LOG.warning("Quadro recusado de %s: %s", self.client_address, erro)
        except OSError:
            LOG.debug("Conexao encerrada: %s", self.client_address)
        except sqlite3.Error:
            LOG.exception("Falha no armazenamento; encerrando conexao")
        finally:
            with self.server.trava:
                if self.server.clientes.get(self.cliente_id) is self.request:
                    del self.server.clientes[self.cliente_id]
                self.server.conexoes.discard(self.request)

    def _responder(self, mensagem, resultado, texto=""):
        self.request.sendall(
            codificar(
                Operacao.RESPOSTA, mensagem.id_solicitacao,
                resultado=resultado, mensagem=texto,
            )
        )

    def _recusar(self, mensagem, texto):
        if mensagem.operacao == Operacao.PUBLICAR:
            self.request.sendall(
                codificar(
                    Operacao.ACK_PUBLICACAO, mensagem.id_solicitacao,
                    uuid=mensagem.campos["uuid"],
                    resultado=Resultado.REQUISICAO_INVALIDA,
                )
            )
        else:
            self._responder(mensagem, Resultado.REQUISICAO_INVALIDA, texto)

    def _tratar(self, mensagem):
        operacao = mensagem.operacao
        if operacao in (
            Operacao.ENTREGAR, Operacao.ACK_ENTREGA,
            Operacao.ACK_PUBLICACAO, Operacao.RESPOSTA,
        ):
            raise ErroProtocolo("Operacao nao recebida nesta etapa do servidor")

        if operacao == Operacao.IDENTIFICAR:
            self._identificar(mensagem)
        elif self.cliente_id is None:
            self._recusar(mensagem, "Identifique o cliente antes das demais operacoes")
        elif operacao == Operacao.CRIAR_TOPICO:
            criado = self.server.armazenamento.criar_topico(mensagem.campos["topico"])
            resultado = Resultado.OK if criado else Resultado.TOPICO_JA_EXISTENTE
            self._responder(mensagem, resultado)
        else:
            self._recusar(mensagem, "Operacao ainda nao implementada nesta etapa")

    def _identificar(self, mensagem):
        cliente_id = mensagem.campos["cliente_id"]
        if self.cliente_id is not None:
            resultado = (
                Resultado.OK if self.cliente_id == cliente_id
                else Resultado.REQUISICAO_INVALIDA
            )
        else:
            with self.server.trava:
                if cliente_id in self.server.clientes:
                    resultado = Resultado.CLIENTE_ID_EM_USO
                else:
                    self.server.clientes[cliente_id] = self.request
                    self.cliente_id = cliente_id
                    resultado = Resultado.OK
            if resultado == Resultado.OK:
                self.server.armazenamento.registrar_cliente(cliente_id)
        self._responder(mensagem, resultado)


class BrokerTCP:
    def __init__(self, host="127.0.0.1", porta=5000, banco="dados/broker.db"):
        self.armazenamento = Armazenamento(banco)
        self._servidor = _ServidorTCP((host, porta), self.armazenamento)
        self._thread = None
        self._fechado = False

    @property
    def endereco(self):
        return self._servidor.server_address

    def iniciar(self):
        if self._fechado or self._thread is not None:
            raise RuntimeError("Este broker ja foi iniciado ou encerrado")
        self._thread = threading.Thread(
            target=self._servidor.serve_forever,
            kwargs={"poll_interval": 0.1},
            name="broker-accept", daemon=True,
        )
        self._thread.start()
        return self

    def encerrar(self):
        if self._fechado:
            return
        self._servidor.encerrando.set()
        if self._thread is not None:
            self._servidor.shutdown()
        with self._servidor.trava:
            conexoes = list(self._servidor.conexoes)
        for conexao in conexoes:
            try:
                conexao.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        self._servidor.server_close()
        if self._thread is not None:
            self._thread.join()
        self._fechado = True

    def __enter__(self):
        return self.iniciar()

    def __exit__(self, *args):
        self.encerrar()
