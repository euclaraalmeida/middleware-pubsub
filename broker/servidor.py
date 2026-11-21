"""Servidor TCP do broker."""

import logging
import socket
import socketserver
import sqlite3
import threading
from time import monotonic

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

from .armazenamento import CAPACIDADE_MENSAGENS, Armazenamento


LOG = logging.getLogger(__name__)
TEMPO_REENVIO = 5


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

    def __init__(self, endereco, armazenamento, tempo_reenvio):
        self.armazenamento = armazenamento
        self.tempo_reenvio = tempo_reenvio
        self.clientes = {}
        self.conexoes = set()
        self.trava = threading.Lock()
        self.encerrando = threading.Event()
        super().__init__(endereco, _Conexao)

    def avisar_entregas(self):
        with self.trava:
            clientes = list(self.clientes.values())
        for cliente in clientes:
            cliente._acordar_envio.set()


class _Conexao(socketserver.BaseRequestHandler):
    def handle(self):
        self.cliente_id = None
        self._trava_envio = threading.Lock()
        self._trava_publicacao = threading.Lock()
        self._publicacao_pendente = None
        self._parar_envio = threading.Event()
        self._acordar_envio = threading.Event()
        self._emissor = None
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
            self._parar_envio.set()
            self._acordar_envio.set()
            if self._emissor is not None:
                self._emissor.join()
            with self.server.trava:
                if self.server.clientes.get(self.cliente_id) is self:
                    del self.server.clientes[self.cliente_id]
                self.server.conexoes.discard(self.request)

    def _enviar(self, quadro):
        # Respostas e entregas compartilham o socket, mas nunca os bytes de um quadro.
        with self._trava_envio:
            self.request.sendall(quadro)

    def _responder(self, mensagem, resultado, texto=""):
        self._enviar(
            codificar(
                Operacao.RESPOSTA, mensagem.id_solicitacao,
                resultado=resultado, mensagem=texto,
            )
        )

    def _recusar(self, mensagem, texto):
        if mensagem.operacao == Operacao.PUBLICAR:
            self._enviar(
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
        if operacao == Operacao.ACK_ENTREGA:
            if self.cliente_id is None or mensagem.campos["cliente_id"] != self.cliente_id:
                raise ErroProtocolo("ACK de entrega nao pertence a esta conexao")
            confirmada = self.server.armazenamento.confirmar_entrega(
                mensagem.campos["uuid"], self.cliente_id,
            )
            if confirmada:
                self.server.avisar_entregas()
            return
        if operacao in (
            Operacao.ENTREGAR, Operacao.ACK_PUBLICACAO, Operacao.RESPOSTA,
        ):
            raise ErroProtocolo("Operacao no sentido incorreto da conexao")

        if operacao == Operacao.IDENTIFICAR:
            self._identificar(mensagem)
        elif self.cliente_id is None:
            self._recusar(mensagem, "Identifique o cliente antes das demais operacoes")
        elif operacao == Operacao.CRIAR_TOPICO:
            criado = self.server.armazenamento.criar_topico(mensagem.campos["topico"])
            resultado = Resultado.OK if criado else Resultado.TOPICO_JA_EXISTENTE
            self._responder(mensagem, resultado)
        elif operacao == Operacao.INSCREVER:
            try:
                self.server.armazenamento.inscrever(self.cliente_id, mensagem.campos["topico"])
                resultado = Resultado.OK
            except sqlite3.IntegrityError:
                resultado = Resultado.TOPICO_INEXISTENTE
            self._responder(mensagem, resultado)
            self._acordar_envio.set()
        elif operacao == Operacao.CANCELAR_INSCRICAO:
            resultado = self.server.armazenamento.cancelar_inscricao(
                self.cliente_id, mensagem.campos["topico"],
            )
            self._responder(mensagem, resultado)
        elif operacao == Operacao.EXCLUIR_TOPICO:
            resultado = self.server.armazenamento.excluir_topico(mensagem.campos["topico"])
            self._responder(mensagem, resultado)
        elif operacao == Operacao.PUBLICAR:
            with self._trava_publicacao:
                anterior = self._publicacao_pendente
                if anterior is not None and anterior.campos["uuid"] != mensagem.campos["uuid"]:
                    raise ErroProtocolo("Aguarde o ACK antes de publicar outra leitura")
                campos = mensagem.campos
                resultado = self.server.armazenamento.publicar(
                    campos["uuid"], campos["topico"], campos["temperatura"],
                )
                if resultado is None:
                    self._publicacao_pendente = mensagem
                else:
                    self._publicacao_pendente = None
            if resultado is None:
                self._acordar_envio.set()
            else:
                self._confirmar_publicacao(mensagem, resultado)
        else:
            self._recusar(mensagem, "Operacao inesperada do cliente")

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
                    self.server.clientes[cliente_id] = self
                    self.cliente_id = cliente_id
                    resultado = Resultado.OK
            if resultado == Resultado.OK:
                self.server.armazenamento.registrar_cliente(cliente_id)
        self._responder(mensagem, resultado)
        if resultado == Resultado.OK and self._emissor is None:
            self._emissor = threading.Thread(
                target=self._enviar_pendentes, name=f"broker-entregas-{cliente_id}",
            )
            self._emissor.start()

    def _publicar_pendente(self):
        with self._trava_publicacao:
            mensagem = self._publicacao_pendente
            if mensagem is None:
                return
            campos = mensagem.campos
            resultado = self.server.armazenamento.publicar(
                campos["uuid"], campos["topico"], campos["temperatura"],
            )
            if resultado is None:
                return
            self._publicacao_pendente = None
        self._confirmar_publicacao(mensagem, resultado)

    def _confirmar_publicacao(self, mensagem, resultado):
        try:
            self._enviar(codificar(
                Operacao.ACK_PUBLICACAO, mensagem.id_solicitacao,
                uuid=mensagem.campos["uuid"], resultado=resultado,
            ))
        finally:
            if resultado == Resultado.OK:
                self.server.avisar_entregas()

    def _enviar_pendentes(self):
        enviados = {}
        try:
            while not self._parar_envio.is_set() and not self.server.encerrando.is_set():
                self._acordar_envio.clear()
                self._publicar_pendente()
                pendentes = self.server.armazenamento.listar_entregas_pendentes(self.cliente_id)
                atuais = {campos["uuid"] for campos in pendentes}
                enviados = {uuid: prazo for uuid, prazo in enviados.items() if uuid in atuais}
                for campos in pendentes:
                    if self._parar_envio.is_set() or self.server.encerrando.is_set():
                        return
                    ultimo_envio = enviados.get(campos["uuid"])
                    if ultimo_envio is None or monotonic() - ultimo_envio >= self.server.tempo_reenvio:
                        self._enviar(codificar(Operacao.ENTREGAR, 0, **campos))
                        enviados[campos["uuid"]] = monotonic()
                self._acordar_envio.wait(self.server.tempo_reenvio)
        except (OSError, sqlite3.Error) as erro:
            if isinstance(erro, sqlite3.Error):
                LOG.exception("Falha no armazenamento; encerrando conexao")
            else:
                LOG.debug("Envio interrompido para %s", self.cliente_id)
            try:
                self.request.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass


class BrokerTCP:
    def __init__(
        self, host="127.0.0.1", porta=5000, banco="dados/broker.db", *,
        tempo_reenvio=TEMPO_REENVIO,
        capacidade=CAPACIDADE_MENSAGENS,
    ):
        if tempo_reenvio <= 0:
            raise ValueError("O tempo de reenvio deve ser positivo")
        self.armazenamento = Armazenamento(banco, capacidade=capacidade)
        self._servidor = _ServidorTCP((host, porta), self.armazenamento, tempo_reenvio)
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
        self._servidor.avisar_entregas()
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
