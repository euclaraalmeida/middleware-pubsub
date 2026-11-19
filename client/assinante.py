"""Cliente para gerenciar tópicos e consumir entregas em uma fila RAM."""

import logging
from queue import Queue
from threading import Event, Lock, Thread

from shared.protocolo import ErroProtocolo, Mensagem, Operacao, codificar

from .conexao import ConexaoTCP, IdentificacaoRecusada


LOG = logging.getLogger(__name__)
INTERVALO_RECONEXAO = 5


class Assinante(ConexaoTCP):
    """Coordena a leitura de respostas e entregas no mesmo socket."""

    def __init__(self, host="127.0.0.1", porta=5000, *, cliente_id):
        codificar(Operacao.IDENTIFICAR, 1, cliente_id=cliente_id)
        super().__init__(host, porta, cliente_id=cliente_id)
        self._entrada = Queue()
        self._aceitos = set()
        self._trava = Lock()
        self._parar = Event()
        self._receptor = None

    def conectar(self):
        """Identifica o cliente e inicia a recepção em segundo plano."""
        with self._trava:
            if self._receptor is not None:
                return
            super().conectar()
            self._parar.clear()
            self._receptor = Thread(target=self._receber_continuamente, daemon=True)
            self._receptor.start()

    def fechar(self):
        """Interrompe a recepção e encerra a conexão."""
        self._parar.set()
        if self._receptor is not None:
            self._receptor.join()
            self._receptor = None
        with self._trava:
            self._fechar_conexao()

    def criar_topico(self, topico):
        return self._comando(Operacao.CRIAR_TOPICO, topico)

    def excluir_topico(self, topico):
        return self._comando(Operacao.EXCLUIR_TOPICO, topico)

    def inscrever(self, topico):
        return self._comando(Operacao.INSCREVER, topico)

    def cancelar_inscricao(self, topico):
        return self._comando(Operacao.CANCELAR_INSCRICAO, topico)

    def receber(self, timeout=None) -> Mensagem:
        """Retira a primeira entrega. Um prazo expirado gera queue.Empty."""
        return self._entrada.get(timeout=timeout)

    def _comando(self, operacao, topico):
        with self._trava:
            if self._conexao is None or self._parar.is_set():
                raise ConnectionError("Assinante sem conexão com o broker")
            identificador = self._proximo_id()
            quadro = codificar(operacao, identificador, topico=topico)
            try:
                self._enviar(quadro)
                return self._aguardar(identificador, Operacao.RESPOSTA)
            except (OSError, ErroProtocolo):
                self._fechar_conexao()
                raise

    def _tratar_espontanea(self, mensagem):
        if mensagem.operacao != Operacao.ENTREGAR:
            return
        uuid = mensagem.campos["uuid"]
        if uuid not in self._aceitos:
            self._entrada.put_nowait(mensagem)
            self._aceitos.add(uuid)
        self._enviar(codificar(Operacao.ACK_ENTREGA, 0, uuid=uuid, cliente_id=self.cliente_id))

    def _receber_continuamente(self):
        while not self._parar.is_set():
            with self._trava:
                conectado = self._conexao is not None
                if conectado:
                    try:
                        self._receber_bloco()
                    except (OSError, ErroProtocolo) as erro:
                        LOG.warning("Conexão com o broker perdida: %s", erro)
                        self._fechar_conexao()
            if conectado:
                # Dá oportunidade para que um comando use o transporte.
                self._parar.wait(0.01)
            elif not self._parar.wait(INTERVALO_RECONEXAO):
                with self._trava:
                    try:
                        super().conectar()
                    except (OSError, ErroProtocolo, IdentificacaoRecusada) as erro:
                        LOG.warning("Reconexão falhou: %s", erro)

    def _receber_bloco(self):
        try:
            mensagens = self._ler(0.2)
        except TimeoutError:
            return  # Conexão ociosa.
        for mensagem in mensagens:
            self._tratar_espontanea(mensagem)
