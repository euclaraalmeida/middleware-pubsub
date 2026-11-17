import socket
import tempfile
import time
import unittest
from pathlib import Path
from uuid import UUID

from broker.servidor import BrokerTCP
from shared.protocolo import (
    MAX_CONTEUDO,
    Operacao,
    Resultado,
    codificar,
    decodificar,
)


UUID_PUBLICACAO = UUID("00112233-4455-6677-8899-aabbccddeeff")


def receber_resposta(conexao):
    def ler(tamanho):
        dados = bytearray()
        while len(dados) < tamanho:
            parte = conexao.recv(tamanho - len(dados))
            if not parte:
                raise AssertionError("Broker encerrou antes de enviar uma resposta completa")
            dados.extend(parte)
        return bytes(dados)

    prefixo = ler(4)
    return decodificar(prefixo + ler(int.from_bytes(prefixo, "big")))


class TestBroker(unittest.TestCase):
    def setUp(self):
        pasta = tempfile.TemporaryDirectory()
        self.addCleanup(pasta.cleanup)
        self.caminho = Path(pasta.name) / "broker.db"
        self.broker = BrokerTCP(porta=0, banco=self.caminho).iniciar()
        self.addCleanup(self.broker.encerrar)

    def conectar(self):
        conexao = socket.create_connection(self.broker.endereco, timeout=2)
        self.addCleanup(conexao.close)
        return conexao

    def identificar(self, conexao, cliente_id="painel_01"):
        conexao.sendall(codificar(Operacao.IDENTIFICAR, 1, cliente_id=cliente_id))
        return receber_resposta(conexao)

    def test_identificacao_e_topico_em_quadros_agrupados(self):
        conexao = self.conectar()
        conexao.sendall(
            codificar(Operacao.IDENTIFICAR, 11, cliente_id="painel_01")
            + codificar(Operacao.CRIAR_TOPICO, 12, topico="temperatura")
        )
        for identificador in (11, 12):
            resposta = receber_resposta(conexao)
            self.assertEqual(resposta.operacao, Operacao.RESPOSTA)
            self.assertEqual(resposta.id_solicitacao, identificador)
            self.assertEqual(resposta.campos["resultado"], Resultado.OK)
        self.assertEqual(self.broker.armazenamento.listar_topicos(), ["temperatura"])
        conexao.sendall(codificar(Operacao.CRIAR_TOPICO, 13, topico="temperatura"))
        self.assertEqual(
            receber_resposta(conexao).campos["resultado"], Resultado.TOPICO_JA_EXISTENTE,
        )

    def test_cliente_lento_e_quadro_fragmentado_nao_bloqueiam_outro(self):
        lento = self.conectar()
        quadro = codificar(Operacao.IDENTIFICAR, 21, cliente_id="lento")
        lento.sendall(quadro[:2])
        saudavel = self.conectar()
        self.assertEqual(self.identificar(saudavel).campos["resultado"], Resultado.OK)
        lento.sendall(quadro[2:7])
        lento.sendall(quadro[7:])
        resposta = receber_resposta(lento)
        self.assertEqual(resposta.id_solicitacao, 21)
        self.assertEqual(resposta.campos["resultado"], Resultado.OK)

    def test_exige_identificacao_antes_de_criar_topico(self):
        conexao = self.conectar()
        conexao.sendall(codificar(Operacao.CRIAR_TOPICO, 1, topico="agua"))
        self.assertEqual(
            receber_resposta(conexao).campos["resultado"], Resultado.REQUISICAO_INVALIDA,
        )
        self.assertEqual(self.broker.armazenamento.listar_topicos(), [])

    def test_cliente_id_duplicado_e_liberado_apos_desconexao(self):
        primeiro = self.conectar()
        self.assertEqual(self.identificar(primeiro).campos["resultado"], Resultado.OK)
        segundo = self.conectar()
        self.assertEqual(
            self.identificar(segundo).campos["resultado"], Resultado.CLIENTE_ID_EM_USO,
        )
        primeiro.shutdown(socket.SHUT_RDWR)
        primeiro.close()
        limite = time.monotonic() + 2
        while True:
            resultado = self.identificar(segundo).campos["resultado"]
            if resultado == Resultado.OK:
                break
            self.assertEqual(resultado, Resultado.CLIENTE_ID_EM_USO)
            self.assertLess(time.monotonic(), limite, "Identificador nao foi liberado")
            time.sleep(0.01)

    def test_publicacao_ainda_nao_recebe_ack_de_sucesso(self):
        conexao = self.conectar()
        self.identificar(conexao)
        conexao.sendall(codificar(
            Operacao.PUBLICAR, 7,
            uuid=UUID_PUBLICACAO, topico="agua", temperatura=28,
        ))
        resposta = receber_resposta(conexao)
        self.assertEqual(resposta.operacao, Operacao.ACK_PUBLICACAO)
        self.assertEqual(resposta.id_solicitacao, 7)
        self.assertEqual(resposta.campos["uuid"], UUID_PUBLICACAO)
        self.assertEqual(resposta.campos["resultado"], Resultado.REQUISICAO_INVALIDA)

    def test_prefixo_fora_do_limite_encerra_apenas_a_conexao_invalida(self):
        invalido = self.conectar()
        with self.assertLogs("broker.servidor", level="WARNING"):
            invalido.sendall((MAX_CONTEUDO + 1).to_bytes(4, "big"))
            self.assertEqual(invalido.recv(1), b"")
        saudavel = self.conectar()
        self.assertEqual(self.identificar(saudavel).campos["resultado"], Resultado.OK)

    def test_reinicio_recupera_topico_e_nao_conexao_antiga(self):
        conexao = self.conectar()
        self.identificar(conexao)
        conexao.sendall(codificar(Operacao.CRIAR_TOPICO, 2, topico="agua"))
        self.assertEqual(receber_resposta(conexao).campos["resultado"], Resultado.OK)
        self.broker.encerrar()
        novo = BrokerTCP(porta=0, banco=self.caminho).iniciar()
        self.addCleanup(novo.encerrar)
        self.assertEqual(novo.armazenamento.listar_topicos(), ["agua"])
        reconectado = socket.create_connection(novo.endereco, timeout=2)
        self.addCleanup(reconectado.close)
        self.assertEqual(self.identificar(reconectado).campos["resultado"], Resultado.OK)

    def test_encerramento_fecha_cliente_com_quadro_incompleto(self):
        conexao = self.conectar()
        conexao.sendall(b"\x00\x00")
        self.broker.encerrar()
        self.broker.encerrar()
        try:
            self.assertEqual(conexao.recv(1), b"")
        except ConnectionResetError:
            pass


if __name__ == "__main__":
    unittest.main()
