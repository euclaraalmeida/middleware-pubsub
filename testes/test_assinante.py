import tempfile
import unittest
from pathlib import Path
from queue import Empty
from unittest.mock import Mock, call, patch
from uuid import uuid4

from broker.servidor import BrokerTCP
from client.assinante import Assinante
from shared.protocolo import AcumuladorQuadros, Operacao, Resultado, codificar, decodificar


def resposta(identificador, resultado=Resultado.OK):
    return codificar(Operacao.RESPOSTA, identificador, resultado=resultado)


class TestAssinante(unittest.TestCase):
    def setUp(self):
        self.assinante = Assinante(cliente_id="painel-01")
        self.conexao = Mock()
        self.assinante._conexao = self.conexao
        self.assinante._acumulador = AcumuladorQuadros()
        self.addCleanup(self.assinante.fechar)
        self.uuid = uuid4()
        self.entrega = codificar(
            Operacao.ENTREGAR, 0, uuid=self.uuid, topico="água", temperatura=23.5,
        )

    def test_quatro_comandos_correlacionam_e_preservam_resultado(self):
        casos = [
            (self.assinante.criar_topico, Operacao.CRIAR_TOPICO, Resultado.TOPICO_JA_EXISTENTE),
            (self.assinante.excluir_topico, Operacao.EXCLUIR_TOPICO, Resultado.TOPICO_COM_PENDENCIAS),
            (self.assinante.inscrever, Operacao.INSCREVER, Resultado.OK),
            (self.assinante.cancelar_inscricao, Operacao.CANCELAR_INSCRICAO, Resultado.INSCRICAO_COM_PENDENCIAS),
        ]
        for identificador, (metodo, operacao, resultado) in enumerate(casos, 1):
            with self.subTest(operacao=operacao):
                self.conexao.recv.side_effect = [resposta(99), resposta(identificador, resultado)]
                mensagem = metodo("água")
                self.assertEqual(mensagem, decodificar(resposta(identificador, resultado)))
                self.conexao.sendall.assert_called_with(codificar(operacao, identificador, topico="água"))

    def test_entrega_intercalada_com_resposta_gera_ack_depois_de_enfileirar(self):
        entrega = decodificar(self.entrega)

        def conferir_envio(quadro):
            mensagem = decodificar(quadro)
            if mensagem.operacao == Operacao.ACK_ENTREGA:
                self.assertEqual(self.assinante.receber(timeout=0), entrega)
                self.assertEqual(mensagem.id_solicitacao, 0)
                self.assertEqual(mensagem.campos, {"uuid": self.uuid, "cliente_id": "painel-01"})

        self.conexao.sendall.side_effect = conferir_envio
        self.conexao.recv.side_effect = [self.entrega[:6], self.entrega[6:] + resposta(1)]
        self.assertEqual(self.assinante.inscrever("água").campos["resultado"], Resultado.OK)
        self.assertEqual(self.conexao.sendall.call_count, 2)

    def test_entrega_duplicada_reenvia_ack_sem_reenfileirar_apos_consumo(self):
        self.conexao.recv.return_value = self.entrega
        self.assinante._receber_bloco()
        self.assertEqual(self.assinante.receber(timeout=0), decodificar(self.entrega))
        self.assinante._receber_bloco()
        with self.assertRaises(Empty):
            self.assinante.receber(timeout=0)
        ack = codificar(Operacao.ACK_ENTREGA, 0, uuid=self.uuid, cliente_id="painel-01")
        self.assertEqual(self.conexao.sendall.call_args_list, [call(ack), call(ack)])

    def test_reconexao_preserva_fila_e_reconhece_reentrega_apos_erro_de_ack(self):
        acumulador_anterior = self.assinante._acumulador
        self.conexao.recv.return_value = self.entrega
        self.conexao.sendall.side_effect = BrokenPipeError("ACK interrompido")
        nova_conexao = Mock()

        def receber_nova_conexao(_):
            if nova_conexao.recv.call_count == 1:
                return resposta(1) + self.entrega
            self.assinante._parar.set()
            raise TimeoutError

        nova_conexao.recv.side_effect = receber_nova_conexao
        with (
            patch("client.conexao.socket.create_connection", return_value=nova_conexao) as abrir,
            patch.object(self.assinante._parar, "wait", return_value=False) as esperar,
            self.assertLogs("client.assinante", "WARNING"),
        ):
            self.assinante._receber_continuamente()

        esperar.assert_any_call(5)
        abrir.assert_called_once()
        self.conexao.close.assert_called_once()
        self.assertIsNot(self.assinante._acumulador, acumulador_anterior)
        identificacao = codificar(Operacao.IDENTIFICAR, 1, cliente_id="painel-01")
        ack = codificar(Operacao.ACK_ENTREGA, 0, uuid=self.uuid, cliente_id="painel-01")
        self.assertEqual(nova_conexao.sendall.call_args_list, [call(identificacao), call(ack)])
        self.assertEqual(self.assinante.receber(timeout=0), decodificar(self.entrega))
        with self.assertRaises(Empty):
            self.assinante.receber(timeout=0)


class TestAssinanteComBroker(unittest.TestCase):
    def test_identifica_e_cria_topico_com_broker_real(self):
        pasta = self.enterContext(tempfile.TemporaryDirectory())
        broker = self.enterContext(BrokerTCP(porta=0, banco=Path(pasta) / "broker.db"))
        assinante = Assinante(*broker.endereco, cliente_id="painel-real")
        self.addCleanup(assinante.fechar)
        assinante.conectar()
        self.assertEqual(assinante.criar_topico("agua").campos["resultado"], Resultado.OK)
        self.assertEqual(
            assinante.criar_topico("agua").campos["resultado"], Resultado.TOPICO_JA_EXISTENTE,
        )
        self.assertEqual(broker.armazenamento.listar_topicos(), ["agua"])
