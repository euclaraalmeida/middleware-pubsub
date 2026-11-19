import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
from uuid import UUID, uuid4

from broker.servidor import BrokerTCP
from client.publicador import FilaCheia, IdentificacaoRecusada, Publicador
from shared.protocolo import ErroProtocolo, Operacao, Resultado, codificar, decodificar


def ack(id_solicitacao, uuid, resultado=Resultado.OK):
    return codificar(
        Operacao.ACK_PUBLICACAO, id_solicitacao, uuid=uuid, resultado=resultado,
    )


class TestPublicador(unittest.TestCase):
    def setUp(self):
        self.cliente = Publicador()
        self.addCleanup(self.cliente.fechar)
        self.conexao = Mock()
        self.abrir_conexao = self.enterContext(
            patch("client.conexao.socket.create_connection", return_value=self.conexao)
        )
        self.enterContext(patch("client.conexao.monotonic", return_value=100))

    def quadros_enviados(self):
        return [
            decodificar(chamada.args[0]) for chamada in self.conexao.sendall.call_args_list
        ]

    def test_fila_cheia_rejeita_nova_leitura(self):
        identificadores = {self.cliente.enfileirar("agua", 25) for _ in range(1000)}
        self.assertEqual(len(identificadores), 1000)
        self.assertTrue(all(isinstance(uuid, UUID) for uuid in identificadores))
        with self.assertRaises(FilaCheia):
            self.cliente.enfileirar("agua", 26)
        self.assertEqual(self.cliente.pendentes, 1000)
        self.abrir_conexao.assert_not_called()

    def test_publica_em_fifo_e_remove_apos_ok_ou_descarte(self):
        primeiro = self.cliente.enfileirar("água", 25.5)
        segundo = self.cliente.enfileirar("rio", 22)
        self.conexao.recv.side_effect = [
            ack(1, primeiro), ack(2, segundo, Resultado.DESCARTADA_SEM_INSCRITOS)
        ]
        self.assertEqual(self.cliente.publicar_proxima(), Resultado.OK)
        self.assertEqual(self.cliente.pendentes, 1)
        self.assertEqual(self.cliente.publicar_proxima(), Resultado.DESCARTADA_SEM_INSCRITOS)
        self.assertIsNone(self.cliente.publicar_proxima())
        self.assertEqual(self.cliente.pendentes, 0)
        quadros = self.quadros_enviados()
        self.assertEqual([quadro.operacao for quadro in quadros], [Operacao.PUBLICAR] * 2)
        self.assertEqual([quadro.id_solicitacao for quadro in quadros], [1, 2])
        self.assertEqual(quadros[0].campos, {"uuid": primeiro, "topico": "água", "temperatura": 25.5})
        self.assertEqual(quadros[1].campos, {"uuid": segundo, "topico": "rio", "temperatura": 22.0})
        self.abrir_conexao.assert_called_once()

    def test_timeout_reenvia_com_mesmo_uuid_e_ignora_ack_atrasado(self):
        uuid = self.cliente.enfileirar("agua", 25)
        atrasado = ack(1, uuid)
        self.conexao.recv.side_effect = [atrasado[:3], TimeoutError(), atrasado[3:] + ack(2, uuid)]
        with self.assertLogs("client.publicador", "INFO") as logs:
            self.assertEqual(self.cliente.publicar_proxima(), Resultado.OK)
        primeiro, segundo = self.quadros_enviados()
        self.assertEqual((primeiro.id_solicitacao, segundo.id_solicitacao), (1, 2))
        self.assertEqual(primeiro.campos, segundo.campos)
        self.conexao.settimeout.assert_called_with(5)
        self.assertTrue(any("retransmitindo" in item for item in logs.output))
        self.assertEqual(self.cliente.pendentes, 0)

    def test_resultados_de_recusa_preservam_leitura(self):
        uuid = self.cliente.enfileirar("agua", 25)
        recusas = [resultado for resultado in Resultado if int(resultado) > 1]
        self.conexao.recv.side_effect = [
            ack(i, uuid, resultado) for i, resultado in enumerate(recusas, 1)
        ]
        for resultado in recusas:
            with self.subTest(resultado=resultado), self.assertLogs("client.publicador", "WARNING"):
                self.assertEqual(self.cliente.publicar_proxima(), resultado)
                self.assertEqual(self.cliente.pendentes, 1)

    def test_ack_com_uuid_incorreto_preserva_leitura_e_fecha_conexao(self):
        self.cliente.enfileirar("agua", 25)
        self.conexao.recv.return_value = ack(1, uuid4())
        with self.assertRaisesRegex(ErroProtocolo, "UUID do ACK"):
            self.cliente.publicar_proxima()
        self.assertEqual(self.cliente.pendentes, 1)
        self.conexao.close.assert_called_once()

    def test_desconexao_preserva_fila_e_permite_nova_tentativa(self):
        uuid = self.cliente.enfileirar("agua", 25)
        nova_conexao = Mock()
        self.abrir_conexao.side_effect = [self.conexao, nova_conexao]
        self.conexao.recv.return_value = b""
        nova_conexao.recv.return_value = ack(2, uuid)
        with self.assertRaises(ConnectionError):
            self.cliente.publicar_proxima()
        self.assertEqual(self.cliente.pendentes, 1)
        self.conexao.close.assert_called_once()
        self.assertEqual(self.cliente.publicar_proxima(), Resultado.OK)
        self.assertEqual(decodificar(nova_conexao.sendall.call_args.args[0]).campos["uuid"], uuid)

    def test_leitura_invalida_nao_ocupa_fila(self):
        with self.assertRaises(ErroProtocolo):
            self.cliente.enfileirar("agua", float("nan"))
        self.assertEqual(self.cliente.pendentes, 0)


class TestPublicadorComBroker(unittest.TestCase):
    def test_identificacao_reutiliza_conexao_com_broker_real(self):
        pasta = self.enterContext(tempfile.TemporaryDirectory())
        broker = self.enterContext(BrokerTCP(porta=0, banco=Path(pasta) / "broker.db"))
        cliente = Publicador(*broker.endereco, cliente_id="sensor-teste")
        concorrente = Publicador(*broker.endereco, cliente_id="sensor-teste")
        self.addCleanup(cliente.fechar)
        self.addCleanup(concorrente.fechar)

        cliente.conectar()
        conexao_original = cliente._conexao
        cliente.conectar()
        self.assertIs(cliente._conexao, conexao_original)
        with self.assertRaises(IdentificacaoRecusada) as erro:
            concorrente.conectar()
        self.assertEqual(erro.exception.resultado, Resultado.CLIENTE_ID_EM_USO)


if __name__ == "__main__":
    unittest.main()
