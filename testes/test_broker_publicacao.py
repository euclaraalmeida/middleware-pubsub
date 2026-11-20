import socket
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from uuid import UUID

from broker.armazenamento import Armazenamento
from broker.servidor import BrokerTCP
from client.publicador import Publicador
from shared.protocolo import Operacao, Resultado, codificar
from testes.test_broker import receber_resposta


UUID_PUBLICACAO = UUID("00112233-4455-6677-8899-aabbccddeeff")


class TestPublicacaoNoBroker(unittest.TestCase):
    def setUp(self):
        pasta = self.enterContext(tempfile.TemporaryDirectory())
        self.caminho = Path(pasta) / "broker.db"
        self.broker = self.enterContext(BrokerTCP(porta=0, banco=self.caminho))
        self.banco = self.broker.armazenamento
        self.banco.criar_topico("agua")

    def consultar(self, sql, parametros=()):
        with closing(sqlite3.connect(self.caminho)) as conexao:
            return conexao.execute(sql, parametros).fetchall()

    def inscrever(self, cliente_id):
        self.banco.registrar_cliente(cliente_id)
        self.banco.inscrever(cliente_id, "agua")

    def publicador(self, cliente_id="sensor-01"):
        cliente = Publicador(*self.broker.endereco, cliente_id=cliente_id)
        self.addCleanup(cliente.fechar)
        return cliente

    def test_cliente_real_recebe_ack_apos_gravacao_de_mensagem_e_destinatarios(self):
        self.inscrever("painel-01")
        self.inscrever("painel-02")
        cliente = self.publicador()
        uuid = cliente.enfileirar("agua", 28.5)
        self.assertEqual(cliente.publicar_proxima(), Resultado.OK)
        self.assertEqual(cliente.pendentes, 0)
        self.assertEqual(
            self.consultar("SELECT mensagem_id, topico, temperatura FROM mensagens"),
            [(uuid.bytes, "agua", 28.5)],
        )
        self.assertEqual(
            self.consultar("SELECT cliente_id, confirmado FROM entregas ORDER BY cliente_id"),
            [("painel-01", 0), ("painel-02", 0)],
        )

    def test_descarte_persiste_resultado_sem_guardar_leitura(self):
        cliente = self.publicador()
        uuid = cliente.enfileirar("agua", 28)
        self.assertEqual(cliente.publicar_proxima(), Resultado.DESCARTADA_SEM_INSCRITOS)
        self.assertEqual(cliente.pendentes, 0)
        self.assertEqual(self.consultar("SELECT * FROM mensagens"), [])
        self.assertEqual(self.consultar("SELECT * FROM entregas"), [])
        self.assertEqual(
            self.consultar("SELECT mensagem_id, resultado FROM identificadores_concluidos"),
            [(uuid.bytes, int(Resultado.DESCARTADA_SEM_INSCRITOS))],
        )

    def test_reenvio_nao_acrescenta_novo_inscrito_a_publicacao_antiga(self):
        self.inscrever("painel-01")
        self.assertEqual(self.banco.publicar(UUID_PUBLICACAO, "agua", 28), Resultado.OK)
        self.inscrever("painel-02")
        self.assertEqual(self.banco.publicar(UUID_PUBLICACAO, "agua", 28), Resultado.OK)
        self.assertEqual(self.consultar("SELECT COUNT(*) FROM mensagens"), [(1,)])
        self.assertEqual(self.consultar("SELECT cliente_id FROM entregas"), [("painel-01",)])

    def test_reenvio_de_descarte_nao_entrega_a_inscrito_posterior(self):
        self.banco.publicar(UUID_PUBLICACAO, "agua", 28)
        self.inscrever("painel-01")
        self.assertEqual(
            self.banco.publicar(UUID_PUBLICACAO, "agua", 28),
            Resultado.DESCARTADA_SEM_INSCRITOS,
        )
        self.assertEqual(self.consultar("SELECT * FROM mensagens"), [])

    def test_uuid_concorrente_grava_uma_publicacao_e_uma_entrega(self):
        self.inscrever("painel-01")
        with ThreadPoolExecutor(max_workers=8) as tarefas:
            resultados = list(tarefas.map(
                lambda _: self.banco.publicar(UUID_PUBLICACAO, "agua", 28), range(16),
            ))
        self.assertEqual(resultados, [Resultado.OK] * 16)
        self.assertEqual(self.consultar("SELECT COUNT(*) FROM mensagens"), [(1,)])
        self.assertEqual(self.consultar("SELECT COUNT(*) FROM entregas"), [(1,)])

    def test_falha_ao_gravar_entregas_desfaz_mensagem_e_nao_envia_sucesso(self):
        self.inscrever("painel-01")
        with closing(sqlite3.connect(self.caminho)) as conexao:
            with conexao:
                conexao.execute(
                    "CREATE TRIGGER simular_falha BEFORE INSERT ON entregas "
                    "BEGIN SELECT RAISE(ABORT, 'falha de teste'); END"
                )
        cliente = self.publicador()
        cliente.enfileirar("agua", 28)
        with self.assertLogs("broker.servidor", "ERROR"):
            with self.assertRaises(ConnectionError):
                cliente.publicar_proxima()
        self.assertEqual(cliente.pendentes, 1)
        self.assertEqual(self.consultar("SELECT * FROM mensagens"), [])
        self.assertEqual(self.consultar("SELECT * FROM entregas"), [])
        self.assertEqual(self.consultar("SELECT * FROM identificadores_concluidos"), [])

    def test_reenvio_apos_perda_de_ack_nao_duplica(self):
        self.inscrever("painel-01")
        conexao = self.enterContext(socket.create_connection(self.broker.endereco, timeout=2))
        conexao.sendall(codificar(Operacao.IDENTIFICAR, 1, cliente_id="sensor-01"))
        self.assertEqual(receber_resposta(conexao).campos["resultado"], Resultado.OK)
        quadro = codificar(
            Operacao.PUBLICAR, 2, uuid=UUID_PUBLICACAO, topico="agua", temperatura=28,
        )
        conexao.sendall(quadro)
        conexao.sendall(codificar(
            Operacao.PUBLICAR, 3, uuid=UUID_PUBLICACAO, topico="agua", temperatura=28,
        ))
        receber_resposta(conexao)  # A primeira confirmacao e ignorada pelo sensor.
        resposta = receber_resposta(conexao)
        self.assertEqual(resposta.id_solicitacao, 3)
        self.assertEqual(resposta.campos["resultado"], Resultado.OK)
        self.assertEqual(self.consultar("SELECT COUNT(*) FROM mensagens"), [(1,)])
        self.assertEqual(self.consultar("SELECT COUNT(*) FROM entregas"), [(1,)])

    def test_resultado_continua_disponivel_apos_reabrir_banco(self):
        self.inscrever("painel-01")
        self.banco.publicar(UUID_PUBLICACAO, "agua", 28)
        reaberto = Armazenamento(self.caminho)
        self.assertEqual(reaberto.publicar(UUID_PUBLICACAO, "agua", 28), Resultado.OK)
        self.assertEqual(self.consultar("SELECT COUNT(*) FROM entregas"), [(1,)])


if __name__ == "__main__":
    unittest.main()
