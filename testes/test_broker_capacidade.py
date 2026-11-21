import select
import socket
import sqlite3
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from unittest.mock import patch
from uuid import UUID, uuid4

from broker.armazenamento import Armazenamento
from broker.estado import consultar_estado
from broker.servidor import BrokerTCP, _Conexao
from client.publicador import Publicador
from shared.protocolo import Operacao, Resultado, codificar, decodificar
from testes.test_broker import receber_resposta


class TestCapacidadeArmazenamento(unittest.TestCase):
    def setUp(self):
        pasta = self.enterContext(tempfile.TemporaryDirectory())
        self.caminho = Path(pasta) / "broker.db"
        self.banco = Armazenamento(self.caminho, capacidade=4)
        self.banco.criar_topico("agua")
        self.banco.registrar_cliente("painel")
        self.banco.inscrever("painel", "agua")

    def test_publicacoes_concorrentes_respeitam_limite(self):
        with ThreadPoolExecutor(max_workers=8) as grupo:
            resultados = list(grupo.map(
                lambda _: self.banco.publicar(uuid4(), "agua", 25), range(24),
            ))
        self.assertEqual(resultados.count(Resultado.OK), 4)
        self.assertEqual(resultados.count(None), 20)
        self.assertEqual(consultar_estado(self.caminho)["mensagens_pendentes"], 4)

    def test_limite_padrao_de_dez_mil_sobrevive_a_reabertura(self):
        with closing(sqlite3.connect(self.caminho)) as conexao:
            with conexao:
                conexao.executemany(
                    "INSERT INTO mensagens(mensagem_id, topico, temperatura) VALUES (?, 'agua', 25)",
                    [(UUID(int=i).bytes,) for i in range(1, 10_001)],
                )
                conexao.execute(
                    "INSERT INTO entregas(mensagem_id, cliente_id) "
                    "SELECT mensagem_id, 'painel' FROM mensagens"
                )
        reaberto = Armazenamento(self.caminho)
        novo = uuid4()
        self.assertIsNone(reaberto.publicar(novo, "agua", 26))
        self.assertEqual(consultar_estado(self.caminho)["mensagens_pendentes"], 10_000)
        reaberto.confirmar_entrega(UUID(int=1), "painel")
        self.assertEqual(reaberto.publicar(novo, "agua", 26), Resultado.OK)
        self.assertEqual(consultar_estado(self.caminho)["mensagens_pendentes"], 10_000)

    def test_banco_cheio_ainda_responde_duplicatas_descarte_e_topico_inexistente(self):
        concluida = uuid4()
        self.banco.publicar(concluida, "agua", 25)
        self.banco.confirmar_entrega(concluida, "painel")
        pendentes = [uuid4() for _ in range(4)]
        for uuid in pendentes:
            self.banco.publicar(uuid, "agua", 25)
        self.banco.criar_topico("vazio")
        self.assertEqual(self.banco.publicar(pendentes[0], "agua", 25), Resultado.OK)
        self.assertEqual(self.banco.publicar(concluida, "agua", 25), Resultado.OK)
        self.assertEqual(
            self.banco.publicar(uuid4(), "vazio", 25), Resultado.DESCARTADA_SEM_INSCRITOS,
        )
        self.assertEqual(
            self.banco.publicar(uuid4(), "inexistente", 25), Resultado.TOPICO_INEXISTENTE,
        )
        self.assertEqual(consultar_estado(self.caminho)["mensagens_pendentes"], 4)

    def test_consulta_nao_cria_banco_inexistente(self):
        caminho = self.caminho.parent / "inexistente.db"
        with self.assertRaises(sqlite3.OperationalError):
            consultar_estado(caminho)
        self.assertFalse(caminho.exists())


class TestCapacidadeNoBroker(unittest.TestCase):
    def setUp(self):
        pasta = self.enterContext(tempfile.TemporaryDirectory())
        self.caminho = Path(pasta) / "broker.db"
        self.broker = BrokerTCP(porta=0, banco=self.caminho, capacidade=1).iniciar()
        self.addCleanup(self.broker.encerrar)
        self.banco = self.broker.armazenamento
        self.banco.criar_topico("agua")

    def conectar(self, cliente_id, inscrever=False):
        conexao = self.enterContext(socket.create_connection(self.broker.endereco, timeout=2))
        conexao.sendall(codificar(Operacao.IDENTIFICAR, 1, cliente_id=cliente_id))
        self.assertEqual(receber_resposta(conexao).campos["resultado"], Resultado.OK)
        if inscrever:
            conexao.sendall(codificar(Operacao.INSCREVER, 2, topico="agua"))
            self.assertEqual(receber_resposta(conexao).campos["resultado"], Resultado.OK)
        return conexao

    def publicar(self):
        cliente = Publicador(*self.broker.endereco, cliente_id="sensor-inicial")
        self.addCleanup(cliente.fechar)
        uuid = cliente.enfileirar("agua", 25)
        self.assertEqual(cliente.publicar_proxima(), Resultado.OK)
        return uuid

    def enviar(self, conexao, uuid, solicitacao=10):
        conexao.sendall(codificar(
            Operacao.PUBLICAR, solicitacao, uuid=uuid, topico="agua", temperatura=26,
        ))

    def confirmar(self, conexao, uuid, cliente_id):
        conexao.sendall(codificar(Operacao.ACK_ENTREGA, 0, uuid=uuid, cliente_id=cliente_id))

    def sem_resposta(self, conexao):
        self.assertFalse(select.select([conexao], [], [], 0.15)[0], "Recebeu ACK sem espaco")

    def aguardar(self, condicao):
        prazo = time.monotonic() + 3
        while not condicao():
            self.assertLess(time.monotonic(), prazo, "Estado esperado nao foi alcancado")
            time.sleep(0.01)

    def test_apenas_ultimo_ack_libera_vaga_e_publicacao_em_espera(self):
        primeiro = self.conectar("painel-1", True)
        segundo = self.conectar("painel-2", True)
        antigo = self.publicar()
        receber_resposta(primeiro)
        receber_resposta(segundo)
        sensor = self.conectar("sensor-espera")
        novo = uuid4()
        self.enviar(sensor, novo)
        self.sem_resposta(sensor)
        self.confirmar(primeiro, antigo, "painel-1")
        self.sem_resposta(sensor)
        self.assertEqual(consultar_estado(self.caminho)["entregas_pendentes"], 1)
        self.confirmar(segundo, antigo, "painel-2")
        resposta = receber_resposta(sensor)
        self.assertEqual(resposta.campos["uuid"], novo)
        self.assertEqual(resposta.campos["resultado"], Resultado.OK)
        self.assertEqual(consultar_estado(self.caminho)["mensagens_pendentes"], 1)

    def test_reenvios_durante_espera_mantem_uma_publicacao_e_id_atual(self):
        painel = self.conectar("painel", True)
        antigo = self.publicar()
        receber_resposta(painel)
        sensor = self.conectar("sensor-espera")
        novo = uuid4()
        for solicitacao in range(10, 15):
            self.enviar(sensor, novo, solicitacao)
        # A resposta seguinte confirma que as tentativas anteriores foram lidas.
        sensor.sendall(codificar(Operacao.CRIAR_TOPICO, 20, topico="barreira"))
        self.assertEqual(receber_resposta(sensor).id_solicitacao, 20)
        self.sem_resposta(sensor)
        self.confirmar(painel, antigo, "painel")
        resposta = receber_resposta(sensor)
        self.assertEqual(resposta.id_solicitacao, 14)
        self.assertEqual(resposta.campos["uuid"], novo)
        self.assertEqual(consultar_estado(self.caminho)["mensagens_pendentes"], 1)

    def test_mesma_conexao_pode_confirmar_enquanto_publicacao_aguarda(self):
        painel = self.conectar("cliente-misto", True)
        antigo = self.publicar()
        receber_resposta(painel)
        novo = uuid4()
        self.enviar(painel, novo)
        self.sem_resposta(painel)
        self.confirmar(painel, antigo, "cliente-misto")
        resposta = receber_resposta(painel)
        self.assertEqual(resposta.operacao, Operacao.ACK_PUBLICACAO)
        self.assertEqual(resposta.campos["uuid"], novo)
        self.assertEqual(receber_resposta(painel).campos["uuid"], novo)

    def test_encerramento_com_banco_cheio_nao_aguarda_liberacao_de_vaga(self):
        self.banco.registrar_cliente("offline")
        self.banco.inscrever("offline", "agua")
        self.publicar()
        sensor = self.conectar("sensor-espera")
        self.enviar(sensor, uuid4())
        self.sem_resposta(sensor)
        inicio = time.monotonic()
        self.broker.encerrar()
        self.assertLess(time.monotonic() - inicio, 2)
        self.assertEqual(sensor.recv(1), b"")
        self.assertEqual(consultar_estado(self.caminho)["mensagens_pendentes"], 1)

    def test_desconexao_libera_identidade_sem_aceitar_publicacao_em_espera(self):
        painel = self.conectar("painel", True)
        antigo = self.publicar()
        receber_resposta(painel)
        sensor = self.conectar("sensor-espera")
        self.enviar(sensor, uuid4())
        self.sem_resposta(sensor)
        sensor.shutdown(socket.SHUT_RDWR)
        sensor.close()
        self.aguardar(lambda: "sensor-espera" not in self.broker._servidor.clientes)
        self.conectar("sensor-espera")
        self.confirmar(painel, antigo, "painel")
        self.aguardar(lambda: consultar_estado(self.caminho)["mensagens_pendentes"] == 0)

    def test_reinicio_com_banco_cheio_preserva_limite_e_reenvio(self):
        painel = self.conectar("painel", True)
        antigo = self.publicar()
        receber_resposta(painel)
        sensor = self.conectar("sensor-espera")
        novo = uuid4()
        self.enviar(sensor, novo)
        self.sem_resposta(sensor)
        self.broker.encerrar()
        self.broker = BrokerTCP(porta=0, banco=self.caminho, capacidade=1).iniciar()
        self.addCleanup(self.broker.encerrar)
        sensor = self.conectar("sensor-espera")
        self.enviar(sensor, novo)
        self.sem_resposta(sensor)
        painel = self.conectar("painel")
        self.assertEqual(receber_resposta(painel).campos["uuid"], antigo)
        self.confirmar(painel, antigo, "painel")
        self.assertEqual(receber_resposta(sensor).campos["uuid"], novo)
        self.assertEqual(consultar_estado(self.caminho)["mensagens_pendentes"], 1)

    def test_uuid_diferente_sem_aguardar_ack_encerra_apenas_infrator(self):
        self.banco.registrar_cliente("offline")
        self.banco.inscrever("offline", "agua")
        self.publicar()
        sensor = self.conectar("sensor-espera")
        self.enviar(sensor, uuid4())
        self.sem_resposta(sensor)
        with self.assertLogs("broker.servidor", "WARNING"):
            self.enviar(sensor, uuid4(), 11)
            self.assertEqual(sensor.recv(1), b"")
        outro = self.conectar("outro")
        outro.sendall(codificar(Operacao.CRIAR_TOPICO, 2, topico="outro"))
        self.assertEqual(receber_resposta(outro).campos["resultado"], Resultado.OK)

    def test_publicador_real_reenvia_apos_ack_perdido_com_banco_cheio(self):
        self.banco.registrar_cliente("offline")
        self.banco.inscrever("offline", "agua")
        cliente = Publicador(*self.broker.endereco, cliente_id="sensor")
        self.addCleanup(cliente.fechar)
        uuid = cliente.enfileirar("agua", 25)
        enviar = _Conexao._enviar
        acks = []

        def perder_primeiro_ack(conexao, quadro):
            mensagem = decodificar(quadro)
            if mensagem.operacao == Operacao.ACK_PUBLICACAO:
                acks.append(mensagem)
                if len(acks) == 1:
                    return
            enviar(conexao, quadro)

        with patch.object(_Conexao, "_enviar", perder_primeiro_ack):
            with patch("client.conexao.TEMPO_ACK", 0.15):
                self.assertEqual(cliente.publicar_proxima(), Resultado.OK)
        self.assertEqual(cliente.pendentes, 0)
        self.assertEqual(len(acks), 2)
        self.assertEqual({ack.campos["uuid"] for ack in acks}, {uuid})
        self.assertNotEqual(acks[0].id_solicitacao, acks[1].id_solicitacao)
        self.assertEqual(consultar_estado(self.caminho)["mensagens_pendentes"], 1)


if __name__ == "__main__":
    unittest.main()
