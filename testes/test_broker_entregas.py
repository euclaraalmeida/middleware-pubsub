import socket
import sqlite3
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from queue import Empty
from unittest.mock import patch
from uuid import UUID

from broker.servidor import BrokerTCP
from client.assinante import Assinante
from client.publicador import Publicador
from shared.protocolo import Operacao, Resultado, codificar, decodificar
from testes.test_broker import receber_resposta


UUID_PUBLICACAO = UUID("00112233-4455-6677-8899-aabbccddeeff")


class TestEntregasNoBroker(unittest.TestCase):
    def setUp(self):
        pasta = self.enterContext(tempfile.TemporaryDirectory())
        self.caminho = Path(pasta) / "broker.db"
        self.broker = BrokerTCP(porta=0, banco=self.caminho, tempo_reenvio=0.1).iniciar()
        self.addCleanup(self.broker.encerrar)
        self.banco = self.broker.armazenamento
        self.banco.criar_topico("agua")

    def consultar(self, sql, parametros=()):
        with closing(sqlite3.connect(self.caminho)) as conexao:
            return conexao.execute(sql, parametros).fetchall()

    def aguardar(self, condicao):
        prazo = time.monotonic() + 3
        while not condicao():
            self.assertLess(time.monotonic(), prazo, "Estado esperado nao foi alcancado")
            time.sleep(0.01)

    def assinante(self, cliente_id):
        cliente = Assinante(*self.broker.endereco, cliente_id=cliente_id)
        self.addCleanup(cliente.fechar)
        cliente.conectar()
        self.assertEqual(cliente.inscrever("agua").campos["resultado"], Resultado.OK)
        return cliente

    def publicador(self):
        cliente = Publicador(*self.broker.endereco, cliente_id="sensor-01")
        self.addCleanup(cliente.fechar)
        return cliente

    def comando(self, conexao, operacao, identificador, **campos):
        conexao.sendall(codificar(operacao, identificador, **campos))
        while True:
            mensagem = receber_resposta(conexao)
            if mensagem.id_solicitacao == identificador:
                return mensagem
            self.assertEqual(mensagem.operacao, Operacao.ENTREGAR)

    def conectar_bruto(self, cliente_id, inscrever=True):
        conexao = socket.create_connection(self.broker.endereco, timeout=2)
        self.addCleanup(conexao.close)
        self.assertEqual(
            self.comando(conexao, Operacao.IDENTIFICAR, 1, cliente_id=cliente_id)
            .campos["resultado"], Resultado.OK,
        )
        if inscrever:
            self.assertEqual(
                self.comando(conexao, Operacao.INSCREVER, 2, topico="agua")
                .campos["resultado"], Resultado.OK,
            )
        return conexao

    def confirmar(self, conexao, uuid, cliente_id):
        conexao.sendall(codificar(
            Operacao.ACK_ENTREGA, 0, uuid=uuid, cliente_id=cliente_id,
        ))

    def test_fluxo_com_dois_assinantes_e_publicador_reais_da_clara(self):
        primeiro = self.assinante("painel-01")
        segundo = self.assinante("painel-02")
        sensor = self.publicador()
        uuid = sensor.enfileirar("agua", 28.5)
        self.assertEqual(sensor.publicar_proxima(), Resultado.OK)
        for cliente in (primeiro, segundo):
            mensagem = cliente.receber(timeout=2)
            self.assertEqual(mensagem.id_solicitacao, 0)
            self.assertEqual(mensagem.campos, {
                "uuid": uuid, "topico": "agua", "temperatura": 28.5,
            })
        self.aguardar(lambda: self.consultar("SELECT COUNT(*) FROM mensagens") == [(0,)])
        self.assertEqual(self.consultar("SELECT * FROM entregas"), [])
        self.assertEqual(
            self.consultar("SELECT mensagem_id, resultado FROM identificadores_concluidos"),
            [(uuid.bytes, int(Resultado.OK))],
        )

    def test_um_ack_nao_remove_pendencia_do_outro_destinatario(self):
        primeiro = self.assinante("painel-01")
        segundo = self.conectar_bruto("painel-02")
        sensor = self.publicador()
        uuid = sensor.enfileirar("agua", 28)
        sensor.publicar_proxima()
        primeiro.receber(timeout=2)
        entrega = receber_resposta(segundo)
        self.assertEqual(entrega.campos["uuid"], uuid)
        self.aguardar(lambda: self.consultar(
            "SELECT confirmado FROM entregas WHERE cliente_id = 'painel-01'",
        ) == [(1,)])
        self.assertEqual(self.consultar("SELECT COUNT(*) FROM mensagens"), [(1,)])
        self.assertEqual(
            self.consultar("SELECT confirmado FROM entregas WHERE cliente_id = 'painel-02'"),
            [(0,)],
        )
        self.confirmar(segundo, uuid, "painel-02")
        self.aguardar(lambda: not self.consultar("SELECT * FROM mensagens"))

    def test_reentrega_sem_ack_mantem_uuid_e_nao_bloqueia_outro_cliente(self):
        lento = self.conectar_bruto("lento")
        saudavel = self.assinante("saudavel")
        sensor = self.publicador()
        uuid = sensor.enfileirar("agua", 28)
        sensor.publicar_proxima()
        primeira = receber_resposta(lento)
        segunda = receber_resposta(lento)
        self.assertEqual(primeira, segunda)
        self.assertEqual(primeira.campos["uuid"], uuid)
        self.assertEqual(saudavel.receber(timeout=2).campos["uuid"], uuid)
        self.confirmar(lento, uuid, "lento")
        self.aguardar(lambda: not self.consultar("SELECT * FROM mensagens"))

    def test_desconectado_recebe_pendencia_ao_reconectar_com_mesmo_id(self):
        conexao = self.conectar_bruto("painel-01")
        conexao.shutdown(socket.SHUT_RDWR)
        conexao.close()
        self.aguardar(lambda: "painel-01" not in self.broker._servidor.clientes)
        sensor = self.publicador()
        uuid = sensor.enfileirar("agua", 28)
        self.assertEqual(sensor.publicar_proxima(), Resultado.OK)
        novo = self.assinante("painel-01")
        self.assertEqual(novo.receber(timeout=2).campos["uuid"], uuid)
        self.aguardar(lambda: not self.consultar("SELECT * FROM mensagens"))

    def test_novo_inscrito_nao_recebe_publicacao_anterior(self):
        anterior = self.conectar_bruto("anterior")
        sensor = self.publicador()
        uuid = sensor.enfileirar("agua", 28)
        sensor.publicar_proxima()
        self.assertEqual(receber_resposta(anterior).campos["uuid"], uuid)
        novo = self.assinante("novo")
        with self.assertRaises(Empty):
            novo.receber(timeout=0.2)
        self.assertEqual(
            self.consultar("SELECT cliente_id FROM entregas WHERE mensagem_id = ?", (uuid.bytes,)),
            [("anterior",)],
        )

    def test_operacoes_com_pendencias_sao_recusadas_e_funcionam_apos_ack(self):
        painel = self.conectar_bruto("painel-01")
        sensor = self.publicador()
        uuid = sensor.enfileirar("agua", 28)
        sensor.publicar_proxima()
        receber_resposta(painel)
        for operacao, identificador, resultado in (
            (Operacao.CANCELAR_INSCRICAO, 3, Resultado.INSCRICAO_COM_PENDENCIAS),
            (Operacao.EXCLUIR_TOPICO, 4, Resultado.TOPICO_COM_PENDENCIAS),
        ):
            self.assertEqual(
                self.comando(painel, operacao, identificador, topico="agua").campos["resultado"],
                resultado,
            )
        self.confirmar(painel, uuid, "painel-01")
        for operacao, identificador in (
            (Operacao.CANCELAR_INSCRICAO, 5), (Operacao.EXCLUIR_TOPICO, 6),
        ):
            self.assertEqual(
                self.comando(painel, operacao, identificador, topico="agua").campos["resultado"],
                Resultado.OK,
            )
        self.assertEqual(self.banco.listar_topicos(), [])

    def test_comandos_reais_retornam_topico_inexistente_e_excluem_inscricoes(self):
        painel = self.assinante("painel-01")
        self.assertEqual(painel.cancelar_inscricao("agua").campos["resultado"], Resultado.OK)
        self.assertEqual(painel.excluir_topico("agua").campos["resultado"], Resultado.OK)
        for metodo in (painel.inscrever, painel.excluir_topico, painel.cancelar_inscricao):
            with self.subTest(metodo=metodo.__name__):
                self.assertEqual(metodo("agua").campos["resultado"], Resultado.TOPICO_INEXISTENTE)
        self.assertEqual(self.banco.listar_inscritos("agua"), [])

    def test_reenvio_apos_ultimo_ack_nao_recria_entregas(self):
        painel = self.assinante("painel-01")
        sensor = self.publicador()
        uuid = sensor.enfileirar("agua", 28)
        sensor.publicar_proxima()
        painel.receber(timeout=2)
        self.aguardar(lambda: not self.consultar("SELECT * FROM mensagens"))
        bruto = self.conectar_bruto("sensor-reenvio", inscrever=False)
        bruto.sendall(codificar(
            Operacao.PUBLICAR, 10, uuid=uuid, topico="agua", temperatura=28,
        ))
        self.assertEqual(receber_resposta(bruto).campos["resultado"], Resultado.OK)
        with self.assertRaises(Empty):
            painel.receber(timeout=0.2)
        self.assertEqual(self.consultar("SELECT * FROM mensagens"), [])

    def test_ack_com_identidade_de_outro_cliente_nao_libera_pendencia(self):
        primeiro = self.conectar_bruto("painel-01")
        segundo = self.conectar_bruto("painel-02")
        sensor = self.publicador()
        uuid = sensor.enfileirar("agua", 28)
        sensor.publicar_proxima()
        receber_resposta(primeiro)
        receber_resposta(segundo)
        with self.assertLogs("broker.servidor", "WARNING"):
            self.confirmar(primeiro, uuid, "painel-02")
            try:
                self.assertEqual(primeiro.recv(1), b"")
            except ConnectionResetError:
                pass
        self.assertEqual(
            self.consultar("SELECT cliente_id, confirmado FROM entregas ORDER BY cliente_id"),
            [("painel-01", 0), ("painel-02", 0)],
        )

    def test_ack_duplicado_e_confirmacoes_concorrentes_sao_idempotentes(self):
        clientes = [f"painel-{i}" for i in range(8)]
        for cliente_id in clientes:
            self.banco.registrar_cliente(cliente_id)
            self.banco.inscrever(cliente_id, "agua")
        self.banco.publicar(UUID_PUBLICACAO, "agua", 28)
        with ThreadPoolExecutor(max_workers=8) as tarefas:
            resultados = list(tarefas.map(
                lambda cliente_id: self.banco.confirmar_entrega(UUID_PUBLICACAO, cliente_id),
                clientes * 2,
            ))
        self.assertEqual(sum(resultados), len(clientes))
        self.assertEqual(self.consultar("SELECT * FROM mensagens"), [])
        self.assertEqual(self.consultar("SELECT * FROM entregas"), [])
        self.assertEqual(self.consultar("SELECT COUNT(*) FROM identificadores_concluidos"), [(1,)])

    def test_reinicio_do_broker_preserva_pendencia_e_resultado_final(self):
        painel = self.conectar_bruto("painel-01")
        sensor = self.publicador()
        uuid = sensor.enfileirar("agua", 28)
        sensor.publicar_proxima()
        receber_resposta(painel)
        self.broker.encerrar()
        self.broker = BrokerTCP(porta=0, banco=self.caminho, tempo_reenvio=0.1).iniciar()
        self.addCleanup(self.broker.encerrar)
        self.banco = self.broker.armazenamento
        novo = self.assinante("painel-01")
        self.assertEqual(novo.receber(timeout=2).campos["uuid"], uuid)
        self.aguardar(lambda: not self.consultar("SELECT * FROM mensagens"))
        self.assertEqual(self.banco.publicar(uuid, "agua", 28), Resultado.OK)
        self.assertEqual(self.consultar("SELECT * FROM mensagens"), [])

    def test_assinante_real_reconfirma_apos_perda_de_ack_sem_reenfileirar(self):
        painel = self.assinante("painel-01")
        sensor = self.publicador()
        enviar = painel._enviar
        confirmacoes = []

        def perder_primeiro_ack(quadro):
            mensagem = decodificar(quadro)
            if mensagem.operacao == Operacao.ACK_ENTREGA:
                confirmacoes.append(mensagem.campos["uuid"])
                if len(confirmacoes) == 1:
                    return
            enviar(quadro)

        with patch.object(painel, "_enviar", side_effect=perder_primeiro_ack):
            uuid = sensor.enfileirar("agua", 28)
            self.assertEqual(sensor.publicar_proxima(), Resultado.OK)
            self.assertEqual(painel.receber(timeout=2).campos["uuid"], uuid)
            self.aguardar(lambda: not self.consultar("SELECT * FROM mensagens"))
        self.assertGreaterEqual(len(confirmacoes), 2)
        self.assertEqual(set(confirmacoes), {uuid})
        with self.assertRaises(Empty):
            painel.receber(timeout=0.2)

    def test_assinante_reconecta_automaticamente_apos_reinicio_na_mesma_porta(self):
        painel = self.assinante("painel-01")
        endereco = self.broker.endereco
        with patch("client.assinante.INTERVALO_RECONEXAO", 0.05):
            self.broker.encerrar()
            self.broker = BrokerTCP(
                *endereco, banco=self.caminho, tempo_reenvio=0.1,
            ).iniciar()
            self.addCleanup(self.broker.encerrar)
            self.banco = self.broker.armazenamento
            sensor = self.publicador()
            uuid = sensor.enfileirar("agua", 28)
            self.assertEqual(sensor.publicar_proxima(), Resultado.OK)
            self.assertEqual(painel.receber(timeout=3).campos["uuid"], uuid)
            self.aguardar(lambda: not self.consultar("SELECT * FROM mensagens"))


if __name__ == "__main__":
    unittest.main()
