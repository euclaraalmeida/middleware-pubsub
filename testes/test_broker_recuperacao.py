import socket
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from uuid import uuid4

from broker.armazenamento import Armazenamento
from broker.estado import consultar_estado
from broker.servidor import BrokerTCP
from client.assinante import Assinante
from shared.protocolo import Operacao, Resultado, codificar
from testes.test_broker import receber_resposta


class TestFalhaDoProcesso(unittest.TestCase):
    def test_publicacao_confirmada_sobrevive_a_termino_forcado_do_broker(self):
        pasta = self.enterContext(tempfile.TemporaryDirectory())
        caminho = Path(pasta) / "broker.db"
        banco = Armazenamento(caminho)
        banco.criar_topico("agua")
        banco.registrar_cliente("painel")
        banco.inscrever("painel", "agua")
        codigo = (
            "import sys, threading; from broker.servidor import BrokerTCP; "
            "b = BrokerTCP(porta=0, banco=sys.argv[1]).iniciar(); "
            "print(b.endereco[1], flush=True); threading.Event().wait(15)"
        )
        processo = subprocess.Popen(
            [sys.executable, "-u", "-c", codigo, str(caminho)],
            cwd=Path(__file__).resolve().parents[1], stdout=subprocess.PIPE, text=True,
        )
        try:
            porta = int(processo.stdout.readline())
            with socket.create_connection(("127.0.0.1", porta), timeout=2) as sensor:
                sensor.sendall(codificar(Operacao.IDENTIFICAR, 1, cliente_id="sensor"))
                receber_resposta(sensor)
                uuid = uuid4()
                sensor.sendall(codificar(
                    Operacao.PUBLICAR, 2, uuid=uuid, topico="agua", temperatura=25,
                ))
                self.assertEqual(receber_resposta(sensor).campos["resultado"], Resultado.OK)
                processo.kill()
                processo.wait(timeout=3)
        finally:
            if processo.poll() is None:
                processo.kill()
                processo.wait(timeout=3)
            processo.stdout.close()

        with BrokerTCP(porta=0, banco=caminho) as broker:
            self.assertEqual(consultar_estado(caminho)["mensagens_pendentes"], 1)
            painel = Assinante(*broker.endereco, cliente_id="painel")
            try:
                painel.conectar()
                self.assertEqual(painel.receber(timeout=3).campos["uuid"], uuid)
                prazo = time.monotonic() + 3
                while consultar_estado(caminho)["mensagens_pendentes"]:
                    self.assertLess(time.monotonic(), prazo)
                    time.sleep(0.01)
                self.assertEqual(broker.armazenamento.publicar(uuid, "agua", 25), Resultado.OK)
                self.assertEqual(consultar_estado(caminho)["mensagens_pendentes"], 0)
            finally:
                painel.fechar()


if __name__ == "__main__":
    unittest.main()
