import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import UUID

from broker.armazenamento import Armazenamento
from shared.protocolo import Resultado


UUID_PUBLICACAO = UUID("00112233-4455-6677-8899-aabbccddeeff")


class TestArmazenamento(unittest.TestCase):
    def setUp(self):
        pasta = tempfile.TemporaryDirectory()
        self.addCleanup(pasta.cleanup)
        self.caminho = Path(pasta.name) / "dados" / "broker.db"
        self.banco = Armazenamento(self.caminho)

    def test_inicializacao_preserva_topicos_e_inscricoes(self):
        self.assertTrue(self.banco.criar_topico("temperatura"))
        self.assertFalse(self.banco.criar_topico("temperatura"))
        self.banco.registrar_cliente("painel_01")
        self.banco.inscrever("painel_01", "temperatura")
        self.banco.inscrever("painel_01", "temperatura")
        reaberto = Armazenamento(self.caminho)
        self.assertEqual(reaberto.listar_topicos(), ["temperatura"])
        self.assertEqual(reaberto.listar_inscritos("temperatura"), ["painel_01"])

    def test_inscricao_exige_cliente_e_topico_existentes(self):
        self.banco.registrar_cliente("painel_01")
        with self.assertRaises(sqlite3.IntegrityError):
            self.banco.inscrever("painel_01", "inexistente")
        self.banco.criar_topico("temperatura")
        with self.assertRaises(sqlite3.IntegrityError):
            self.banco.inscrever("desconhecido", "temperatura")

    def test_nao_ignora_nomes_vazios(self):
        with self.assertRaises(sqlite3.IntegrityError):
            self.banco.criar_topico("")
        with self.assertRaises(sqlite3.IntegrityError):
            self.banco.registrar_cliente("")

    def test_topico_e_texto_nao_comando_sql(self):
        nome = "agua'); DROP TABLE topicos; --"
        self.banco.criar_topico(nome)
        self.assertEqual(self.banco.listar_topicos(), [nome])

    def test_criacao_concorrente_do_mesmo_topico(self):
        with ThreadPoolExecutor(max_workers=8) as tarefas:
            resultados = list(tarefas.map(self.banco.criar_topico, ["agua"] * 16))
        self.assertEqual(sum(resultados), 1)
        self.assertEqual(self.banco.listar_topicos(), ["agua"])

    def test_esquema_preserva_pendencias_e_identificador_concluido(self):
        self.banco.criar_topico("agua")
        self.banco.registrar_cliente("painel_01")
        mensagem_id = UUID_PUBLICACAO.bytes
        conexao = sqlite3.connect(self.caminho)
        try:
            conexao.execute("PRAGMA foreign_keys = ON")
            with conexao:
                conexao.execute(
                    "INSERT INTO mensagens(mensagem_id, topico, temperatura) VALUES (?, ?, ?)",
                    (mensagem_id, "agua", 28.0),
                )
                conexao.execute(
                    "INSERT INTO entregas(mensagem_id, cliente_id) VALUES (?, ?)",
                    (mensagem_id, "painel_01"),
                )
            with self.assertRaises(sqlite3.IntegrityError):
                with conexao:
                    conexao.execute("DELETE FROM topicos WHERE nome = 'agua'")
        finally:
            conexao.close()

        Armazenamento(self.caminho)
        conexao = sqlite3.connect(self.caminho)
        try:
            conexao.execute("PRAGMA foreign_keys = ON")
            self.assertEqual(
                conexao.execute("SELECT cliente_id, confirmado FROM entregas").fetchall(),
                [("painel_01", 0)],
            )
            with conexao:
                conexao.execute(
                    "INSERT INTO identificadores_concluidos(mensagem_id, resultado) VALUES (?, ?)",
                    (mensagem_id, int(Resultado.OK)),
                )
                conexao.execute("DELETE FROM mensagens WHERE mensagem_id = ?", (mensagem_id,))
            self.assertEqual(conexao.execute("SELECT COUNT(*) FROM entregas").fetchone()[0], 0)
            self.assertEqual(
                conexao.execute("SELECT mensagem_id, resultado FROM identificadores_concluidos").fetchall(),
                [(mensagem_id, int(Resultado.OK))],
            )
        finally:
            conexao.close()


if __name__ == "__main__":
    unittest.main()
