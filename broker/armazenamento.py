"""Banco SQLite do broker."""

import sqlite3
from contextlib import contextmanager
from pathlib import Path


ESQUEMA = """
CREATE TABLE IF NOT EXISTS clientes (
    cliente_id TEXT PRIMARY KEY NOT NULL CHECK (length(cliente_id) > 0)
);

CREATE TABLE IF NOT EXISTS topicos (
    nome TEXT PRIMARY KEY NOT NULL CHECK (length(nome) > 0)
);

CREATE TABLE IF NOT EXISTS inscricoes (
    cliente_id TEXT NOT NULL REFERENCES clientes(cliente_id),
    topico TEXT NOT NULL REFERENCES topicos(nome),
    PRIMARY KEY (cliente_id, topico)
);

CREATE TABLE IF NOT EXISTS mensagens (
    mensagem_id BLOB PRIMARY KEY NOT NULL CHECK (length(mensagem_id) = 16),
    topico TEXT NOT NULL REFERENCES topicos(nome),
    temperatura REAL NOT NULL,
    criado_em TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS entregas (
    mensagem_id BLOB NOT NULL
        REFERENCES mensagens(mensagem_id) ON DELETE CASCADE,
    cliente_id TEXT NOT NULL REFERENCES clientes(cliente_id),
    confirmado INTEGER NOT NULL DEFAULT 0 CHECK (confirmado IN (0, 1)),
    PRIMARY KEY (mensagem_id, cliente_id)
);

CREATE INDEX IF NOT EXISTS entregas_por_cliente
    ON entregas(cliente_id, confirmado);

CREATE TABLE IF NOT EXISTS identificadores_concluidos (
    mensagem_id BLOB PRIMARY KEY NOT NULL CHECK (length(mensagem_id) = 16),
    resultado INTEGER NOT NULL CHECK (resultado BETWEEN 0 AND 255)
);
"""


class Armazenamento:
    def __init__(self, caminho="dados/broker.db"):
        self.caminho = Path(caminho)
        self.caminho.parent.mkdir(parents=True, exist_ok=True)
        with self._conexao() as conexao:
            conexao.execute("PRAGMA journal_mode = WAL")
            conexao.executescript(ESQUEMA)

    @contextmanager
    def _conexao(self):
        # Cada operacao abre sua propria conexao, sem compartilha-la entre threads.
        conexao = sqlite3.connect(self.caminho, timeout=5)
        try:
            conexao.execute("PRAGMA foreign_keys = ON")
            with conexao:
                yield conexao
        finally:
            conexao.close()

    def registrar_cliente(self, cliente_id):
        with self._conexao() as conexao:
            conexao.execute(
                "INSERT INTO clientes(cliente_id) VALUES (?) "
                "ON CONFLICT(cliente_id) DO NOTHING",
                (cliente_id,),
            )

    def criar_topico(self, topico):
        """Retorna True se criou o topico, False se ele ja existia."""
        with self._conexao() as conexao:
            cursor = conexao.execute(
                "INSERT INTO topicos(nome) VALUES (?) ON CONFLICT(nome) DO NOTHING",
                (topico,),
            )
            return cursor.rowcount == 1

    def listar_topicos(self):
        with self._conexao() as conexao:
            return [
                linha[0]
                for linha in conexao.execute("SELECT nome FROM topicos ORDER BY nome")
            ]

    def inscrever(self, cliente_id, topico):
        """Cadastra uma inscricao; cliente e topico precisam existir."""
        with self._conexao() as conexao:
            conexao.execute(
                "INSERT INTO inscricoes(cliente_id, topico) VALUES (?, ?) "
                "ON CONFLICT(cliente_id, topico) DO NOTHING",
                (cliente_id, topico),
            )

    def listar_inscritos(self, topico):
        with self._conexao() as conexao:
            return [
                linha[0]
                for linha in conexao.execute(
                    "SELECT cliente_id FROM inscricoes WHERE topico = ? ORDER BY cliente_id",
                    (topico,),
                )
            ]
