"""Banco SQLite do broker."""

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from uuid import UUID

from shared.protocolo import Resultado


CAPACIDADE_MENSAGENS = 10_000


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
    def __init__(self, caminho="dados/broker.db", *, capacidade=CAPACIDADE_MENSAGENS):
        if not isinstance(capacidade, int) or capacidade <= 0:
            raise ValueError("A capacidade deve ser um inteiro positivo")
        self.capacidade = capacidade
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

    def publicar(self, mensagem_id, topico, temperatura):
        """Retorna o resultado, ou None se uma nova leitura precisa esperar espaco."""
        with self._conexao() as conexao:
            # Reserva a escrita antes da consulta para serializar UUIDs concorrentes.
            conexao.execute("BEGIN IMMEDIATE")
            resultado = conexao.execute(
                "SELECT resultado FROM identificadores_concluidos WHERE mensagem_id = ?",
                (mensagem_id.bytes,),
            ).fetchone()
            if resultado is not None:
                return Resultado(resultado[0])
            if conexao.execute(
                "SELECT 1 FROM mensagens WHERE mensagem_id = ?", (mensagem_id.bytes,),
            ).fetchone():
                return Resultado.OK
            if not conexao.execute(
                "SELECT 1 FROM topicos WHERE nome = ?", (topico,),
            ).fetchone():
                return Resultado.TOPICO_INEXISTENTE

            destinatarios = conexao.execute(
                "SELECT cliente_id FROM inscricoes WHERE topico = ?", (topico,),
            ).fetchall()
            if not destinatarios:
                conexao.execute(
                    "INSERT INTO identificadores_concluidos(mensagem_id, resultado) "
                    "VALUES (?, ?)",
                    (mensagem_id.bytes, int(Resultado.DESCARTADA_SEM_INSCRITOS)),
                )
                return Resultado.DESCARTADA_SEM_INSCRITOS

            quantidade = conexao.execute("SELECT COUNT(*) FROM mensagens").fetchone()[0]
            if quantidade >= self.capacidade:
                return None

            conexao.execute(
                "INSERT INTO mensagens(mensagem_id, topico, temperatura) VALUES (?, ?, ?)",
                (mensagem_id.bytes, topico, temperatura),
            )
            conexao.executemany(
                "INSERT INTO entregas(mensagem_id, cliente_id) VALUES (?, ?)",
                [(mensagem_id.bytes, cliente[0]) for cliente in destinatarios],
            )
            return Resultado.OK

    def listar_entregas_pendentes(self, cliente_id):
        with self._conexao() as conexao:
            linhas = conexao.execute(
                "SELECT m.mensagem_id, m.topico, m.temperatura "
                "FROM mensagens m JOIN entregas e ON e.mensagem_id = m.mensagem_id "
                "WHERE e.cliente_id = ? AND e.confirmado = 0 ORDER BY m.rowid",
                (cliente_id,),
            ).fetchall()
            return [
                {"uuid": UUID(bytes=uuid), "topico": topico, "temperatura": temperatura}
                for uuid, topico, temperatura in linhas
            ]

    def confirmar_entrega(self, mensagem_id, cliente_id):
        with self._conexao() as conexao:
            conexao.execute("BEGIN IMMEDIATE")
            cursor = conexao.execute(
                "UPDATE entregas SET confirmado = 1 "
                "WHERE mensagem_id = ? AND cliente_id = ? AND confirmado = 0",
                (mensagem_id.bytes, cliente_id),
            )
            if cursor.rowcount == 0:
                return False
            if not conexao.execute(
                "SELECT 1 FROM entregas WHERE mensagem_id = ? AND confirmado = 0",
                (mensagem_id.bytes,),
            ).fetchone():
                conexao.execute(
                    "INSERT INTO identificadores_concluidos(mensagem_id, resultado) "
                    "VALUES (?, ?)", (mensagem_id.bytes, int(Resultado.OK)),
                )
                conexao.execute(
                    "DELETE FROM mensagens WHERE mensagem_id = ?", (mensagem_id.bytes,),
                )
            return True

    def cancelar_inscricao(self, cliente_id, topico):
        with self._conexao() as conexao:
            conexao.execute("BEGIN IMMEDIATE")
            if not conexao.execute(
                "SELECT 1 FROM topicos WHERE nome = ?", (topico,),
            ).fetchone():
                return Resultado.TOPICO_INEXISTENTE
            if conexao.execute(
                "SELECT 1 FROM entregas e JOIN mensagens m "
                "ON m.mensagem_id = e.mensagem_id "
                "WHERE e.cliente_id = ? AND m.topico = ? AND e.confirmado = 0",
                (cliente_id, topico),
            ).fetchone():
                return Resultado.INSCRICAO_COM_PENDENCIAS
            conexao.execute(
                "DELETE FROM inscricoes WHERE cliente_id = ? AND topico = ?",
                (cliente_id, topico),
            )
            return Resultado.OK

    def excluir_topico(self, topico):
        with self._conexao() as conexao:
            conexao.execute("BEGIN IMMEDIATE")
            if not conexao.execute(
                "SELECT 1 FROM topicos WHERE nome = ?", (topico,),
            ).fetchone():
                return Resultado.TOPICO_INEXISTENTE
            if conexao.execute(
                "SELECT 1 FROM mensagens WHERE topico = ?", (topico,),
            ).fetchone():
                return Resultado.TOPICO_COM_PENDENCIAS
            conexao.execute("DELETE FROM inscricoes WHERE topico = ?", (topico,))
            conexao.execute("DELETE FROM topicos WHERE nome = ?", (topico,))
            return Resultado.OK
