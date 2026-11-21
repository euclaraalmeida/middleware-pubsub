"""Consulta o SQLite do broker sem abrir uma porta ou alterar seus dados."""

import argparse
import sqlite3
from contextlib import closing
from pathlib import Path


def consultar_estado(caminho):
    endereco = Path(caminho).resolve().as_uri() + "?mode=ro"
    with closing(sqlite3.connect(endereco, uri=True, timeout=5)) as conexao:
        linha = conexao.execute(
            "SELECT (SELECT COUNT(*) FROM mensagens), "
            "(SELECT COUNT(*) FROM entregas WHERE confirmado = 0), "
            "(SELECT COUNT(*) FROM identificadores_concluidos)"
        ).fetchone()
    return dict(zip(("mensagens_pendentes", "entregas_pendentes", "concluidas"), linha))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--banco", default="dados/broker.db")
    args = parser.parse_args()
    try:
        estado = consultar_estado(args.banco)
    except sqlite3.Error as erro:
        parser.exit(1, f"Nao foi possivel consultar o banco: {erro}\n")
    for campo, valor in estado.items():
        print(f"{campo}={valor}")


if __name__ == "__main__":
    main()
