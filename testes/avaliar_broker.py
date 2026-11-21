"""Repete a integracao e valida saturacao real, sem alterar o avaliador da Clara."""

import argparse
import csv
import hashlib
import platform
import select
import socket
import subprocess
import sys
import tempfile
from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic, sleep
from unittest.mock import patch
from uuid import uuid4

from broker.estado import consultar_estado
from broker.servidor import BrokerTCP, _Conexao
from client.publicador import Publicador
from shared.protocolo import Operacao, Resultado, codificar, decodificar
from testes.test_broker import receber_resposta


def exigir(condicao, texto):
    if not condicao:
        raise AssertionError(texto)


def cenario(broker, pasta, nome, parametros):
    comando = [
        sys.executable, "-B", "-m", "exemplos.avaliar", *parametros,
        "--porta", str(broker.endereco[1]), "--saida", str(pasta / nome),
        "--versao", "broker com capacidade; fontes em manifesto.txt",
    ]
    subprocess.run(comando, check=True, timeout=60)
    resumo = next((pasta / nome).rglob("resumo.csv"))
    with resumo.open(encoding="utf-8", newline="") as arquivo:
        linha = next(csv.DictReader(arquivo))
    exigir(int(linha["erros"]) == 0, f"Erros em {nome}")
    assinantes = 2 if parametros[0] in ("dois-assinantes", "lento", "desconectado") else 1
    exigir(int(linha["recebidas"]) == int(linha["aceitas"]) * assinantes, "Faltam entregas")
    exigir(int(linha["pendentes_saida"]) == 0, "Produtor ainda tem leituras")
    with resumo.with_name("assinantes.csv").open(encoding="utf-8", newline="") as arquivo:
        for assinante in csv.DictReader(arquivo):
            exigir(int(assinante["pendentes_consumo"]) == 0, "Consumo incompleto")
            exigir(int(assinante["esperadas_nao_observadas"]) == 0, "Chegadas incompletas")
    return linha


def saturar(broker, pasta):
    banco = broker.armazenamento
    banco.criar_topico("saturacao")
    banco.registrar_cliente("painel-saturacao")
    banco.inscrever("painel-saturacao", "saturacao")
    inicio = monotonic()
    with ExitStack() as recursos:
        produtor = Publicador(*broker.endereco, cliente_id="sensor-saturacao")
        recursos.callback(produtor.fechar)
        for i in range(banco.capacidade):
            produtor.enfileirar("saturacao", 25)
            exigir(produtor.publicar_proxima() == Resultado.OK, "Publicacao recusada")
            if (i + 1) % 1000 == 0:
                print(f"Saturacao: {i + 1}/{banco.capacidade}", flush=True)
        cheio = consultar_estado(banco.caminho)
        exigir(cheio["mensagens_pendentes"] == banco.capacidade, "Limite nao foi atingido")

        def conectar(cliente_id):
            conexao = recursos.enter_context(socket.create_connection(broker.endereco, timeout=5))
            conexao.sendall(codificar(Operacao.IDENTIFICAR, 1, cliente_id=cliente_id))
            exigir(receber_resposta(conexao).campos["resultado"] == Resultado.OK, "Identificacao")
            return conexao

        sensor = conectar("sensor-espera")
        novo = uuid4()
        sensor.sendall(codificar(
            Operacao.PUBLICAR, 2, uuid=novo, topico="saturacao", temperatura=26,
        ))
        sensor.sendall(codificar(Operacao.CRIAR_TOPICO, 3, topico="barreira-saturacao"))
        exigir(receber_resposta(sensor).id_solicitacao == 3, "ACK antecipado")
        exigir(not select.select([sensor], [], [], 0.25)[0], "ACK com broker cheio")
        painel = conectar("painel-saturacao")
        primeira = receber_resposta(painel).campos["uuid"]
        painel.sendall(codificar(
            Operacao.ACK_ENTREGA, 0, uuid=primeira, cliente_id="painel-saturacao",
        ))
        resposta = receber_resposta(sensor)
        exigir(resposta.campos["uuid"] == novo, "UUID incorreto apos liberar vaga")
        exigir(resposta.campos["resultado"] == Resultado.OK, "Publicacao nao retomou")
        final = consultar_estado(banco.caminho)
        exigir(final["mensagens_pendentes"] == banco.capacidade, "Contagem incorreta")
    linhas = [dict(
        capacidade=banco.capacidade, preenchidas_por_tcp=cheio["mensagens_pendentes"],
        ack_antecipado=0, novas_aceitas_apos_um_ack=1,
        mensagens_apos_liberacao=final["mensagens_pendentes"],
        entregas_apos_liberacao=final["entregas_pendentes"],
        duracao_s=monotonic() - inicio, uuid_novo=str(novo),
    )]
    with (pasta / "saturacao.csv").open("w", encoding="utf-8", newline="") as arquivo:
        escritor = csv.DictWriter(arquivo, fieldnames=linhas[0].keys())
        escritor.writeheader()
        escritor.writerows(linhas)
    return novo


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--saida", type=Path, default=Path("resultados/broker"))
    args = parser.parse_args()
    pasta = args.saida.resolve() / f"validacao-{uuid4().hex}"
    pasta.mkdir(parents=True)
    raiz = Path(__file__).resolve().parents[1]
    with (pasta / "manifesto.txt").open("w", encoding="utf-8") as arquivo:
        arquivo.write(f"UTC: {datetime.now(timezone.utc).isoformat()}\n")
        arquivo.write(f"Python: {platform.python_version()}\nSO: {platform.platform()}\n")
        for padrao in ("broker/*.py", "client/*.py", "shared/*.py", "exemplos/*.py", "testes/*.py"):
            for fonte in sorted(raiz.glob(padrao)):
                arquivo.write(f"{hashlib.sha256(fonte.read_bytes()).hexdigest()} {fonte.relative_to(raiz).as_posix()}\n")
    with tempfile.TemporaryDirectory() as temporaria:
        caminho = Path(temporaria) / "broker.db"
        with BrokerTCP(porta=0, banco=caminho) as broker:
            comum = ["--mensagens", "20", "--duracao", "5", "--espera", "2.5"]
            for nome, extras in (
                ("basico", ["--intervalo", "0.03"]),
                ("produtores", ["--intervalos", "0", "0.02", "0.08"]),
                ("dois-assinantes", ["--intervalo", "0.03"]),
                ("lento", ["--intervalo", "0.02", "--lentidao", "0.10"]),
                ("desconectado", ["--intervalo", "0.03", "--desconexao", "0.55"]),
                ("rajadas", ["--mensagens", "60", "--intervalo", "0.10", "--lote", "30"]),
            ):
                cenario(broker, pasta, nome, [nome, *comum, *extras])

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
                linha = cenario(broker, pasta, "perda-ack", [
                    "basico", "--mensagens", "1", "--duracao", "8", "--espera", "1",
                ])
            exigir(len(acks) == 2 and int(linha["retransmissoes"]) == 1, "Nao houve retransmissao")
            exigir(len({ack.campos["uuid"] for ack in acks}) == 1, "UUID mudou no reenvio")
            prazo = monotonic() + 5
            while consultar_estado(caminho)["mensagens_pendentes"]:
                exigir(monotonic() < prazo, "Integracao deixou pendencias")
                sleep(0.02)
            novo = saturar(broker, pasta)
        with BrokerTCP(porta=0, banco=caminho) as recuperado:
            exigir(consultar_estado(caminho)["mensagens_pendentes"] == 10_000, "Perda no reinicio")
            exigir(recuperado.armazenamento.publicar(novo, "saturacao", 26) == Resultado.OK, "Deduplicacao")
        (pasta / "verificacoes.txt").write_text(
            "Seis cenarios completos; perda real do primeiro ACK; retransmissao com mesmo UUID; "
            "saturacao TCP de 10.000 leituras; espera sem ACK; liberacao de uma vaga; "
            "reinicio com 10.000 pendencias e deduplicacao: OK.\n",
            encoding="utf-8",
        )
    print(f"Validacao concluida: {pasta}")


if __name__ == "__main__":
    main()
