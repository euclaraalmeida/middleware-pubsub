"""Executa cenários de carga contra um broker TCP real."""

import argparse
import csv
import platform
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from queue import Empty
from statistics import median
from threading import Event, Lock
from time import monotonic, sleep
from uuid import uuid4

from client.assinante import Assinante
from client.publicador import FilaCheia, Publicador
from shared.protocolo import Operacao, Resultado, decodificar


CENARIOS = ("basico", "produtores", "dois-assinantes", "lento", "desconectado", "rajadas")
COLUNAS = (
    "evento", "tempo_s", "cliente", "uuid", "solicitacao", "resultado",
    "fila", "latencia_s", "detalhe",
)


class Registro:
    def __init__(self):
        self.inicio = monotonic()
        self.eventos = []
        self._trava = Lock()

    def registrar(self, evento, cliente, **campos):
        linha = dict(evento=evento, tempo_s=monotonic() - self.inicio, cliente=cliente, **campos)
        with self._trava:
            self.eventos.append(linha)


class PublicadorMedido(Publicador):
    def __init__(self, args, cliente_id, registro):
        super().__init__(args.host, args.porta, cliente_id=cliente_id)
        self.registro = registro
        self.limite_envio = float("inf")
        self._uuid_enviado = None

    def _enviar(self, quadro):
        mensagem = decodificar(quadro)
        if mensagem.operacao == Operacao.PUBLICAR:
            if monotonic() >= self.limite_envio:
                raise TimeoutError("Janela de envio encerrada")
            self._uuid_enviado = mensagem.campos["uuid"]
            self.registro.registrar(
                "envio", self.cliente_id, uuid=self._uuid_enviado,
                solicitacao=mensagem.id_solicitacao, fila=self.pendentes,
            )
        super()._enviar(quadro)

    def _aguardar(self, identificador, operacao):
        resposta = super()._aguardar(identificador, operacao)
        if operacao == Operacao.ACK_PUBLICACAO and resposta.campos["uuid"] == self._uuid_enviado:
            self.registro.registrar(
                "ack", self.cliente_id, uuid=self._uuid_enviado, solicitacao=identificador,
                resultado=resposta.campos["resultado"].name,
            )
        return resposta


class AssinanteMedido(Assinante):
    def __init__(self, args, cliente_id, registro):
        super().__init__(args.host, args.porta, cliente_id=cliente_id)
        self.registro = registro

    def _tratar_espontanea(self, mensagem):
        if mensagem.operacao == Operacao.ENTREGAR:
            uuid = mensagem.campos["uuid"]
            evento = "reentrega" if uuid in self._aceitos else "chegada"
            self.registro.registrar(evento, self.cliente_id, uuid=uuid)
        super()._tratar_espontanea(mensagem)


def produzir(cliente, topico, mensagens, intervalo, lote, inicio, prazo, registro):
    geradas = 0
    proximo_lote = inicio
    try:
        while geradas < mensagens and monotonic() < prazo:
            espera = min(proximo_lote, prazo) - monotonic()
            if espera > 0:
                sleep(espera)
            if monotonic() >= prazo:
                break
            quantidade = min(lote, mensagens - geradas)
            for _ in range(quantidade):
                temperatura = 20 + (geradas % 101) / 10
                geradas += 1
                try:
                    uuid = cliente.enfileirar(topico, temperatura)
                except FilaCheia:
                    registro.registrar("fila_cheia", cliente.cliente_id, fila=cliente.pendentes)
                else:
                    registro.registrar("enfileirada", cliente.cliente_id, uuid=uuid, fila=cliente.pendentes)

            while cliente.pendentes and monotonic() < cliente.limite_envio:
                resultado = cliente.publicar_proxima()
                if resultado not in (Resultado.OK, Resultado.DESCARTADA_SEM_INSCRITOS):
                    raise RuntimeError(f"Publicação recusada ({resultado.name})")
            proximo_lote = monotonic() + intervalo
    except Exception as erro:
        registro.registrar("erro", cliente.cliente_id, detalhe=str(erro))
    finally:
        registro.registrar("pendencias_saida", cliente.cliente_id, fila=cliente.pendentes)


def consumir(cliente, parar, demora, retorno, registro):
    try:
        if retorno is not None:
            if parar.wait(max(0, retorno - monotonic())):
                return
            cliente.conectar()
            registro.registrar("retorno", cliente.cliente_id)
        while not parar.is_set():
            try:
                mensagem = cliente.receber(timeout=0.1)
            except Empty:
                continue
            registro.registrar("consumo", cliente.cliente_id, uuid=mensagem.campos["uuid"])
            parar.wait(demora)
    except Exception as erro:
        registro.registrar("erro", cliente.cliente_id, detalhe=str(erro))


def exigir_ok(resposta):
    resultado = resposta.campos["resultado"]
    if resultado != Resultado.OK:
        raise RuntimeError(f"Preparação recusada ({resultado.name}) {resposta.campos['mensagem']}")


def executar(args, topico, ids_assinantes, registro):
    intervalos = args.intervalos if args.cenario == "produtores" else [args.intervalo]
    lote = args.lote if args.cenario == "rajadas" else 1
    assinantes = [AssinanteMedido(args, cliente_id, registro) for cliente_id in ids_assinantes]
    produtores = [
        PublicadorMedido(args, f"{topico}-p{i}", registro) for i in range(1, len(intervalos) + 1)
    ]
    parar = Event()
    try:
        for assinante in assinantes:
            assinante.conectar()
            if assinante is assinantes[0]:
                exigir_ok(assinante.criar_topico(topico))
            exigir_ok(assinante.inscrever(topico))
        for produtor in produtores:
            produtor.conectar()
        if args.cenario == "desconectado":
            assinantes[-1].fechar()
            registro.registrar("desconexao", assinantes[-1].cliente_id)

        inicio = monotonic()
        registro.registrar("inicio", "")
        prazo = inicio + args.duracao
        with ThreadPoolExecutor(max_workers=len(assinantes) + len(produtores)) as grupo:
            try:
                for i, assinante in enumerate(assinantes):
                    demora = args.lentidao if i == 1 and args.cenario == "lento" else 0
                    retorno = inicio + args.desconexao if i == 1 and args.cenario == "desconectado" else None
                    grupo.submit(consumir, assinante, parar, demora, retorno, registro)
                tarefas = []
                for produtor, intervalo in zip(produtores, intervalos):
                    produtor.limite_envio = prazo + args.espera
                    tarefas.append(grupo.submit(
                        produzir, produtor, topico, args.mensagens, intervalo, lote, inicio, prazo, registro,
                    ))
                for tarefa in tarefas:
                    tarefa.result()
                registro.registrar("fim_publicacoes", "")
                parar.wait(args.espera)
            finally:
                parar.set()
    finally:
        for cliente in produtores + assinantes:
            cliente.fechar()
        registro.registrar("fim", "")


def gravar_csv(caminho, colunas, linhas):
    with caminho.open("w", encoding="utf-8", newline="") as arquivo:
        escritor = csv.DictWriter(arquivo, fieldnames=colunas)
        escritor.writeheader()
        escritor.writerows(linhas)


def salvar(args, pasta, topico, assinantes, registro, iniciado_em):
    eventos = sorted(registro.eventos, key=lambda linha: linha["tempo_s"])
    primeiro_envio = {}
    tentativas = {}
    aceitas = set()
    descartadas = set()
    ack_latencias = []
    por_assinante = {}
    for cliente in assinantes:
        por_assinante[cliente] = {
            "cliente": cliente, "chegadas": set(), "consumidas": set(),
            "latencias": [], "reentregas": 0, "pico_fila": 0,
        }

    for linha in eventos:
        evento, cliente, tempo = (linha[chave] for chave in ("evento", "cliente", "tempo_s"))
        uuid = linha.get("uuid")
        if evento == "envio":
            primeiro_envio.setdefault(uuid, tempo)
            tentativas[cliente, linha["solicitacao"]] = tempo
        elif evento == "ack":
            latencia = tempo - tentativas[cliente, linha["solicitacao"]]
            linha["latencia_s"] = latencia
            ack_latencias.append(latencia)
            if linha["resultado"] == Resultado.OK.name:
                aceitas.add(uuid)
            elif linha["resultado"] == Resultado.DESCARTADA_SEM_INSCRITOS.name:
                descartadas.add(uuid)
        elif evento == "chegada":
            dados = por_assinante[cliente]
            dados["chegadas"].add(uuid)
            dados["pico_fila"] = max(
                dados["pico_fila"], len(dados["chegadas"]) - len(dados["consumidas"]),
            )
            if uuid in primeiro_envio:
                latencia = tempo - primeiro_envio[uuid]
                linha["latencia_s"] = latencia
                dados["latencias"].append(latencia)
        elif evento == "reentrega":
            por_assinante[cliente]["reentregas"] += 1
        elif evento == "consumo":
            por_assinante[cliente]["consumidas"].add(uuid)

    duracao = {linha["evento"]: linha["tempo_s"] for linha in eventos
               if linha["evento"] in ("inicio", "fim_publicacoes", "fim")}
    inicio = duracao.get("inicio", 0)
    janela_publicacao = duracao.get("fim_publicacoes", inicio) - inicio
    janela_total = duracao["fim"] - inicio if "inicio" in duracao else 0
    resumo_assinantes = []
    for dados in por_assinante.values():
        resumo_assinantes.append(dict(
            cliente=dados["cliente"], recebidas=len(dados["chegadas"]),
            consumidas=len(dados["consumidas"]), reentregas=dados["reentregas"],
            esperadas_nao_observadas=len(aceitas - dados["chegadas"]),
            pendentes_consumo=len(dados["chegadas"] - dados["consumidas"]),
            pico_fila_entrada=dados["pico_fila"],
            recebidas_por_s=len(dados["chegadas"]) / janela_total if janela_total > 0 else None,
            latencia_chegada_mediana_s=median(dados["latencias"]) if dados["latencias"] else None,
        ))

    resumo = dict(
        **{nome: valor for nome, valor in vars(args).items() if nome != "saida"},
        topico=topico, iniciado_utc=iniciado_em, python=platform.python_version(),
        ambiente=platform.platform(), arquitetura=platform.machine(),
        janela_publicacao_s=janela_publicacao if janela_publicacao > 0 else None,
        janela_total_s=janela_total if janela_total > 0 else None,
        enfileiradas=sum(linha["evento"] == "enfileirada" for linha in eventos),
        enviadas_distintas=len(primeiro_envio), aceitas=len(aceitas), descartadas=len(descartadas),
        tentativas_envio=len(tentativas), retransmissoes=len(tentativas) - len(primeiro_envio),
        recebidas=sum(linha["recebidas"] for linha in resumo_assinantes),
        fila_cheia=sum(linha["evento"] == "fila_cheia" for linha in eventos),
        pendentes_saida=sum(linha["fila"] for linha in eventos if linha["evento"] == "pendencias_saida"),
        pico_fila_saida=max((linha["fila"] for linha in eventos if linha["evento"] == "enfileirada"), default=0),
        aceitas_por_s=len(aceitas) / janela_publicacao if janela_publicacao > 0 else None,
        latencia_ack_mediana_s=median(ack_latencias) if ack_latencias else None,
        erros=sum(linha["evento"] == "erro" for linha in eventos),
    )
    gravar_csv(pasta / "eventos.csv", COLUNAS, eventos)
    gravar_csv(pasta / "resumo.csv", resumo.keys(), [resumo])
    if resumo_assinantes:
        gravar_csv(pasta / "assinantes.csv", resumo_assinantes[0].keys(), resumo_assinantes)
    return resumo


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("cenario", choices=CENARIOS)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--porta", type=int, default=5000)
    parser.add_argument("--mensagens", type=int, default=30, help="Máximo gerado por produtor")
    parser.add_argument("--intervalo", type=float, default=0.05, help="Segundos entre leituras ou lotes")
    parser.add_argument("--intervalos", type=float, nargs="+", default=[0, 0.02, 0.1])
    parser.add_argument("--duracao", type=float, default=10, help="Janela máxima para gerar leituras")
    parser.add_argument("--espera", type=float, default=3, help="Tempo de observação após os produtores")
    parser.add_argument("--lentidao", type=float, default=0.15, help="Pausa do consumidor lento")
    parser.add_argument("--desconexao", type=float, default=1, help="Tempo offline do segundo assinante")
    parser.add_argument("--lote", type=int, default=10)
    parser.add_argument("--versao", default="", help="Revisão do broker ou descrição da compilação")
    parser.add_argument("--saida", type=Path, default=Path("resultados"))
    args = parser.parse_args()
    if args.mensagens < 1 or args.lote < 1 or not 1 <= args.porta <= 65535:
        parser.error("Use mensagens e lote positivos e porta entre 1 e 65535")
    tempos = [args.duracao, args.espera, args.intervalo, args.lentidao, args.desconexao, *args.intervalos]
    if args.duracao <= 0 or any(not 0 <= tempo < float("inf") for tempo in tempos):
        parser.error("Use tempos finitos, com duração positiva e demais tempos não negativos")
    if args.cenario == "produtores" and len(set(args.intervalos)) < 2:
        parser.error("O cenário produtores exige ao menos dois intervalos diferentes")

    iniciado_em = datetime.now(timezone.utc)
    topico = f"avaliacao-{args.cenario}-{uuid4().hex}"
    pasta = args.saida / args.cenario / topico
    pasta.mkdir(parents=True)
    registro = Registro()
    numero_assinantes = 2 if args.cenario in ("dois-assinantes", "lento", "desconectado") else 1
    assinantes = [f"{topico}-a{i}" for i in range(1, numero_assinantes + 1)]
    try:
        executar(args, topico, assinantes, registro)
    except Exception as erro:
        registro.registrar("erro", "", detalhe=str(erro))
    resumo = salvar(args, pasta, topico, assinantes, registro, iniciado_em.isoformat())
    print(f"{pasta} | aceitas={resumo['aceitas']} recebidas={resumo['recebidas']} erros={resumo['erros']}")
    return int(resumo["erros"] > 0)


if __name__ == "__main__":
    raise SystemExit(main())
