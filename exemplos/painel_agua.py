"""Exibe temperaturas recebidas. Execute com python -m exemplos.painel_agua."""

import argparse
import logging

from client.assinante import Assinante
from client.conexao import IdentificacaoRecusada
from shared.protocolo import ErroProtocolo, Resultado


def main():
    parser = argparse.ArgumentParser(description="Recebe temperaturas da água")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--porta", type=int, default=5000)
    parser.add_argument("--cliente-id", required=True)
    parser.add_argument("--topico", default="agua/temperatura")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    assinante = Assinante(args.host, args.porta, cliente_id=args.cliente_id)
    try:
        assinante.conectar()
        resposta = assinante.inscrever(args.topico)
        resultado = resposta.campos["resultado"]
        if resultado != Resultado.OK:
            logging.error("Inscrição recusada (%s) %s", resultado.name, resposta.campos["mensagem"])
            return 1
        while True:
            campos = assinante.receber().campos
            print(f"{campos['topico']} | {campos['temperatura']:.2f} °C | {campos['uuid']}", flush=True)
    except KeyboardInterrupt:
        return 0
    except (OSError, ErroProtocolo, IdentificacaoRecusada) as erro:
        logging.error("Falha no painel: %s", erro)
        return 1
    finally:
        assinante.fechar()


if __name__ == "__main__":
    raise SystemExit(main())
