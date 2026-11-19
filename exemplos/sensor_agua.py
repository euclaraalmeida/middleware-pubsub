"""Sensor de temperatura simulado. Execute com python -m exemplos.sensor_agua."""

import argparse
import logging
import random
import time
from uuid import uuid4

from client.publicador import IdentificacaoRecusada, Publicador
from shared.protocolo import ErroProtocolo, Resultado


def main():
    parser = argparse.ArgumentParser(description="Simula a temperatura da água")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--porta", type=int, default=5000)
    parser.add_argument("--topico", default="agua/temperatura")
    identidade = parser.add_mutually_exclusive_group()
    identidade.add_argument("--cliente-id")
    identidade.add_argument("--sem-identificacao", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    cliente_id = None if args.sem_identificacao else args.cliente_id or f"sensor-{uuid4()}"
    cliente = Publicador(args.host, args.porta, cliente_id=cliente_id)
    try:
        cliente.conectar()
        while True:
            temperatura = round(random.uniform(20, 30), 2)
            identificador = cliente.enfileirar(args.topico, temperatura)
            logging.info("%s enfileirada em %s: %.2f °C", identificador, args.topico, temperatura)
            resultado = cliente.publicar_proxima()
            if resultado not in (Resultado.OK, Resultado.DESCARTADA_SEM_INSCRITOS):
                return 1
            logging.info("%s resultado definitivo: %s", identificador, resultado.name)
            time.sleep(1)
    except KeyboardInterrupt:
        return 0
    except (OSError, ErroProtocolo, IdentificacaoRecusada) as erro:
        logging.error("Falha no sensor: %s", erro)
        return 1
    finally:
        cliente.fechar()


if __name__ == "__main__":
    raise SystemExit(main())
