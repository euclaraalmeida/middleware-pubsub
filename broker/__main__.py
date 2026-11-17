"""Execute da raiz do repositorio com: python -m broker."""

import argparse
import logging
import time

from .servidor import BrokerTCP


def main():
    parser = argparse.ArgumentParser(description="Broker TCP Publish/Subscribe")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--porta", type=int, default=5000)
    parser.add_argument("--banco", default="dados/broker.db")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    try:
        with BrokerTCP(args.host, args.porta, args.banco) as broker:
            logging.info("Broker ouvindo em %s:%s", *broker.endereco)
            logging.info("SQLite: %s | Ctrl+C para encerrar", broker.armazenamento.caminho)
            while True:
                time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Broker encerrado")


if __name__ == "__main__":
    main()
