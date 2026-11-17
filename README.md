# Middleware Publish/Subscribe

Projeto em Python para telemetria de sensores remotos, com TCP, protocolo
binario compartilhado e persistencia SQLite no broker.

## Requisitos

Python 3.11 ou superior. Esta etapa usa somente a biblioteca padrao do Python;
nao e necessario instalar pacotes com pip.

## Executar os testes

Na raiz do repositorio:

```powershell
python -m unittest discover -s testes -v
```

Os testes do broker usam portas livres e bancos temporarios. Nao e necessario
iniciar outro servidor para executa-los.

## Iniciar o broker

```powershell
python -m broker
```

O endereco inicial e `127.0.0.1:5000` e o banco fica em `dados/broker.db`.
Encerre com Ctrl+C. Para escolher outra porta ou arquivo:

```powershell
python -m broker --porta 5001 --banco dados/teste.db
```

Para acessar de outro computador de uma rede local de testes, inicie com
`--host 0.0.0.0` e configure no cliente o IP do computador do broker.
O protocolo atual identifica clientes, mas nao autentica usuarios.

## Entrega de terca-feira, 17/11

- Clara: codificador, decodificador e contrato em [docs/protocolo.md](docs/protocolo.md).
- Mariana: servidor TCP concorrente, leitura de quadros parciais ou agrupados,
  esquema SQLite e testes de inicializacao e persistencia.

O broker ja recebe `IDENTIFICAR` e `CRIAR_TOPICO` e responde usando o mesmo
ID de solicitacao. Cada cliente precisa se identificar antes de criar topicos.
Um `cliente_id` simultaneo e recusado; uma desconexao libera o identificador
sem apagar o cadastro persistente.

As operacoes de publicacao, entrega, ACK de entrega, cancelamento e exclusao
ainda nao estao implementadas. `PUBLICAR` recebe `ACK_PUBLICACAO` com
`REQUISICAO_INVALIDA`, nunca um sucesso sem gravacao. Os demais comandos
ainda indisponiveis recebem erro; mensagens de resposta ou entrega enviadas
no sentido incorreto encerram a conexao. O fluxo completo sera integrado
nas proximas etapas do cronograma.

## Organizacao

- `shared/protocolo.py`: contrato binario implementado por Clara.
- `broker/servidor.py`: servidor TCP e atendimento por conexao.
- `broker/armazenamento.py`: SQLite e operacoes basicas de cadastro.
- `testes/`: testes do protocolo, banco e servidor.
- [docs/broker.md](docs/broker.md): esquema e orientacoes para a integracao.
