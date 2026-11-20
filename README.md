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

## Entrega de quarta-feira, 18/11

O broker recebe `PUBLICAR` e grava a mensagem com seus destinatarios em uma
transacao SQLite antes do `ACK_PUBLICACAO`. Sem inscritos, responde
`DESCARTADA_SEM_INSCRITOS` e guarda somente o UUID e o resultado. Um topico
inexistente recebe `TOPICO_INEXISTENTE`.

Reenvios com o mesmo UUID repetem o resultado, sem acrescentar mensagens ou
destinatarios. A selecao usa as inscricoes cadastradas, inclusive offline.
Nesta etapa, elas podem ser preparadas pelo armazenamento para testar o
publicador real de Clara; comandos de inscricao e entrega ficam para quinta.

## Entrega de quinta-feira, 19/11

O broker gerencia inscricoes e topicos pelo protocolo existente. Encaminha
`ENTREGAR` com ID zero aos destinatarios gravados, inclusive apos reconexao.
Cada cliente possui uma rotina de envio separada da recepcao de comandos.
Respostas e entregas sao escritas no socket como quadros completos.

O `ACK_ENTREGA` so confirma a pendencia do cliente identificado na conexao.
Sem ACK, a entrega e reenviada apos cinco segundos. O conteudo so e removido
apos todos os destinatarios confirmarem, mantendo UUID e resultado em disco.
Cancelamento e exclusao com pendencias retornam os erros do protocolo.

## Testar com os exemplos da Clara

Execute os comandos da raiz do repositorio. Primeiro, em um terminal:

```powershell
python -m broker
```

Em outro terminal, prepare o topico uma vez:

```powershell
python -c "from client.assinante import Assinante; c = Assinante(cliente_id='preparacao'); c.conectar(); print(c.criar_topico('agua/temperatura').campos['resultado'].name); c.fechar()"
```

Inicie os paineis antes de publicar, cada um em seu terminal:

```powershell
python -m exemplos.painel_agua --cliente-id painel-agua-01
python -m exemplos.painel_agua --cliente-id painel-agua-02
```

Em outro terminal, inicie o sensor:

```powershell
python -m exemplos.sensor_agua --cliente-id sensor-agua-01
```

Os exemplos nao criam o topico automaticamente. Sem topico, as operacoes
retornam `TOPICO_INEXISTENTE`. Sem inscritos, a publicacao e descartada com aviso.
Use Ctrl+C para encerrar cada programa.

O controle de capacidade de 10.000 mensagens, os scripts de carga e as metricas
continuam para a etapa de sexta-feira. Esta entrega fecha quarta e quinta,
sem mudar os clientes ou o protocolo compartilhado.

## Organizacao

- `shared/protocolo.py`: contrato binario implementado por Clara.
- `broker/servidor.py`: servidor TCP e atendimento por conexao.
- `broker/armazenamento.py`: SQLite e operacoes basicas de cadastro.
- `testes/`: testes do protocolo, banco e servidor.
- [docs/broker.md](docs/broker.md): esquema e orientacoes para a integracao.
