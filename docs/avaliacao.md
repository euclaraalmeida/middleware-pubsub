# Avaliação do cenário de sensores remotos

O avaliador executa os clientes reais contra o broker TCP da Mariana. Cada
execução cria um tópico e IDs próprios para isolar os dados. Use um broker e
um banco reservados para avaliação, pois os tópicos, as inscrições e os
resultados por UUID ficam no SQLite.

## Como repetir os cenários

Na raiz do projeto, inicie o broker em um terminal:

```powershell
python -B -m broker --banco dados/avaliacao.db
```

Em outro terminal, rode os cenários na sequência abaixo. Os comandos usam
os parâmetros dos ensaios de sexta. O host padrão é local. Use `--host` e
`--porta` para indicar outro broker e `--versao` para registrar a revisão.

```powershell
python -B -m exemplos.avaliar basico --mensagens 20 --intervalo 0.03 --duracao 5 --espera 2.5
python -B -m exemplos.avaliar produtores --mensagens 20 --intervalos 0 0.02 0.08 --duracao 5 --espera 2.5
python -B -m exemplos.avaliar dois-assinantes --mensagens 20 --intervalo 0.03 --duracao 5 --espera 2.5
python -B -m exemplos.avaliar lento --mensagens 20 --intervalo 0.02 --lentidao 0.10 --duracao 5 --espera 2.5
python -B -m exemplos.avaliar desconectado --mensagens 20 --intervalo 0.03 --desconexao 0.55 --duracao 5 --espera 2.5
python -B -m exemplos.avaliar rajadas --mensagens 60 --intervalo 0.10 --lote 30 --duracao 5 --espera 2.5
```

| Cenário | Configuração |
| --- | --- |
| `basico` | Um produtor e um assinante ativo. |
| `produtores` | Um produtor por valor de `--intervalos`, com um assinante ativo. |
| `dois-assinantes` | Um produtor e dois assinantes ativos. |
| `lento` | Um produtor e dois assinantes. O segundo pausa entre consumos. A thread de recepção continua aceitando e confirmando entradas na fila. |
| `desconectado` | Um assinante continua ativo e o outro fecha a conexão depois de se inscrever. Após `--desconexao`, o segundo abre outra conexão com o mesmo ID. |
| `rajadas` | O produtor coloca até `--lote` leituras na fila de uma vez e drena o lote antes de começar o próximo. |

`--mensagens` é o teto gerado por produtor. `--duracao` limita a janela
para iniciar novos lotes. O cliente envia uma leitura por vez e aguarda o
ACK, por isso a taxa obtida inclui a espera pelo broker. O intervalo é
contado após drenar o lote anterior. O envio em andamento pode ultrapassar
a duração configurada enquanto aguarda um ACK. `--espera` define a janela
de observação após o fim dos produtores e a margem para drenar a saída.
Para o cenário desconectado, dê tempo para o retorno e a drenagem nessa
janela. Um erro de conexão ou uma publicação recusada aparece no CSV e
gera código de saída 1.

## Arquivos e métricas

A saída padrão está em `resultados/<cenario>/<id-da-execucao>/`.
Use `--saida` para escolher outra raiz.

- `eventos.csv` guarda os eventos por UUID, cliente e ID da solicitação
  quando disponível. Os eventos incluem tentativa de envio, ACK,
  chegada, reentrega, consumo, desconexão, retorno e fila cheia.
- `resumo.csv` guarda parâmetros, horário UTC, ambiente Python e SO,
  contagens, pendências de saída e taxa de publicações aceitas.
- `assinantes.csv` guarda chegadas únicas, consumo, reentregas, pico
  inferido de fila, taxa e mediana de latência por assinante.

Os clientes do avaliador compartilham um processo e um relógio
`time.monotonic()`. O UUID já existente liga os eventos sem mudar o
protocolo. Os clientes continuam usando conexões TCP distintas.

| Medição | Início e fim |
| --- | --- |
| Latência do ACK | Da chamada de envio de uma tentativa até o retorno do ACK correlacionado por ID e UUID. Inclui o envio local, TCP, broker e a leitura da resposta. |
| Latência de chegada | Do primeiro envio observado do UUID até o assinante decodificar a primeira entrega. Inclui espera por reconexão ou reenvio, se houver. |
| Publicação e retransmissão | Conta UUIDs com tentativa de envio, tentativas totais, tentativas adicionais por UUID e resultados `OK` ou descarte. Uma tentativa registrada pode falhar no socket antes de chegar ao broker. |
| Recepção e consumo | Chegada única é contada por assinante. A mesma publicação entregue a dois assinantes conta como duas chegadas. O consumo é registrado quando a aplicação retira a leitura da fila. |
| Pendências | A saída soma as filas dos produtores ao final. `pendentes_consumo` conta chegadas ainda sem consumo. `esperadas_nao_observadas` compara publicações com ACK `OK` às chegadas de cada assinante. Esses números descrevem a observação dos clientes. |
| Taxa | Publicações aceitas por segundo usam a janela dos produtores. Chegadas por segundo usam a janela total, incluindo observação e encerramento das conexões. |

Ausência de amostras de latência gera campo vazio. Valores em zero nas
contagens representam zero eventos observados. O cenário lento mede
atraso de consumo na aplicação. A fila e o conjunto de UUIDs do assinante
ficam em RAM, e a instrumentação registra eventos em RAM até gravar os CSVs.
Os ensaios medem o ambiente local e incluem o custo da instrumentação e
do agendamento das threads. A latência não é comparável entre relógios
independentes de dois processos por este método.

## Ensaios executados em 20/11/2026

Os seis comandos foram executados uma vez em TCP local, com banco
temporário, broker do commit `1a4d74e` e o avaliador desta etapa. Os
18 CSVs estão em [resultados](../resultados/). Cada resumo registra a
porta usada e o ambiente de execução.

| Cenário | Publicações aceitas | Chegadas únicas | Mediana do ACK (ms) | Mediana da chegada por assinante (ms) |
| --- | ---: | ---: | ---: | --- |
| Básico | 20 | 20 | 6,71 | 14,42 |
| Produtores | 60 | 60 | 31,71 | 69,44 |
| Dois assinantes | 20 | 40 | 18,95 | 38,41 / 50,01 |
| Lento | 20 | 40 | 13,81 | 31,01 / 36,06 |
| Desconectado | 20 | 40 | 6,52 | 16,97 / 244,77 |
| Rajadas | 60 | 60 | 25,57 | 46,94 |

As execuções terminaram com todas as entregas esperadas observadas e
sem pendências de consumo ou de saída. A fila de saída chegou a 30 no
cenário de rajadas. No cenário lento, o pico inferido da fila de entrada
foi 13 no consumidor lento e 1 no ativo. O assinante que voltou recebeu
as 16 leituras enviadas antes do evento de retorno. Nenhum ensaio registrou
retransmissão ou reentrega. Esses ensaios curtos verificam a coleta e o
fluxo local. Comparações de desempenho exigem repetições com os mesmos
parâmetros e condições de execução.

## Pendências de integração

A saturação no limite global de 10.000 mensagens depende da implementação
de Mariana. Uma medição direta da quantidade de pendências no broker
depende de uma forma acordada de observá-las. Os CSVs atuais mostram
as filas locais e as entregas esperadas ainda não observadas.
O cenário de desconexão usa uma queda planejada do assinante. Ensaios
controlados que exijam injeção de falha no broker, como suprimir o
primeiro ACK de publicação, ficam pendentes dessa integração.
