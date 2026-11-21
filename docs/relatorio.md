# Middleware Publish/Subscribe para sensores remotos

Clara Almeida e Mariana Sarinho — Programação Distribuída

## Cenário e objetivo

Sensores remotos produzem leituras de temperatura da água e podem perder
temporariamente a conexão. O sistema distribui essas leituras aos aplicativos
inscritos nos tópicos correspondentes. O objetivo é preservar as publicações
aceitas pelo broker e retomar entregas ainda não confirmadas após reconexões.

Clara implementou o protocolo binário, a biblioteca cliente, os exemplos e o
avaliador de carga. Mariana implementou o broker TCP, o armazenamento SQLite,
o controle de capacidade e as verificações de persistência e recuperação.

## Arquitetura e fluxo

O projeto usa Python e somente sua biblioteca padrão. Há um único broker.
Os sensores usam `Publicador`; os aplicativos usam `Assinante`. Essas classes
escondem a codificação dos quadros, a conexão TCP e as confirmações.

O tópico precisa existir antes das inscrições e publicações. Cada cliente
informa um `cliente_id`, que não pode estar em duas conexões simultâneas.
As inscrições são persistentes e continuam cadastradas durante desconexões.
Um novo inscrito não recebe publicações anteriores à sua inscrição.

Ao produzir uma leitura, a biblioteca gera um UUID e a coloca na fila RAM do
sensor, limitada inicialmente a 1.000 leituras. A tentativa de enfileirar além
desse limite gera `FilaCheia`; a aplicação precisa tratar essa situação. O
UUID permanece igual nos reenvios. IDs de solicitação identificam cada
tentativa e permitem correlacionar a resposta correspondente.

O broker verifica se o UUID já é conhecido. Para uma publicação nova, consulta
o tópico e os inscritos. Sem inscritos, guarda o UUID e o resultado
`DESCARTADA_SEM_INSCRITOS`, sem guardar o conteúdo. Com inscritos e espaço,
grava a leitura e seus destinatários na mesma transação SQLite. Somente após
o commit envia `ACK_PUBLICACAO`. O sensor remove a leitura da fila após `OK`
ou descarte definitivo. Outros resultados preservam a leitura.

O broker encaminha a mensagem aos destinatários registrados. Cada biblioteca
assinante a coloca na fila RAM e envia `ACK_ENTREGA`. O broker registra cada
confirmação separadamente. Sem ACK, reenvia após cinco segundos. Um conjunto
de UUIDs no assinante evita nova entrada na fila durante a mesma execução.

Após a última confirmação, o broker remove conteúdo e entregas, mantendo UUID
e resultado. Se um ACK de publicação se perder, o publicador repete após cinco
segundos. O broker devolve o resultado anterior, sem recriar a publicação.

## Comunicação TCP e protocolo binário

TCP fornece um fluxo ordenado de bytes e trata retransmissões do transporte.
Usá-lo evita implementar controle de perdas, remontagem e ordenação sobre
datagramas. A biblioteca reaproveita a conexão, reduzindo a necessidade de
abrir uma conexão para cada leitura. O protocolo binário com `struct` usa
campos de tamanho definido e texto UTF-8 com comprimento explícito.

O custo dessa escolha inclui estabelecimento e manutenção de conexões,
cabeçalhos de rede, buffers e espera por retransmissões. A entrega ordenada
pode atrasar dados posteriores enquanto uma perda é recuperada. Para este
projeto, a simplicidade de implementação e a confiabilidade do transporte
justificam esse custo. Conexões persistentes e a codificação binária ajudam
a reduzir o custo por leitura, sem afirmar que TCP é o transporte mais leve.

Cada quadro começa com quatro bytes de tamanho, seguidos pela operação,
ID de solicitação e campos daquela operação. Isso permite reconstruir quadros
quando o TCP entrega bytes fragmentados ou várias mensagens juntas. UUIDs
ocupam 16 bytes e a temperatura é um número binário de quatro bytes.
O formato reduz a representação textual, com o custo de exigir um contrato
preciso entre codificador e decodificador. Não há JSON na comunicação de rede.

O ACK do TCP não substitui os ACKs da aplicação: receber bytes não comprova
que o broker fez commit ou que a biblioteca assinante aceitou uma leitura.

## Persistência, concorrência e capacidade

O SQLite guarda clientes, tópicos, inscrições, mensagens, entregas por cliente
e UUIDs concluídos. Cada operação abre sua própria conexão; transações de
escrita usam `BEGIN IMMEDIATE` nos fluxos que exigem verificação e alteração
atômicas. WAL permite leitura durante escrita, mas o SQLite continua com um
escritor por vez. Essa escolha simplifica a persistência, com possível limite
de vazão sob muitas escritas concorrentes.

O servidor possui uma rotina de recepção e uma rotina de envio por conexão
identificada. Há um lock para coordenar quadros escritos no mesmo socket.
O lock global de clientes protege somente os cadastros de conexões e não
permanece adquirido durante espera pela rede. Cancelar uma inscrição ou
excluir um tópico com entregas pendentes é recusado.

O limite padrão é de 10.000 leituras ainda pendentes. Uma leitura para vários
inscritos ocupa uma vaga. A contagem e a inclusão ocorrem na mesma transação,
evitando ultrapassar o limite por publicações concorrentes. UUIDs conhecidos
e descartes sem inscritos não precisam esperar por capacidade.

Quando o broker está cheio, mantém um pedido em espera por conexão, sem ACK
de sucesso. O cliente continua responsável pela cópia local. Reenvios do mesmo
UUID atualizam a tentativa aguardando resposta. Como o publicador da biblioteca
espera o ACK antes da próxima leitura, não é necessária uma fila ilimitada de
pedidos dentro de cada conexão do broker. A recepção continua aceitando ACKs
enquanto esse pedido aguarda.

O último destinatário que confirma uma leitura libera sua vaga. Um aviso
acorda as rotinas de envio, que tentam aceitar as publicações em espera.
A transação que finalmente aceita a leitura define seus destinatários.
Reiniciar o broker preserva a contagem porque ela vem do banco. Inscritos que
ficam offline por muito tempo podem ocupar toda a capacidade; nesse caso,
novas publicações aguardam. O limite controla o acúmulo de leituras, sem
estabelecer cotas individuais ou expiração de inscrições.

## Verificação e resultados disponíveis

Os comandos reproduzíveis estão em [validação do broker](validacao_broker.md)
e [avaliação da Clara](avaliacao.md). A suíte automatizada reúne 77 testes.
Ela cobre os fluxos normais, deduplicação, falhas de gravação, ACKs individuais,
reconexão, concorrência, limite de capacidade e limpeza de conexões.
Um teste termina à força um processo separado após confirmar uma publicação
e recupera a entrega usando o mesmo arquivo de banco.

Os ensaios iniciais da Clara, executados em 20/11 no broker `1a4d74e`, estão
preservados em `resultados/`. Foram seis cenários, uma execução de cada:

| Cenário | Publicações aceitas | Chegadas únicas | Mediana do ACK (ms) |
| --- | ---: | ---: | ---: |
| Básico | 20 | 20 | 6,71 |
| Produtores | 60 | 60 | 31,71 |
| Dois assinantes | 20 | 40 | 18,95 |
| Consumidor lento | 20 | 40 | 13,81 |
| Desconectado | 20 | 40 | 6,52 |
| Rajadas | 60 | 60 | 25,57 |

As 40 chegadas de uma publicação para dois inscritos representam entregas
distintas, sem indicar duplicação indevida. Todas as entregas esperadas foram
observadas nesses ensaios. O cenário lento atrasa o consumo na aplicação,
enquanto a biblioteca continua confirmando a recepção em RAM.

A validação complementar do broker utiliza os mesmos seis cenários e acrescenta
supressão do primeiro ACK, preenchimento de 10.000 vagas pela rede e reinício
com banco cheio. Seus registros ficam separados em `resultados/broker/`, com
um manifesto das fontes executadas. Esses ensaios verificam o comportamento;
não constituem uma comparação estatística de desempenho. A rodada inicial
da validação foi executada em paralelo com a suíte de testes, sem controle
exclusivo da máquina.

Na execução complementar de 21/11, os seis cenários terminaram sem erros:
20/20 chegadas no básico, 60/60 com produtores, 20/40 com dois assinantes,
20/40 com consumidor lento, 20/40 com desconexão e 60/60 em rajadas. A notação
é publicações aceitas/chegadas somadas dos destinatários. A perda do primeiro
ACK gerou duas tentativas para uma publicação e uma única chegada.
A saturação aceitou 10.000 leituras por TCP; a seguinte só recebeu ACK após
uma confirmação liberar espaço. O reinício preservou 10.000 pendências.
Os arquivos estão em
[`validacao-a23a3486314543788dbccde792daff5d`](../resultados/broker/validacao-a23a3486314543788dbccde792daff5d/).

As repetições com cargas variadas, os gráficos e a interpretação quantitativa
de Clara ainda precisam ser incorporados antes da entrega final do relatório.

## Garantias e limitações

A persistência e a deduplicação protegem as publicações aceitas pelo broker.
Falhas na comunicação podem gerar reenvios. O projeto não oferece processamento
exatamente uma vez de ponta a ponta: o ACK do assinante significa entrada na
fila RAM, e uma falha posterior pode perder essa leitura antes do processamento.
A deduplicação local também se perde com o reinício do processo assinante.

As filas do sensor são voláteis. Encerrar o processo antes da confirmação pode
perder as leituras que só estavam na memória. O exemplo simples de sensor
encerra em erro de conexão; a biblioteca preserva sua fila no mesmo objeto
e permite reconexão por nova chamada, mas isso não transforma o exemplo em
coleta contínua durante uma queda. O assinante reconecta automaticamente
após perda de uma conexão estabelecida, reutilizando seu identificador.

O limite de 10.000 restringe leituras pendentes no broker. Ele não limita o
histórico de UUIDs concluídos, o número de inscrições ou conexões, nem a fila
RAM e o conjunto de UUIDs do assinante. Não equivale a um limite total de disco
ou memória. O protocolo identifica clientes, mas não oferece autenticação
ou criptografia. A avaliação considera uma rede local de testes.

Há um único broker, sem replicação. O serviço fica indisponível durante sua
queda e retoma pendências ao voltar com o mesmo banco. Os testes não simulam
falha física do disco ou queda de energia. O projeto prioriza uma implementação
simples e verificável, mantendo explícitas essas limitações.

## Conclusão técnica e preparação da entrega

A solução conecta sensores e inscritos por TCP, persiste publicações antes
de confirmar e recupera entregas pendentes. Os identificadores permitem
repetir tentativas sem criar novas publicações. A capacidade global aplica
espera quando um inscrito impede a conclusão de muitas leituras.

Para a entrega acadêmica, faltam consolidar os gráficos e a análise de cargas
repetidas, revisar este texto em conjunto e identificar a revisão final no
Git. O [andamento do cronograma](cronograma.md) separa essas pendências por
responsabilidade.
