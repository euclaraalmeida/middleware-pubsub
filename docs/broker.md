# Base do broker - Mariana

## Interface de rede

Iniciar da raiz com `python -m broker`. Endereco padrao: `127.0.0.1:5000`.
O servidor usa diretamente `shared.protocolo`, sem outro formato de mensagens.
O prefixo de quatro bytes e validado antes de ler o corpo do quadro.

Cada conexao possui uma thread, que recebe os comandos em sequencia.
Uma leitura parcial nao bloqueia as outras conexoes. Os locks de identificacao
protegem somente os mapas de conexoes, sem aguardar rede ou banco dentro deles.
O encerramento fecha as conexoes e aguarda suas threads.

O cliente envia `IDENTIFICAR` com ID de solicitacao diferente de zero.
Recebe `RESPOSTA` com o mesmo ID e `OK` ou `CLIENTE_ID_EM_USO`.
Depois pode enviar `CRIAR_TOPICO`, recebendo `OK` ou `TOPICO_JA_EXISTENTE`.
Nao ha uma mensagem de boas-vindas anterior a IDENTIFICAR.

`PUBLICAR` recebe `ACK_PUBLICACAO` com o UUID e o ID da tentativa. O broker
responde somente apos a transacao SQLite. Sem inscritos, registra o resultado
de descarte; com inscritos, grava a leitura e seus destinatarios. Um reenvio
repete o resultado, mesmo se as inscricoes mudaram depois da publicacao.
Nenhum codigo ou campo foi acrescentado ao protocolo compartilhado.

## Armazenamento

O SQLite e criado automaticamente no arquivo escolhido por `--banco`.
Cada operacao abre e fecha uma conexao propria. Chaves estrangeiras estao
habilitadas em cada conexao; transacoes nao compartilham objetos de conexao
entre threads. O modo WAL permite leituras durante uma gravacao.

| Tabela | Responsabilidade |
| --- | --- |
| clientes | Identificadores cadastrados, sem indicar conexao ativa. |
| topicos | Nomes dos topicos existentes. |
| inscricoes | Pares cliente/topico, preservados durante desconexoes. |
| mensagens | UUID de 16 bytes, topico, temperatura e momento do registro. |
| entregas | Destinatarios de cada mensagem e sua confirmacao individual. |
| identificadores_concluidos | UUID e resultado preservados apos remover o conteudo. |

O comando `INSCREVER` grava a associacao do cliente identificado ao topico.
Inscrever exige que o topico ja exista. Repetir uma inscricao nao duplica
destinatarios. `CANCELAR_INSCRICAO` e `EXCLUIR_TOPICO` consultam as pendencias
na mesma transacao da alteracao e recusam operacoes que perderiam entregas.

A transacao de publicacao usa `BEGIN IMMEDIATE` antes de consultar o UUID,
selecionar inscritos e salvar mensagem e destinatarios. Isso serializa as
escritas concorrentes sem manter um lock de rede durante o acesso ao banco.
Os registros em `identificadores_concluidos` guardam os descartes; uma mensagem
ainda em `mensagens` tem resultado `OK`.

## Entregas e reconexao

A identificacao bem-sucedida inicia a rotina de envio da conexao. Ela consulta
somente as entregas pendentes daquele cliente e transmite `ENTREGAR` com ID
zero. Sem ACK, reenvia o mesmo UUID apos cinco segundos. O intervalo pode ser
reduzido no construtor de `BrokerTCP` para os testes, sem alterar o protocolo.

A recepcao de comandos e o envio de entregas usam threads separadas. Um lock
por conexao impede a mistura de bytes entre uma resposta e uma entrega.
O broker nunca mantem seu lock global de clientes durante um envio de rede.

O campo cliente_id de `ACK_ENTREGA` deve coincidir com a identidade da conexao.
ACKs repetidos ou atrasados nao recriam mensagens. O ultimo ACK grava o UUID e
o resultado em `identificadores_concluidos` e remove conteudo e entregas na
mesma transacao. O ACK continua significando entrada na fila RAM do cliente,
conforme implementado pela biblioteca da Clara.

Desconectar nao cancela inscricoes. As publicacoes continuam selecionando os
inscritos cadastrados naquele instante, como previsto no cronograma. Ao voltar
com o mesmo cliente_id, o inscrito recebe suas pendencias. Um inscrito novo nao
recebe leituras anteriores. Reiniciar o broker tambem preserva esse estado.

## Capacidade

O limite padrao e de 10.000 registros em `mensagens`, independentemente da
quantidade de destinatarios de cada leitura. `--capacidade` permite reduzir
esse numero em ensaios. A contagem e o INSERT ficam na mesma transacao
`BEGIN IMMEDIATE`, impedindo que publicadores concorrentes excedam o limite.
UUIDs conhecidos, descartes sem inscritos e erros de topico sao verificados
antes da capacidade. Esses casos nao precisam aguardar uma vaga.

Quando nao ha espaco, `Armazenamento.publicar` retorna `None` internamente.
Isso nao e um novo resultado do protocolo. A conexao guarda um unico pedido
de publicacao em espera, sem grava-lo ou confirmar sucesso. Reenvios do mesmo
UUID atualizam o ID da tentativa aguardando resposta. O publicador deve esperar
o ACK antes de enviar outra leitura, como ja faz a biblioteca da Clara.
Enviar outro UUID enquanto esse pedido aguarda encerra somente a conexao
infratora, evitando uma fila ilimitada de pedidos por conexao.

A rotina de envio tenta novamente quando recebe um aviso de ACK ou quando
vence o intervalo de verificacao, de cinco segundos. A rotina de recepcao
continua disponivel para ACKs e comandos, inclusive se a mesma conexao publica
e recebe leituras. A espera nao segura uma transacao SQLite nem o lock global
de clientes. A publicacao e confirmada apenas depois do commit.

Os destinatarios sao selecionados na transacao que aceita a leitura. Antes
disso o pedido ainda esta em espera. O ultimo ACK de cada leitura remove o
conteudo e libera uma vaga; ACKs parciais nao a liberam. Ao reabrir o banco,
a capacidade e consultada nos registros existentes, sem um contador em RAM
que precise ser reconstruido. Se um banco ja tiver mais registros que o novo
limite configurado, eles sao preservados e novas leituras aguardam.

Ao perder a conexao ou encerrar o broker, um pedido ainda nao aceito pode ser
descartado no servidor. O sensor continua responsavel por sua copia RAM e
deve reenviar o mesmo UUID. Se a gravacao ja ocorreu, a deduplicacao devolve
o resultado persistido. A politica de retencao das inscricoes offline permanece.

## Observacao

`python -m broker.estado --banco dados/broker.db` abre o SQLite em modo
somente leitura. Mostra leituras pendentes, entregas individuais sem ACK e
UUIDs concluidos. Uma leitura para dois inscritos pode representar uma
mensagem e duas entregas pendentes. `concluidas` inclui descartes sem inscritos
e leituras cujos destinatarios todos confirmaram; nao e um contador de ACKs
individuais. O comando nao cria bancos ausentes.

A exclusao de mensagens remove suas entregas por chave estrangeira; ela nao
remove os identificadores concluidos. Essa exclusao so devera ser chamada
apos verificar as confirmacoes exigidas. Topicos referenciados por mensagens
ou inscricoes nao sao apagados automaticamente pelo banco.

## Conferencia com Clara

- Usar os codigos e campos ja definidos em `docs/protocolo.md`.
- Informar cliente_id no primeiro comando e reutilizar a conexao TCP.
- Manter IDs de solicitacao nas respostas e UUIDs nos reenvios.
- PUBLICAR e ACK_PUBLICACAO integrados com o publicador real de quarta-feira.
- Inscricoes, ENTREGAR e ACK_ENTREGA integrados com o assinante real de quinta.
- Criar o topico antes de iniciar os exemplos de painel e sensor.
- A documentacao de cliente da Clara ainda descreve limites da base de terca;
  ela pode atualizar esse status com os resultados da integracao, sem mudar a API.
- A validacao completa do broker esta em `docs/validacao_broker.md`.
