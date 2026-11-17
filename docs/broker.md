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

Publicacao e encaminhamento continuam para quarta e quinta-feira. A base nao
gera `OK` para esses comandos sem implementar seu efeito. Nenhum codigo ou
campo foi acrescentado ao protocolo compartilhado.

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

O metodo `inscrever` esta disponivel para testes locais do banco, mas o comando
de rede `INSCREVER` ainda depende da etapa de quinta-feira. Inscrever exige
que o cliente e o topico ja existam.

Ainda falta implementar a transacao de publicacao: consultar o UUID, selecionar
inscritos e salvar mensagem e destinatarios antes do ACK. As tabelas existem,
mas isso nao significa que publicacao, deduplicacao ou entrega ja funcionam.

A exclusao de mensagens remove suas entregas por chave estrangeira; ela nao
remove os identificadores concluidos. Essa exclusao so devera ser chamada
apos verificar as confirmacoes exigidas. Topicos referenciados por mensagens
ou inscricoes nao sao apagados automaticamente pelo banco.

## Conferencia com Clara

- Usar os codigos e campos ja definidos em `docs/protocolo.md`.
- Informar cliente_id no primeiro comando e reutilizar a conexao TCP.
- Manter IDs de solicitacao nas respostas e UUIDs nos reenvios.
- Integrar PUBLICAR e ACK_PUBLICACAO na quarta-feira.
- Integrar inscricoes, ENTREGAR e ACK_ENTREGA na quinta-feira.
